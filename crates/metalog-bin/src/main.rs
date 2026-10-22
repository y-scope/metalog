use std::{process, sync::Arc};

use clap::{Parser, Subcommand};
use metalog_proto::coordinator::{
    admin_service_client::AdminServiceClient,
    metadata_ingestion_service_server::MetadataIngestionServiceServer,
    RegisterTableRequest,
};

#[derive(Parser)]
#[command(name = "metalog", about = "CLP Metastore Service")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the metalog node server.
    Serve {
        /// Path to the node configuration file.
        #[arg(long, default_value = "/etc/clp/node.yaml")]
        config: String,
    },
    /// Administrative operations.
    #[command(subcommand)]
    Admin(AdminCommands),
}

#[derive(Subcommand)]
enum AdminCommands {
    /// Register a new table.
    RegisterTable {
        /// gRPC server address.
        #[arg(long, default_value = "localhost:9090")]
        addr: String,
        /// Table name (required).
        #[arg(long)]
        table: String,
        /// Display name (optional, defaults to table name).
        #[arg(long)]
        display_name: Option<String>,
        /// JSON config blob (optional).
        #[arg(long)]
        config_json: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve { config } => {
            if let Err(e) = run_server(&config).await {
                tracing::error!(error = %e, "server failed");
                process::exit(1);
            }
        }
        Commands::Admin(admin) => {
            if let Err(e) = run_admin(admin).await {
                tracing::error!(error = %e, "admin command failed");
                process::exit(1);
            }
        }
    }
}

async fn run_server(config_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;

    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_util::sync::CancellationToken;

    // Install handlers before advertising readiness, including signals arriving during startup.
    let shutdown = shutdown_signal()?;
    tracing::info!(config = config_path, "loading configuration");
    let config = metalog_config::NodeConfig::load(config_path)?;
    if config.worker.concurrency > 0 {
        return Err("consolidation workers are not supported by this Rust server".into());
    }
    let node_id = config.resolve_node_id()?;
    let pool = metalog_db::new_pool(&config.database.primary).await?;
    let read_pool = match &config.database.replica {
        Some(replica) => metalog_db::new_pool(replica).await?,
        None => pool.clone(),
    };
    let (db_type, version) = metalog_db::detect_database_type(&pool).await?;
    tracing::info!(?db_type, %version, %node_id, "connected to database");
    let is_mariadb = db_type.is_mariadb();
    let mut resources = metalog_node::Resources::new(
        pool.clone(),
        is_mariadb,
        config.logging.failure_log_interval(),
    );
    resources.read_db = Some(read_pool.clone());
    let shared = Arc::new(resources);
    let storage = Arc::new(metalog_storage::Registry::from_config(&config.storage).await?);
    let writer = Arc::new(
        metalog_ingestion::BatchingWriter::new(pool.clone(), is_mariadb)
            .with_storage(storage.clone()),
    );
    let mut builder =
        metalog_node::NodeBuilder::new(config.clone(), shared).with_writer(writer.clone());
    if !config.storage.backends.is_empty() {
        builder = builder.with_storage(storage);
    }
    let mut node = builder.build();

    let health_server = if config.health.enabled {
        let primary = pool.clone();
        let replica = read_pool.clone();
        Some(
            metalog_health::HealthServer::new(config.health.port).with_readiness_probe(move || {
                let primary = primary.clone();
                let replica = replica.clone();
                async move {
                    sqlx::query("SELECT 1").execute(&primary).await.is_ok()
                        && sqlx::query("SELECT 1").execute(&replica).await.is_ok()
                }
            }),
        )
    } else {
        None
    };
    let health_listener = match &health_server {
        Some(server) => Some(server.bind().await?),
        None => None,
    };
    let grpc_addr = metalog_grpc::GrpcServer::new(config.grpc.port).addr();
    let grpc_listener = if config.grpc.has_any_service() {
        Some(tokio::net::TcpListener::bind(grpc_addr).await?)
    } else {
        None
    };

    // Query/admin-only roles do not run coordinators. Ingestion roles require
    // coordinator.enabled (validated by config), and HA discovers table ownership.
    node.start(&[]).await?;
    if config.coordinator.enabled {
        let ha = metalog_ha::HAModule::new(pool.clone(), &node_id, config.coordinator.clone());
        if let Err(error) = ha.registry().register_node().await {
            node.stop().await;
            return Err(error.into());
        }
        let token = node.token().clone();
        let (on_start, on_stop, stalled) = node.ha_callbacks();
        node.track_background_task(ha.start_liveness(token.clone()));
        node.track_background_task(ha.start_reconciliation(token, on_start, on_stop, stalled));
    }
    let reader = Arc::new(metalog_metastore::MetadataReader::new(read_pool.clone()));

    // Build the tonic gRPC server with configured services.
    let grpc_config = &config.grpc;
    let mut router = tonic::transport::Server::builder();

    // Ingestion service.
    let ingestion_svc = if grpc_config.ingestion.enabled {
        let svc = Arc::new(metalog_ingestion::IngestionService::new(
            writer.clone(),
            grpc_config.is_blocking_ingestion(),
        ));
        let handler = metalog_grpc::IngestionHandler::new(svc);
        tracing::info!("ingestion service enabled");
        Some(MetadataIngestionServiceServer::new(handler))
    } else {
        None
    };

    // Admin service.
    let admin_svc = if grpc_config.admin.enabled {
        let registration = Arc::new(metalog_coordinator::TableRegistration::new(
            pool.clone(),
            &config.coordinator.table_compression,
        ));
        let handler = metalog_grpc::AdminHandler::new(registration, None);
        tracing::info!("admin service enabled");
        Some(metalog_proto::coordinator::admin_service_server::AdminServiceServer::new(handler))
    } else {
        None
    };

    // Query service.
    let query_svc = if grpc_config.query.enabled {
        let engine = Arc::new(metalog_query::SplitQueryEngine::new(read_pool.clone()));
        let handler = metalog_grpc::QueryHandler::new(engine, read_pool.clone());
        tracing::info!("query service enabled");
        Some(
            metalog_proto::query::split_query_service_server::SplitQueryServiceServer::new(handler),
        )
    } else {
        None
    };

    // Metadata service.
    let metadata_svc = if grpc_config.metadata.enabled {
        let handler = metalog_grpc::MetadataHandler::new(reader.clone());
        tracing::info!("metadata service enabled");
        Some(metalog_proto::query::metadata_service_server::MetadataServiceServer::new(handler))
    } else {
        None
    };

    let grpc_shutdown = CancellationToken::new();
    let health_shutdown = CancellationToken::new();
    let mut grpc_handle = grpc_listener.map(|listener| {
        let shutdown = grpc_shutdown.clone();
        tokio::spawn(async move {
            tracing::info!(addr = %grpc_addr, "gRPC server listening");
            router
                .add_optional_service(ingestion_svc)
                .add_optional_service(admin_svc)
                .add_optional_service(query_svc)
                .add_optional_service(metadata_svc)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                )
                .await
                .map_err(|e| e.to_string())
        })
    });
    let mut health_handle = match (health_server.clone(), health_listener) {
        (Some(server), Some(listener)) => {
            let shutdown = health_shutdown.clone();
            Some(tokio::spawn(async move {
                server
                    .serve_with_shutdown(listener, shutdown.cancelled_owned())
                    .await
                    .map_err(|e| e.to_string())
            }))
        }
        _ => None,
    };
    if let Some(server) = &health_server {
        server.set_ready(true);
    }
    tracing::info!(%node_id, "metalog node ready");

    // Listener failures and task panics are process failures, not healthy zombies.
    let mut failure = tokio::select! {
        result = shutdown => result.err().map(|e| e.to_string()),
        result = await_server(&mut grpc_handle) => {
            grpc_handle.take();
            Some(format!("gRPC server stopped unexpectedly: {result:?}"))
        }
        result = await_server(&mut health_handle) => {
            health_handle.take();
            Some(format!("health server stopped unexpectedly: {result:?}"))
        }
    };
    if let Some(server) = &health_server {
        server.set_ready(false);
    }
    tracing::info!("stopping listeners and draining requests");
    grpc_shutdown.cancel();
    if let Some(mut handle) = grpc_handle {
        match tokio::time::timeout(Duration::from_secs(30), &mut handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(result) => {
                failure.get_or_insert_with(|| format!("gRPC shutdown failed: {result:?}"));
            }
            Err(_) => {
                tracing::warn!("gRPC drain deadline reached; cancelling remaining requests");
                handle.abort();
                let _ = handle.await;
            }
        }
    }
    // No new requests can enqueue records after this point. Stop HA/coordinators,
    // release ownership, then persist every accepted record before process exit.
    node.stop().await;
    health_shutdown.cancel();
    if let Some(mut handle) = health_handle {
        if tokio::time::timeout(Duration::from_secs(5), &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
    if let Some(error) = failure {
        return Err(std::io::Error::other(error).into());
    }
    Ok(())
}

async fn await_server(
    handle: &mut Option<tokio::task::JoinHandle<Result<(), String>>>,
) -> Result<(), String> {
    match handle {
        Some(handle) => handle.await.map_err(|e| e.to_string())?,
        None => std::future::pending().await,
    }
}

fn shutdown_signal(
) -> Result<impl std::future::Future<Output = Result<(), std::io::Error>>, std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => Ok(()),
                _ = terminate.recv() => Ok(()),
            }
        })
    }
    #[cfg(not(unix))]
    Ok(async { tokio::signal::ctrl_c().await })
}

async fn run_admin(cmd: AdminCommands) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        AdminCommands::RegisterTable {
            addr,
            table,
            display_name,
            config_json,
        } => {
            let endpoint = format!("http://{addr}");
            tracing::info!(%endpoint, table = %table, "connecting to admin service");

            let mut client = AdminServiceClient::connect(endpoint).await?;

            let request = RegisterTableRequest {
                table_name: table.clone(),
                display_name: display_name.unwrap_or_default(),
                config_json,
            };

            let response = client.register_table(request).await?.into_inner();

            if response.created {
                tracing::info!(table = %response.table_name, "table registered");
                println!("Registered table '{}'", response.table_name);
            } else {
                tracing::info!(table = %response.table_name, "table already exists (idempotent)");
                println!("Table '{}' already exists", response.table_name);
            }

            Ok(())
        }
    }
}
