use std::process;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use metalog_proto::coordinator::{
    admin_service_client::AdminServiceClient,
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
    tracing::info!(config = config_path, "loading configuration");

    let config = metalog_config::NodeConfig::load(config_path)?;
    let node_id = config.resolve_node_id()?;
    tracing::info!(node_id = %node_id, "resolved node identity");

    // Create database pool.
    let pool = metalog_db::new_pool(&config.database.primary).await?;
    let (db_type, version) = metalog_db::detect_database_type(&pool).await?;
    tracing::info!(?db_type, %version, "connected to database");

    let is_mariadb = db_type.is_mariadb();
    let failure_log_interval = config.logging.failure_log_interval();

    // Create shared resources.
    let shared = Arc::new(metalog_node::Resources::new(
        pool.clone(),
        is_mariadb,
        failure_log_interval,
    ));

    // Create the BatchingWriter for ingestion.
    let writer = Arc::new(metalog_ingestion::BatchingWriter::new(
        pool.clone(),
        is_mariadb,
    ));

    // Build node with the writer attached.
    let mut node = metalog_node::NodeBuilder::new(config.clone(), shared.clone())
        .with_writer(writer.clone())
        .build();

    // Initialize schema.
    // In HA mode the reconciliation loop drives table assignment, so we start with no tables.
    // In community edition we start all existing tables directly.
    let reader = Arc::new(metalog_metastore::MetadataReader::new(pool.clone()));

    let tables_to_start: Vec<String> = if config.coordinator.enabled {
        vec![]
    } else {
        match reader.list_tables().await {
            Ok(tables) => tables,
            Err(_) => {
                // Schema may not exist yet on first boot; start with no tables.
                tracing::info!("no existing tables found (first boot?)");
                vec![]
            }
        }
    };

    node.start(&tables_to_start).await?;

    // Wire HA module when coordinator mode is enabled.
    // The reconciliation loop drives table assignment; the liveness loop sends heartbeats.
    if config.coordinator.enabled {
        let ha = metalog_ha::HAModule::new(
            pool.clone(),
            &node_id,
            config.coordinator.clone(),
        );
        ha.registry().register_node().await?;
        tracing::info!(node_id = %node_id, "HA mode: registered node in registry");

        let token = node.token().clone();
        let (on_start, on_stop, stall_checker) = node.ha_callbacks();

        ha.start_liveness(token.clone());
        ha.start_reconciliation(token, on_start, on_stop, stall_checker);
        tracing::info!("HA reconciliation loop started");
    }

    // Wire premium Kafka module for admin source registration.
    let kafka = Arc::new(metalog_kafka::KafkaModule::new(pool.clone()));
    tracing::info!("node started");

    // Build the tonic gRPC server with configured services.
    let grpc_config = &config.grpc;
    let grpc_server = metalog_grpc::GrpcServer::new(grpc_config.port);
    let addr = grpc_server.addr();

    let mut router = tonic::transport::Server::builder();

    // Ingestion service.
    let ingestion_svc = if grpc_config.ingestion.enabled {
        let svc = Arc::new(metalog_ingestion::IngestionService::new(
            writer.clone(),
            grpc_config.is_blocking_ingestion(),
        ));
        let handler = metalog_grpc::IngestionHandler::new(svc);
        tracing::info!("ingestion service enabled");
        Some(
            metalog_proto::coordinator::metadata_ingestion_service_server::MetadataIngestionServiceServer::new(handler),
        )
    } else {
        None
    };

    // Admin service.
    let admin_svc = if grpc_config.admin.enabled {
        let registration = Arc::new(metalog_coordinator::TableRegistration::new(
            pool.clone(),
            &config.coordinator.table_compression,
        ));
        let handler = metalog_grpc::AdminHandler::new(registration, Some(kafka.clone()));
        tracing::info!("admin service enabled");
        Some(
            metalog_proto::coordinator::admin_service_server::AdminServiceServer::new(handler),
        )
    } else {
        None
    };

    // Query service.
    let query_svc = if grpc_config.query.enabled {
        let engine = Arc::new(metalog_query::SplitQueryEngine::new(pool.clone()));
        let handler = metalog_grpc::QueryHandler::new(engine);
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
        Some(
            metalog_proto::query::metadata_service_server::MetadataServiceServer::new(handler),
        )
    } else {
        None
    };

    // Build the final router with all enabled services.
    // tonic requires adding services to the router in one chain since
    // add_service changes the type. We use add_optional_service to handle
    // the Option<T> for each service.
    let grpc_handle = tokio::spawn(async move {
        tracing::info!(%addr, "gRPC server listening");
        router
            .add_optional_service(ingestion_svc)
            .add_optional_service(admin_svc)
            .add_optional_service(query_svc)
            .add_optional_service(metadata_svc)
            .serve(addr)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "gRPC server failed");
                e
            })
    });

    // Wait for shutdown signal.
    tracing::info!("metalog node started, press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    tracing::info!("shutdown signal received");

    // Abort the gRPC server task (tonic's serve runs forever).
    grpc_handle.abort();
    let _ = grpc_handle.await;

    // Stop node (drains writer, cancels coordinators).
    node.stop().await;

    Ok(())
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
                config_json: config_json,
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
