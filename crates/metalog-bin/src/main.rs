use std::process;

use clap::{Parser, Subcommand};

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
    let shared = std::sync::Arc::new(metalog_node::Resources::new(
        pool,
        is_mariadb,
        failure_log_interval,
    ));

    // Build node (enterprise edition with all premium providers).
    let mut node = metalog_node::NodeBuilder::new(config, shared).build();

    // TODO: Register premium providers:
    // - AggExtension
    // - SketchExtension
    // - KafkaModule
    // - HAModule
    // - ConsolidationModule
    // - RetentionModule

    // Wait for shutdown signal.
    tracing::info!("metalog node started, press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;

    tracing::info!("shutdown signal received");
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
            tracing::info!(addr = %addr, table = %table, "registering table");

            // Connect to the admin gRPC service.
            let endpoint = format!("http://{addr}");
            tracing::info!(%endpoint, "connecting to server");

            // TODO: Use AdminService client (RegisterTable RPC).
            // Currently just verifies the CLI argument parsing works.
            tracing::info!(
                table = %table,
                display_name = ?display_name,
                config_json = ?config_json,
                "table registration would be sent via AdminService RPC"
            );

            Ok(())
        }
    }
}
