package com.yscope.clp.service.node;

import com.yscope.clp.service.common.config.YamlConfigLoader;
import com.yscope.clp.service.coordinator.ingestion.server.grpc.IngestionGrpcServer;
import com.yscope.clp.service.query.api.ApiServerConfig;
import com.yscope.clp.service.query.api.server.vertx.VertxApiServer;
import io.vertx.core.Vertx;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Standalone entry point that wires Node with the default gRPC ingestion server. */
public class NodeMain {
  private static final Logger logger = LoggerFactory.getLogger(NodeMain.class);

  /**
   * Main entry point for running a Node.
   *
   * @param args Command line arguments: [config-path]
   */
  public static void main(String[] args) {
    // Determine config path
    Path configPath = args.length > 0 ? Paths.get(args[0]) : Paths.get("/etc/clp/node.yaml");

    logger.info("Starting Node with config: {}", configPath);

    try {
      // Load node-level configuration (database, storage, health)
      // Unit configuration comes from the database, not YAML
      YamlConfigLoader<NodeConfig> yamlConfig =
          new YamlConfigLoader<>(NodeConfig.class, configPath, "config/node.yaml");

      // Create and start node
      Node node = new Node(yamlConfig, IngestionGrpcServer.factory());
      node.start();

      NodeConfig.ApiConfig apiCfg = yamlConfig.get().getNode().getApi();
      if (apiCfg.isEnabled()) {
        // Deploy the embedded gRPC API server, sharing the node's DB credentials.
        // Using fromNodeDatabaseConfig avoids the need for separate DB_* env vars.
        ApiServerConfig apiConfig =
            ApiServerConfig.fromNodeDatabaseConfig(
                yamlConfig.get().getNode().getDatabase(), apiCfg.getGrpcPort());
        Vertx vertx = Vertx.vertx();
        String deploymentId =
            vertx
                .deployVerticle(
                    new VertxApiServer(apiConfig, node.getSharedResources().getDataSource()))
                .toCompletionStage()
                .toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        logger.info("API server deployed on port {}", apiConfig.grpcPort());

        // Register shutdown hook for graceful termination
        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(
                    () -> {
                      logger.info("Shutdown signal received");
                      try {
                        vertx
                            .undeploy(deploymentId)
                            .toCompletionStage()
                            .toCompletableFuture()
                            .get(30, TimeUnit.SECONDS);
                        vertx
                            .close()
                            .toCompletionStage()
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);
                      } catch (Exception e) {
                        logger.warn("Error stopping API server during shutdown", e);
                      }
                      node.close();
                    }));
      } else {
        logger.info(
            "Embedded API server disabled (set node.api.enabled: true to enable)");

        // Register shutdown hook for graceful termination (no API server to stop)
        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> {
              logger.info("Shutdown signal received");
              node.close();
            }));
      }

      // Keep main thread alive
      Thread.currentThread().join();

    } catch (YamlConfigLoader.ConfigException e) {
      logger.error("Failed to load configuration: {}", e.getMessage());
      System.exit(1);
    } catch (Exception e) {
      logger.error("Failed to start node", e);
      System.exit(1);
    }
  }
}
