package com.yscope.clp.service.node;

import com.yscope.clp.service.common.config.YamlConfigLoader;
import com.yscope.clp.service.coordinator.ingestion.MetadataPollerFactory;
import com.yscope.clp.service.coordinator.ingestion.server.IngestionServerFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Headless entry point that runs coordinators and workers without any gRPC or Vert.x servers.
 *
 * <p>Integrators who provide their own transport layer (e.g., Spring gRPC) can use this entry point
 * directly. For the full gRPC server experience, use {@code ServerMain} in the grpc-server
 * module.
 */
public class NodeMain {
  private static final Logger logger = LoggerFactory.getLogger(NodeMain.class);

  /**
   * Main entry point for running a headless Node.
   *
   * @param args Command line arguments: [config-path]
   */
  public static void main(String[] args) {
    Path configPath = args.length > 0 ? Paths.get(args[0]) : Paths.get("/etc/clp/node.yaml");

    logger.info("Starting headless Node with config: {}", configPath);

    try {
      YamlConfigLoader<NodeConfig> yamlConfig =
          new YamlConfigLoader<>(NodeConfig.class, configPath, "config/node.yaml");

      Node node =
          new Node(yamlConfig, IngestionServerFactory.disabled(), MetadataPollerFactory.disabled());
      node.start();

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    logger.info("Shutdown signal received");
                    node.close();
                  }));

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
