package com.yscope.metalog.node;

import com.yscope.metalog.common.config.YamlConfigLoader;
import com.yscope.metalog.coordinator.ingestion.IngestionService;
import com.yscope.metalog.coordinator.ingestion.KafkaMetadataPollerFactory;
import com.yscope.metalog.coordinator.ingestion.server.IngestionServerFactory;
import com.yscope.metalog.query.api.ApiServerConfig;
import com.yscope.metalog.query.api.server.vertx.grpc.GrpcServer;
import com.yscope.metalog.query.core.CacheService;
import com.yscope.metalog.query.core.QueryService;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Production entry point: starts a Node (coordinators + workers) plus a unified gRPC server hosting
 * all services (ingestion, query, catalog, admin) on a single port.
 *
 * <p>The gRPC server is configured via the {@code node.grpc} section in {@code node.yaml}. Each
 * service can be independently disabled.
 *
 * <p>For embedding the node in a host application without the gRPC server, use {@link NodeMain}.
 *
 * <p>Usage:
 *
 * <pre>
 * java -jar grpc-server/target/metalog-grpc-server-*.jar [config-path]
 * </pre>
 */
public class ServerMain {
  private static final Logger logger = LoggerFactory.getLogger(ServerMain.class);

  public static void main(String[] args) {
    Path configPath = args.length > 0 ? Paths.get(args[0]) : Paths.get("/etc/clp/node.yaml");

    logger.info("Starting server with config: {}", configPath);

    // Use AtomicReferences so the shutdown hook can see resources as they are created.
    // This handles the case where startup fails partway through — the hook cleans up
    // whatever was successfully initialized.
    AtomicReference<GrpcServer> grpcServerRef = new AtomicReference<>();
    AtomicReference<QueryService> queryServiceRef = new AtomicReference<>();
    AtomicReference<Node> nodeRef = new AtomicReference<>();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Shutdown signal received");
                  GrpcServer server = grpcServerRef.get();
                  if (server != null) {
                    try {
                      server.stop();
                    } catch (InterruptedException e) {
                      logger.warn("gRPC server shutdown interrupted", e);
                      Thread.currentThread().interrupt();
                    }
                  }
                  QueryService qs = queryServiceRef.get();
                  if (qs != null) {
                    qs.close();
                  }
                  Node n = nodeRef.get();
                  if (n != null) {
                    n.close();
                  }
                }));

    try {
      YamlConfigLoader<NodeConfig> yamlConfig =
          new YamlConfigLoader<>(NodeConfig.class, configPath, "config/node.yaml");

      NodeConfig config = yamlConfig.get();
      NodeConfig.GrpcConfig grpcConfig = config.getNode().getGrpc();

      // Start the headless node (coordinators + workers) with ingestion server disabled
      // since the unified GrpcServer handles ingestion instead
      Node node =
          new Node(yamlConfig, IngestionServerFactory.disabled(), new KafkaMetadataPollerFactory());
      nodeRef.set(node);
      node.start();

      if (grpcConfig.isEnabled()) {
        // Build query service for query/catalog services
        ApiServerConfig apiConfig =
            ApiServerConfig.fromNodeDatabaseConfig(
                config.getNode().getDatabase(), grpcConfig.getPort());
        CacheService cacheService = new CacheService(apiConfig.cache());
        QueryService queryService = new QueryService(apiConfig.dataSource(), cacheService);
        queryServiceRef.set(queryService);

        // Get ingestion service from the node for the ingestion gRPC service
        IngestionService ingestionService = node.getIngestionService();

        GrpcServer grpcServer =
            new GrpcServer(
                grpcConfig,
                queryService,
                apiConfig.timeout(),
                apiConfig.streaming(),
                node.getSharedResources().getDataSource(),
                ingestionService);
        grpcServerRef.set(grpcServer);
        grpcServer.start();
        logger.info("Unified gRPC server started on port {}", grpcConfig.getPort());
      } else {
        logger.info("Unified gRPC server disabled (set node.grpc.enabled: true to enable)");
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
