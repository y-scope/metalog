package com.yscope.metalog.query.api.server.vertx.grpc;

import com.yscope.metalog.coordinator.ingestion.IngestionService;
import com.yscope.metalog.coordinator.ingestion.server.grpc.IngestionGrpcService;
import com.yscope.metalog.node.NodeConfig;
import com.yscope.metalog.query.api.ApiServerConfig;
import com.yscope.metalog.query.core.QueryService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified gRPC server hosting all services on a single port.
 *
 * <p>Each service is independently toggleable via {@link NodeConfig.GrpcConfig}:
 *
 * <ul>
 *   <li><b>ingestion</b> — MetadataIngestionService (receive metadata from Spark/producers)
 *   <li><b>query</b> — QuerySplitsService (stream file splits with filtering)
 *   <li><b>catalog</b> — MetadataService (list tables, dimensions, aggregations)
 *   <li><b>admin</b> — CoordinatorService (runtime table registration)
 * </ul>
 *
 * <p>Example usage with grpcurl:
 *
 * <pre>
 * grpcurl -plaintext localhost:9090 list
 * </pre>
 */
public class GrpcServer {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final int port;
  private final Server server;
  private final QuerySplitsGrpcService splitsService;

  /**
   * Create a unified gRPC server with per-service toggles.
   *
   * @param grpcConfig unified gRPC configuration with service toggles
   * @param queryService query service for splits and metadata (may be null if query/catalog
   *     disabled)
   * @param timeoutConfig timeout settings for streaming RPCs
   * @param streamingConfig streaming settings (page size, etc.)
   * @param dataSource shared datasource for admin service
   * @param ingestionService ingestion service (may be null if ingestion disabled)
   */
  public GrpcServer(
      NodeConfig.GrpcConfig grpcConfig,
      QueryService queryService,
      ApiServerConfig.TimeoutConfig timeoutConfig,
      ApiServerConfig.StreamingConfig streamingConfig,
      DataSource dataSource,
      IngestionService ingestionService) {
    this.port = grpcConfig.getPort();

    List<String> enabledServices = new ArrayList<>();
    ServerBuilder<?> builder = ServerBuilder.forPort(port);

    // Ingestion service — receive metadata from Spark/producers
    if (grpcConfig.isIngestionEnabled() && ingestionService != null) {
      builder.addService(new IngestionGrpcService(ingestionService));
      enabledServices.add("ingestion");
    }

    // Query service — stream file splits with filtering
    if (grpcConfig.isQueryEnabled() && queryService != null) {
      this.splitsService =
          new QuerySplitsGrpcService(
              queryService.getSplitQueryEngine(), timeoutConfig, streamingConfig);
      builder.addService(splitsService);
      enabledServices.add("query");
    } else {
      this.splitsService = null;
    }

    // Catalog service — list tables, dimensions, aggregations
    if (grpcConfig.isCatalogEnabled() && queryService != null) {
      builder.addService(new MetadataGrpcService(queryService));
      enabledServices.add("catalog");
    }

    // Admin service — runtime table registration
    if (grpcConfig.isAdminEnabled()) {
      builder.addService(new CoordinatorGrpcService(dataSource));
      enabledServices.add("admin");
    }

    builder.addService(ProtoReflectionService.newInstance());
    this.server = builder.build();

    LOG.info("gRPC server configured on port {} with services: {}", port, enabledServices);
  }

  /** Start the gRPC server. */
  public void start() throws IOException {
    server.start();
    LOG.info("gRPC server started on port {}", port);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Shutting down gRPC server...");
                  try {
                    GrpcServer.this.stop();
                  } catch (InterruptedException e) {
                    LOG.error("gRPC server shutdown interrupted", e);
                    Thread.currentThread().interrupt();
                  }
                }));
  }

  /** Stop the gRPC server gracefully. */
  public void stop() throws InterruptedException {
    if (splitsService != null) {
      splitsService.close();
    }
    if (server != null) {
      server.shutdown();
      if (!server.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn("gRPC server did not terminate in time, forcing shutdown");
        server.shutdownNow();
      }
      LOG.info("gRPC server stopped");
    }
  }

  /** Block until the server shuts down. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /** Get the actual port the server is running on. */
  public int getPort() {
    return server.getPort();
  }

  /** Check if the server is running. */
  public boolean isRunning() {
    return server != null && !server.isShutdown();
  }
}
