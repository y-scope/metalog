package com.yscope.clp.service.query.api.server.vertx.grpc;

import com.yscope.clp.service.query.api.ApiServerConfig;
import com.yscope.clp.service.query.core.QueryService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC server for streaming APIs.
 *
 * <p>Provides:
 *
 * <ul>
 *   <li>Split streaming with early termination
 *   <li>Reflection service for grpcurl/grpcui
 *   <li>Graceful shutdown with drain timeout
 * </ul>
 *
 * <p>Example usage with grpcurl:
 *
 * <pre>
 * # List services
 * grpcurl -plaintext localhost:9090 list
 *
 * # Stream splits (first page, limit to 10 results)
 * grpcurl -plaintext -d '{
 *   "table": "clp_table",
 *   "limit": 10,
 *   "state_filter": ["ARCHIVE_CLOSED"],
 *   "order_by": [{"column": "max_timestamp", "order": "DESC"}]
 * }' localhost:9090 com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
 *
 * # Stream splits (next page using cursor from previous response)
 * grpcurl -plaintext -d '{
 *   "table": "clp_table",
 *   "limit": 10,
 *   "state_filter": ["ARCHIVE_CLOSED"],
 *   "order_by": [{"column": "max_timestamp", "order": "DESC"}],
 *   "cursor": {"values": [{"int_val": 1704067200}], "id": 123}
 * }' localhost:9090 com.yscope.clp.service.query.api.proto.grpc.QuerySplitsService/StreamSplits
 *
 * </pre>
 */
public class GrpcServer {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final int port;
  private final Server server;
  private final QuerySplitsGrpcService splitsService;

  public GrpcServer(
      int port,
      QueryService queryService,
      ApiServerConfig.TimeoutConfig timeoutConfig,
      ApiServerConfig.StreamingConfig streamingConfig,
      DataSource dataSource) {
    this.port = port;

    this.splitsService =
        new QuerySplitsGrpcService(
            queryService.getSplitQueryEngine(), timeoutConfig, streamingConfig);
    MetadataGrpcService metadataService = new MetadataGrpcService(queryService);

    this.server =
        ServerBuilder.forPort(port)
            .addService(splitsService)
            .addService(metadataService)
            .addService(new CoordinatorGrpcService(dataSource))
            .addService(ProtoReflectionService.newInstance()) // For grpcurl discovery
            .build();
  }

  /** Start the gRPC server. */
  public void start() throws IOException {
    server.start();
    LOG.info("gRPC server started on port {}", port);

    // Register shutdown hook
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
    splitsService.close();
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
