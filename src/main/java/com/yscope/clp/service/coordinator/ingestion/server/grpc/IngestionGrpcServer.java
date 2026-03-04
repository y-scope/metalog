package com.yscope.clp.service.coordinator.ingestion.server.grpc;

import com.yscope.clp.service.coordinator.ingestion.server.IngestionServer;
import com.yscope.clp.service.coordinator.ingestion.server.IngestionServerFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC server for the metadata ingestion endpoint.
 *
 * <p>Follows the same pattern as the existing {@code GrpcServer} for split queries. Lifecycle is
 * managed by {@link com.yscope.clp.service.node.Node}.
 */
public class IngestionGrpcServer implements IngestionServer {
  private static final Logger logger = LoggerFactory.getLogger(IngestionGrpcServer.class);
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final int port;
  private final Server server;

  /** Returns an {@link IngestionServerFactory} that creates gRPC-based ingestion servers. */
  public static IngestionServerFactory factory() {
    return (port, service) -> new IngestionGrpcServer(port, new IngestionGrpcService(service));
  }

  public IngestionGrpcServer(int port, IngestionGrpcService service) {
    this.port = port;
    this.server =
        ServerBuilder.forPort(port)
            .addService(service)
            .addService(ProtoReflectionService.newInstance())
            .build();
  }

  @Override
  public void start() throws IOException {
    server.start();
    logger.info("Ingestion gRPC server started on port {}", port);
  }

  @Override
  public void stop() {
    server.shutdown();
    try {
      if (!server.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        logger.warn("Ingestion gRPC server did not terminate in time, forcing shutdown");
        server.shutdownNow();
      }
    } catch (InterruptedException e) {
      server.shutdownNow();
      Thread.currentThread().interrupt();
    }
    logger.info("Ingestion gRPC server stopped");
  }
}
