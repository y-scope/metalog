package com.yscope.clp.service.query.api.server.vertx;

import com.yscope.clp.service.node.NodeConfig;
import com.yscope.clp.service.query.api.ApiServerConfig;
import com.yscope.clp.service.query.api.server.vertx.grpc.GrpcServer;
import com.yscope.clp.service.query.core.CacheService;
import com.yscope.clp.service.query.core.QueryService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.IOException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone API Server entry point.
 *
 * <p>Read-only metadata access layer supporting multiple protocols:
 *
 * <ul>
 *   <li>gRPC — Streaming with early termination, internal services
 *   <li>REST (future) — Simple queries, browser clients, tooling
 *   <li>MCP (future) — LLM integration via natural language
 * </ul>
 *
 * <p>All protocols share the same QueryService — each is a thin adapter.
 */
public class VertxApiServer extends AbstractVerticle {

  private static final Logger LOG = LoggerFactory.getLogger(VertxApiServer.class);

  private final ApiServerConfig config;
  private final QueryService queryService;
  private final CacheService cacheService;
  private final DataSource coordinatorDataSource;
  private final boolean ownsCoordinatorDataSource;

  private GrpcServer grpcServer;

  /**
   * Create an API server with an externally-managed coordinator DataSource.
   *
   * <p>Used when the Node provides its own shared DataSource (read-write). The caller is
   * responsible for the DataSource lifecycle — it will NOT be closed on stop.
   */
  public VertxApiServer(ApiServerConfig config, DataSource coordinatorDataSource) {
    this.config = config;
    this.coordinatorDataSource = coordinatorDataSource;
    this.ownsCoordinatorDataSource = false;
    this.cacheService = new CacheService(config.cache());
    this.queryService = new QueryService(config.dataSource(), cacheService);
  }

  /**
   * Create a standalone API server that manages its own coordinator DataSource.
   *
   * <p>Used when running as a standalone process (via {@link #main}). Creates a read-write
   * DataSource from the same connection settings as the query DataSource.
   */
  public VertxApiServer(ApiServerConfig config) {
    this.config = config;
    HikariConfig hc = config.dataSource().toHikariConfig();
    hc.setReadOnly(false);
    hc.setPoolName("coordinator-write-pool");
    this.coordinatorDataSource = new HikariDataSource(hc);
    this.ownsCoordinatorDataSource = true;
    this.cacheService = new CacheService(config.cache());
    this.queryService = new QueryService(config.dataSource(), cacheService);
  }

  @Override
  public void start(Promise<Void> startPromise) {
    try {
      // Build a GrpcConfig for standalone mode: query + catalog + admin enabled, no ingestion
      NodeConfig.GrpcConfig grpcConfig = new NodeConfig.GrpcConfig();
      grpcConfig.setPort(config.grpcPort());
      grpcConfig.getServices().setIngestion(false);

      this.grpcServer =
          new GrpcServer(
              grpcConfig,
              queryService,
              config.timeout(),
              config.streaming(),
              coordinatorDataSource,
              null);
      grpcServer.start();
      startPromise.complete();
    } catch (IOException e) {
      LOG.error("Failed to start gRPC server", e);
      startPromise.fail(e);
    }
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    if (grpcServer != null) {
      try {
        grpcServer.stop();
      } catch (InterruptedException e) {
        LOG.warn("gRPC server shutdown interrupted", e);
        Thread.currentThread().interrupt();
      }
    }
    queryService.close();
    if (ownsCoordinatorDataSource && coordinatorDataSource instanceof HikariDataSource hds) {
      hds.close();
    }
    LOG.info("API server stopped");
    stopPromise.complete();
  }

  public static void main(String[] args) {
    ApiServerConfig config = ApiServerConfig.fromEnvironment();
    Vertx vertx = Vertx.vertx();

    vertx
        .deployVerticle(new VertxApiServer(config))
        .onSuccess(id -> LOG.info("API Server deployed: {}", id))
        .onFailure(
            err -> {
              LOG.error("Failed to deploy API Server", err);
              System.exit(1);
            });
  }
}
