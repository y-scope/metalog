package com.yscope.metalog.common.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight HTTP server for Kubernetes health check endpoints.
 *
 * <p>Provides standard health check endpoints:
 *
 * <ul>
 *   <li>{@code /health/live} - Liveness probe: is the process running?
 *   <li>{@code /health/ready} - Readiness probe: is the process ready to accept traffic?
 * </ul>
 *
 * <p>Uses JDK's built-in HttpServer to avoid external dependencies.
 *
 * <h3>Kubernetes Integration</h3>
 *
 * <pre>
 * livenessProbe:
 *   httpGet:
 *     path: /health/live
 *     port: 8081
 *   initialDelaySeconds: 10
 *   periodSeconds: 10
 *
 * readinessProbe:
 *   httpGet:
 *     path: /health/ready
 *     port: 8081
 *   initialDelaySeconds: 5
 *   periodSeconds: 5
 * </pre>
 */
public class HealthCheckServer {
  private static final Logger logger = LoggerFactory.getLogger(HealthCheckServer.class);

  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final int HTTP_OK = 200;
  private static final int HTTP_SERVICE_UNAVAILABLE = 503;

  private final HttpServer server;
  private final ExecutorService executor;
  private final List<HealthCheck> livenessChecks = new CopyOnWriteArrayList<>();
  private final List<HealthCheck> readinessChecks = new CopyOnWriteArrayList<>();

  /**
   * Create a health check server on the specified port.
   *
   * @param port Port to listen on (default: 8081)
   * @throws IOException if the server cannot be created
   */
  public HealthCheckServer(int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    this.executor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "HealthCheckServer");
              t.setDaemon(true);
              return t;
            });
    this.server.setExecutor(executor);

    // Register endpoints
    server.createContext("/health/live", this::handleLiveness);
    server.createContext("/health/ready", this::handleReadiness);

    logger.info("Health check server configured on port {}", port);
  }

  /**
   * Register a liveness check.
   *
   * <p>Liveness checks determine if the process is alive and should continue running. If a liveness
   * check fails, Kubernetes will restart the container.
   *
   * @param name Check name for logging
   * @param check Check function returning true if healthy
   * @return this for fluent configuration
   */
  public HealthCheckServer addLivenessCheck(String name, Supplier<Boolean> check) {
    livenessChecks.add(new HealthCheck(name, check));
    return this;
  }

  /**
   * Register a readiness check.
   *
   * <p>Readiness checks determine if the process is ready to accept traffic. If a readiness check
   * fails, Kubernetes will stop sending traffic.
   *
   * @param name Check name for logging
   * @param check Check function returning true if ready
   * @return this for fluent configuration
   */
  public HealthCheckServer addReadinessCheck(String name, Supplier<Boolean> check) {
    readinessChecks.add(new HealthCheck(name, check));
    return this;
  }

  /** Start the health check server. */
  public void start() {
    server.start();
    logger.info(
        "Health check server started with {} liveness checks, {} readiness checks",
        livenessChecks.size(),
        readinessChecks.size());
  }

  /** Stop the health check server. */
  public void stop() {
    server.stop(0);
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
    logger.info("Health check server stopped");
  }

  /**
   * Get the actual port the server is bound to.
   *
   * <p>Useful when using port 0 (ephemeral port) for testing.
   *
   * @return The actual port number
   */
  public int getPort() {
    return server.getAddress().getPort();
  }

  private void handleLiveness(HttpExchange exchange) throws IOException {
    HealthResult result = runChecks(livenessChecks);
    sendResponse(exchange, result);
  }

  private void handleReadiness(HttpExchange exchange) throws IOException {
    HealthResult result = runChecks(readinessChecks);
    sendResponse(exchange, result);
  }

  private HealthResult runChecks(List<HealthCheck> checks) {
    if (checks.isEmpty()) {
      return new HealthResult(true, "No checks configured");
    }

    StringBuilder details = new StringBuilder();
    boolean allHealthy = true;

    for (HealthCheck check : checks) {
      boolean healthy;
      try {
        healthy = check.check.get();
      } catch (Exception e) {
        logger.warn("Health check '{}' threw exception", check.name, e);
        healthy = false;
      }

      if (!healthy) {
        allHealthy = false;
        if (details.length() > 0) details.append(", ");
        details.append(check.name).append(": UNHEALTHY");
      }
    }

    if (allHealthy) {
      return new HealthResult(true, "All " + checks.size() + " checks passed");
    } else {
      return new HealthResult(false, details.toString());
    }
  }

  private void sendResponse(HttpExchange exchange, HealthResult result) throws IOException {
    String json =
        String.format(
            "{\"status\":\"%s\",\"details\":\"%s\"}",
            result.healthy ? "UP" : "DOWN", result.details.replace("\"", "\\\""));

    byte[] response = json.getBytes(StandardCharsets.UTF_8);
    int statusCode = result.healthy ? HTTP_OK : HTTP_SERVICE_UNAVAILABLE;

    exchange.getResponseHeaders().set("Content-Type", CONTENT_TYPE_JSON);
    exchange.sendResponseHeaders(statusCode, response.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  private static class HealthCheck {
    final String name;
    final Supplier<Boolean> check;

    HealthCheck(String name, Supplier<Boolean> check) {
      this.name = name;
      this.check = check;
    }
  }

  private static class HealthResult {
    final boolean healthy;
    final String details;

    HealthResult(boolean healthy, String details) {
      this.healthy = healthy;
      this.details = details;
    }
  }
}
