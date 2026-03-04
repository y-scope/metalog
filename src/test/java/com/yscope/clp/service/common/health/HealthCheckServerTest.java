package com.yscope.clp.service.common.health;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for HealthCheckServer. */
class HealthCheckServerTest {

  private static final int TEST_PORT = 0; // Use ephemeral port to avoid port collisions
  private HealthCheckServer server;
  private int actualPort; // Actual port bound by the OS

  @BeforeEach
  void setUp() throws IOException {
    server = new HealthCheckServer(TEST_PORT);
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void testLivenessEndpoint_noChecks_returnsHealthy() throws IOException {
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(200, conn.getResponseCode());
    String response = new String(conn.getInputStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"UP\""));
  }

  @Test
  void testReadinessEndpoint_noChecks_returnsHealthy() throws IOException {
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/ready").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(200, conn.getResponseCode());
    String response = new String(conn.getInputStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"UP\""));
  }

  @Test
  void testLivenessEndpoint_healthyCheck_returnsHealthy() throws IOException {
    server.addLivenessCheck("test-check", () -> true);
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(200, conn.getResponseCode());
    String response = new String(conn.getInputStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"UP\""));
  }

  @Test
  void testLivenessEndpoint_unhealthyCheck_returns503() throws IOException {
    server.addLivenessCheck("failing-check", () -> false);
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(503, conn.getResponseCode());
    String response = new String(conn.getErrorStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"DOWN\""));
    assertTrue(response.contains("failing-check"));
  }

  @Test
  void testReadinessEndpoint_unhealthyCheck_returns503() throws IOException {
    server.addReadinessCheck("failing-check", () -> false);
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/ready").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(503, conn.getResponseCode());
    String response = new String(conn.getErrorStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"DOWN\""));
  }

  @Test
  void testLivenessEndpoint_multipleChecks_allMustPass() throws IOException {
    server.addLivenessCheck("check1", () -> true);
    server.addLivenessCheck("check2", () -> true);
    server.addLivenessCheck("check3", () -> false); // One failing
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(503, conn.getResponseCode());
    String response = new String(conn.getErrorStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"DOWN\""));
    assertTrue(response.contains("check3"));
  }

  @Test
  void testLivenessEndpoint_checkThrowsException_treatedAsUnhealthy() throws IOException {
    server.addLivenessCheck(
        "throwing-check",
        () -> {
          throw new RuntimeException("Simulated failure");
        });
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(503, conn.getResponseCode());
    String response = new String(conn.getErrorStream().readAllBytes());
    assertTrue(response.contains("\"status\":\"DOWN\""));
  }

  @Test
  void testFluentConfiguration() throws IOException {
    server
        .addLivenessCheck("live1", () -> true)
        .addLivenessCheck("live2", () -> true)
        .addReadinessCheck("ready1", () -> true)
        .start();
    actualPort = server.getPort(); // Get actual ephemeral port

    // Verify liveness
    HttpURLConnection liveConn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    assertEquals(200, liveConn.getResponseCode());

    // Verify readiness
    HttpURLConnection readyConn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/ready").openConnection();
    assertEquals(200, readyConn.getResponseCode());
  }

  @Test
  void testContentTypeIsJson() throws IOException {
    server.start();
    actualPort = server.getPort(); // Get actual ephemeral port

    HttpURLConnection conn =
        (HttpURLConnection)
            new URL("http://localhost:" + actualPort + "/health/live").openConnection();
    conn.setRequestMethod("GET");

    assertEquals(200, conn.getResponseCode());
    assertEquals("application/json", conn.getContentType());
  }
}
