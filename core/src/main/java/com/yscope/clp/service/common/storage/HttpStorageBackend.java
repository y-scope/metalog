package com.yscope.clp.service.common.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP storage backend.
 *
 * <p>Reads and writes files via an HTTP/HTTPS endpoint. Reads use GET, writes use PUT, and deletes
 * use DELETE. Useful for storage systems that expose an HTTP interface (e.g., WebDAV, custom REST
 * APIs, or presigned URL endpoints).
 */
@StorageBackendType(value = "http", requiresBucket = false)
public class HttpStorageBackend implements StorageBackend {
  private static final Logger logger = LoggerFactory.getLogger(HttpStorageBackend.class);

  private static final int CONNECT_TIMEOUT_MS = 10_000;
  private static final int READ_TIMEOUT_MS = 60_000;

  private final String backendName;
  private final String baseUrl;

  /**
   * Create an HttpStorageBackend.
   *
   * @param name human-readable backend name (e.g., "http")
   * @param baseUrl base URL for file access (e.g., "https://example.com/data")
   */
  public HttpStorageBackend(String name, String baseUrl) {
    this.backendName = name;
    // Strip trailing slash for consistent URL construction
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
  }

  /**
   * Create an HttpStorageBackend from a configuration map.
   *
   * <p>Used by {@link StorageBackendFactory} for reflection-based construction.
   *
   * @param name human-readable backend name
   * @param config configuration map; must contain {@code "baseUrl"}
   */
  public HttpStorageBackend(String name, Map<String, Object> config) {
    this(name, (String) config.get("baseUrl"));
  }

  @Override
  public String name() {
    return backendName;
  }

  @Override
  public String namespace() {
    return null;
  }

  @Override
  public void downloadToFile(String path, Path localPath)
      throws StorageException, ObjectNotFoundException {
    try {
      HttpURLConnection conn = openGetConnection(path);
      try (InputStream in = conn.getInputStream()) {
        Files.copy(in, localPath, StandardCopyOption.REPLACE_EXISTING);
      } finally {
        conn.disconnect();
      }
    } catch (IOException e) {
      throw new StorageException("Failed to download from HTTP: " + buildUrl(path), e);
    }
  }

  @Override
  public long uploadFromFile(String path, Path localPath) throws StorageException {
    try {
      long size = Files.size(localPath);
      HttpURLConnection conn = openConnection(path);
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setFixedLengthStreamingMode(size);
      try (InputStream in = Files.newInputStream(localPath);
          OutputStream out = conn.getOutputStream()) {
        in.transferTo(out);
      }
      checkWriteStatus(conn, path);
      conn.disconnect();
      return size;
    } catch (IOException e) {
      throw new StorageException("Failed to upload to HTTP: " + buildUrl(path), e);
    }
  }

  @Override
  public void deleteFile(String path) throws StorageException {
    try {
      HttpURLConnection conn = openConnection(path);
      conn.setRequestMethod("DELETE");
      int status = conn.getResponseCode();
      conn.disconnect();
      // 404 is fine — the file is already gone
      if (status != 404 && status >= 400) {
        throw new StorageException("HTTP " + status + " deleting: " + buildUrl(path));
      }
    } catch (IOException e) {
      throw new StorageException("Failed to delete from HTTP: " + buildUrl(path), e);
    }
  }

  @Override
  public void close() {
    logger.info("HttpStorageBackend closed (name={}, baseUrl={})", backendName, baseUrl);
  }

  private String buildUrl(String path) {
    if (path == null || path.isEmpty()) {
      return baseUrl;
    }
    return baseUrl + "/" + path;
  }

  private HttpURLConnection openConnection(String path) throws IOException {
    HttpURLConnection conn =
        (HttpURLConnection) URI.create(buildUrl(path)).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    return conn;
  }

  /**
   * Open a GET connection and check the response status.
   *
   * @return the connected {@link HttpURLConnection} (caller must disconnect)
   */
  private HttpURLConnection openGetConnection(String path)
      throws IOException, StorageException, ObjectNotFoundException {
    HttpURLConnection conn = openConnection(path);
    conn.setRequestMethod("GET");
    int status = conn.getResponseCode();
    if (status == 404) {
      conn.disconnect();
      throw new ObjectNotFoundException("HTTP 404: " + buildUrl(path));
    }
    if (status >= 400) {
      conn.disconnect();
      throw new StorageException("HTTP " + status + " reading: " + buildUrl(path));
    }
    return conn;
  }

  private void checkWriteStatus(HttpURLConnection conn, String path)
      throws IOException, StorageException {
    int status = conn.getResponseCode();
    if (status >= 400) {
      conn.disconnect();
      throw new StorageException("HTTP " + status + " writing: " + buildUrl(path));
    }
  }
}
