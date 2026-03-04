package com.yscope.clp.service.common.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yscope.clp.service.common.config.db.DatabaseTypeDetector;
import com.yscope.clp.service.common.config.db.DatabaseTypeDetector.DatabaseInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ServiceConfig Unit Tests")
class ServiceConfigTest {

  private final List<String> setProperties = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (String key : setProperties) {
      System.clearProperty(key);
    }
    setProperties.clear();
  }

  private void setProperty(String key, String value) {
    System.setProperty(key, value);
    setProperties.add(key);
  }

  /**
   * Create a mock DatabaseInfo since the constructor is package-private to the db package.
   */
  private static DatabaseInfo mockDatabaseInfo(
      DatabaseTypeDetector.DatabaseType type,
      DatabaseTypeDetector.CompressionType compression,
      boolean supportsPartitioning) {
    DatabaseInfo info = mock(DatabaseInfo.class);
    when(info.getType()).thenReturn(type);
    when(info.getRecommendedCompression()).thenReturn(compression);
    when(info.supportsPartitioning()).thenReturn(supportsPartitioning);
    return info;
  }

  // ==========================================
  // System Property Overrides
  // ==========================================

  @Nested
  @DisplayName("System Property Overrides")
  class SystemPropertyOverrides {

    @Test
    void testDatabaseHost_overriddenBySystemProperty() {
      setProperty("db.host", "my-db-host");
      ServiceConfig config = new ServiceConfig();
      assertEquals("my-db-host", config.getDatabaseHost());
    }

    @Test
    void testDatabasePort_overriddenBySystemProperty() {
      setProperty("db.port", "5432");
      ServiceConfig config = new ServiceConfig();
      assertEquals(5432, config.getDatabasePort());
    }

    @Test
    void testKafkaBootstrapServers_overriddenBySystemProperty() {
      setProperty("kafka.bootstrap.servers", "broker1:9092,broker2:9092");
      ServiceConfig config = new ServiceConfig();
      assertEquals("broker1:9092,broker2:9092", config.getKafkaBootstrapServers());
    }

    @Test
    void testDeletionEnabled_overriddenBySystemProperty() {
      setProperty("coordinator.deletion.enabled", "true");
      ServiceConfig config = new ServiceConfig();
      assertTrue(config.isDeletionEnabled());
    }
  }

  // ==========================================
  // Compression
  // ==========================================

  @Nested
  @DisplayName("Compression")
  class Compression {

    @Test
    void testGetCompression_explicitLz4_returnsLz4() {
      setProperty("db.compression", "lz4");
      ServiceConfig config = new ServiceConfig();
      assertEquals("lz4", config.getCompression());
    }

    @Test
    void testGetCompression_explicitNone_returnsNone() {
      setProperty("db.compression", "none");
      ServiceConfig config = new ServiceConfig();
      assertEquals("none", config.getCompression());
    }

    @Test
    void testGetCompression_invalidValue_fallsBackToDefault() {
      setProperty("db.compression", "zstd");
      ServiceConfig config = new ServiceConfig();
      // Falls back to default lz4 (no detectedDatabaseInfo)
      assertEquals("lz4", config.getCompression());
    }

    @Test
    void testGetCompression_caseInsensitive() {
      setProperty("db.compression", "  LZ4  ");
      ServiceConfig config = new ServiceConfig();
      assertEquals("lz4", config.getCompression());
    }

    @Test
    void testGetCompressionClause_lz4_returnsClause() {
      setProperty("db.compression", "lz4");
      ServiceConfig config = new ServiceConfig();
      assertEquals("COMPRESSION='lz4'", config.getCompressionClause());
    }

    @Test
    void testGetCompressionClause_none_returnsEmpty() {
      setProperty("db.compression", "none");
      ServiceConfig config = new ServiceConfig();
      assertEquals("", config.getCompressionClause());
    }

    @Test
    void testIsLz4Compression_withLz4_returnsTrue() {
      setProperty("db.compression", "lz4");
      ServiceConfig config = new ServiceConfig();
      assertTrue(config.isLz4Compression());
    }

    @Test
    void testIsLz4Compression_withNone_returnsFalse() {
      setProperty("db.compression", "none");
      ServiceConfig config = new ServiceConfig();
      assertFalse(config.isLz4Compression());
    }

    @Test
    void testGetCompression_withDetectedDatabaseInfo_usesRecommended() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.VITESS,
          DatabaseTypeDetector.CompressionType.NONE, false);
      config.setDetectedDatabaseInfo(info);
      // Vitess recommends NONE
      assertEquals("none", config.getCompression());
    }

    @Test
    void testGetCompression_explicitOverridesDetected() {
      setProperty("db.compression", "lz4");
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.VITESS,
          DatabaseTypeDetector.CompressionType.NONE, false);
      config.setDetectedDatabaseInfo(info);
      // Explicit value overrides auto-detected
      assertEquals("lz4", config.getCompression());
    }
  }

  // ==========================================
  // Database Type
  // ==========================================

  @Nested
  @DisplayName("Database Type")
  class DatabaseType {

    @Test
    void testGetDatabaseType_withoutDetection_returnsMySQL() {
      ServiceConfig config = new ServiceConfig();
      assertEquals("mysql", config.getDatabaseType());
    }

    @Test
    void testGetDatabaseType_withMariaDB_returnsMariadb() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.MARIADB,
          DatabaseTypeDetector.CompressionType.LZ4, true);
      config.setDetectedDatabaseInfo(info);
      assertEquals("mariadb", config.getDatabaseType());
    }

    @Test
    void testIsMariaDB_withMariaDB_returnsTrue() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.MARIADB,
          DatabaseTypeDetector.CompressionType.LZ4, true);
      config.setDetectedDatabaseInfo(info);
      assertTrue(config.isMariaDB());
    }

    @Test
    void testIsMariaDB_withMySQL_returnsFalse() {
      ServiceConfig config = new ServiceConfig();
      assertFalse(config.isMariaDB());
    }

    @Test
    void testGetDetectedDatabaseInfo_beforeDetection_returnsNull() {
      ServiceConfig config = new ServiceConfig();
      assertNull(config.getDetectedDatabaseInfo());
    }

    @Test
    void testGetDetectedDatabaseInfo_afterDetection_returnsInfo() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.MYSQL,
          DatabaseTypeDetector.CompressionType.LZ4, true);
      config.setDetectedDatabaseInfo(info);
      assertNotNull(config.getDetectedDatabaseInfo());
      assertEquals(DatabaseTypeDetector.DatabaseType.MYSQL, config.getDetectedDatabaseInfo().getType());
    }
  }

  // ==========================================
  // Worker ID
  // ==========================================

  @Nested
  @DisplayName("Worker ID")
  class WorkerId {

    @Test
    void testGetWorkerId_withSystemProperty_returnsProperty() {
      setProperty("worker.id", "my-worker-1");
      ServiceConfig config = new ServiceConfig();
      assertEquals("my-worker-1", config.getWorkerId());
    }
  }

  // ==========================================
  // Partition Manager
  // ==========================================

  @Nested
  @DisplayName("Partition Manager")
  class PartitionManager {

    @Test
    void testIsPartitionManagerThreadEnabled_withoutDetection_defaultsTrue() {
      ServiceConfig config = new ServiceConfig();
      assertTrue(config.isPartitionManagerThreadEnabled());
    }

    @Test
    void testIsPartitionManagerThreadEnabled_withVitess_returnsFalse() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.VITESS,
          DatabaseTypeDetector.CompressionType.NONE, false);
      config.setDetectedDatabaseInfo(info);
      assertFalse(config.isPartitionManagerThreadEnabled());
    }

    @Test
    void testIsPartitionManagerThreadEnabled_explicitTrue_overridesVitess() {
      setProperty("coordinator.partition.manager.thread.enabled", "true");
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.VITESS,
          DatabaseTypeDetector.CompressionType.NONE, false);
      config.setDetectedDatabaseInfo(info);
      assertTrue(config.isPartitionManagerThreadEnabled());
    }

    @Test
    void testIsPartitionManagerThreadEnabled_explicitFalse_overridesMySQL() {
      setProperty("coordinator.partition.manager.thread.enabled", "false");
      ServiceConfig config = new ServiceConfig();
      assertFalse(config.isPartitionManagerThreadEnabled());
    }

    @Test
    void testIsPartitioningSupported_withMySQL_returnsTrue() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.MYSQL,
          DatabaseTypeDetector.CompressionType.LZ4, true);
      config.setDetectedDatabaseInfo(info);
      assertTrue(config.isPartitioningSupported());
    }

    @Test
    void testIsPartitioningSupported_withVitess_returnsFalse() {
      ServiceConfig config = new ServiceConfig();
      DatabaseInfo info = mockDatabaseInfo(
          DatabaseTypeDetector.DatabaseType.VITESS,
          DatabaseTypeDetector.CompressionType.NONE, false);
      config.setDetectedDatabaseInfo(info);
      assertFalse(config.isPartitioningSupported());
    }
  }

  // ==========================================
  // JDBC URL
  // ==========================================

  @Nested
  @DisplayName("JDBC URL")
  class JdbcUrl {

    @Test
    void testGetJdbcUrl_usesConfiguredHostPortDatabase() {
      setProperty("db.host", "db.example.com");
      setProperty("db.port", "3307");
      setProperty("db.database", "mydb");
      ServiceConfig config = new ServiceConfig();

      String url = config.getJdbcUrl();

      assertTrue(url.contains("db.example.com"));
      assertTrue(url.contains("3307"));
      assertTrue(url.contains("mydb"));
    }

    @Test
    void testGetJdbcUrl_defaultValues_containsLocalhostAndDefaults() {
      ServiceConfig config = new ServiceConfig();
      String url = config.getJdbcUrl();
      assertTrue(url.contains("localhost"));
      assertTrue(url.contains("3306"));
      assertTrue(url.contains("clp_metastore"));
    }
  }

  // ==========================================
  // Storage Backends
  // ==========================================

  @Nested
  @DisplayName("Storage Backends")
  class StorageBackends {

    @Test
    void testGetStorageBackends_legacyMinioProperties_returnsSingleBackend() {
      ServiceConfig config = new ServiceConfig();
      Map<String, ObjectStorageConfig> backends = config.getStorageBackends();
      assertEquals(1, backends.size());
      assertTrue(backends.containsKey("minio"));
    }

    @Test
    void testGetStorageBackends_legacyDefaults_haveExpectedValues() {
      ServiceConfig config = new ServiceConfig();
      ObjectStorageConfig minio = config.getStorageBackends().get("minio");
      assertEquals("http://localhost:9000", minio.getEndpoint());
      assertEquals("minioadmin", minio.getAccessKey());
      assertEquals("minioadmin", minio.getSecretKey());
      assertEquals("us-east-1", minio.getRegion());
      assertTrue(minio.isForcePathStyle());
    }

    @Test
    void testGetStorageBackends_newStyleProperties_usesNewProperties() {
      setProperty("storage.backends.minio.endpoint", "http://minio:9000");
      setProperty("storage.backends.minio.access.key", "mykey");
      setProperty("storage.backends.minio.secret.key", "mysecret");
      ServiceConfig config = new ServiceConfig();

      Map<String, ObjectStorageConfig> backends = config.getStorageBackends();
      assertEquals(1, backends.size());
      ObjectStorageConfig minio = backends.get("minio");
      assertEquals("http://minio:9000", minio.getEndpoint());
      assertEquals("mykey", minio.getAccessKey());
      assertEquals("mysecret", minio.getSecretKey());
    }
  }

  // ==========================================
  // Bucket Name Fallback
  // ==========================================

  @Nested
  @DisplayName("Bucket Name Fallback")
  class BucketNameFallback {

    @Test
    void testStorageIrBucket_newKeyOverridesLegacy() {
      setProperty("storage.ir.bucket", "new-ir-bucket");
      setProperty("minio.ir.bucket", "legacy-ir-bucket");
      ServiceConfig config = new ServiceConfig();
      assertEquals("new-ir-bucket", config.getStorageIrBucket());
    }

    @Test
    void testStorageIrBucket_fallsBackToLegacyMinioKey() {
      setProperty("minio.ir.bucket", "legacy-ir-bucket");
      ServiceConfig config = new ServiceConfig();
      assertEquals("legacy-ir-bucket", config.getStorageIrBucket());
    }

    @Test
    void testStorageArchiveBucket_newKeyOverridesLegacy() {
      setProperty("storage.archive.bucket", "new-archive-bucket");
      setProperty("minio.archive.bucket", "legacy-archive-bucket");
      ServiceConfig config = new ServiceConfig();
      assertEquals("new-archive-bucket", config.getStorageArchiveBucket());
    }

    @Test
    void testStorageArchiveBucket_fallsBackToLegacyMinioKey() {
      setProperty("minio.archive.bucket", "legacy-archive-bucket");
      ServiceConfig config = new ServiceConfig();
      assertEquals("legacy-archive-bucket", config.getStorageArchiveBucket());
    }
  }

  // ==========================================
  // Planner and Backpressure Config
  // ==========================================

  @Nested
  @DisplayName("Planner Config")
  class PlannerConfig {

    @Test
    void testPlannerConfig_defaults() {
      ServiceConfig config = new ServiceConfig();
      assertEquals(5000, config.getPlannerIntervalMs());
      assertEquals(1000, config.getBackpressureHighWatermark());
      assertEquals(600000, config.getTaskTimeoutMs());
    }

    @Test
    void testPlannerIntervalMs_overriddenBySystemProperty() {
      setProperty("coordinator.planner.interval.ms", "10000");
      ServiceConfig config = new ServiceConfig();
      assertEquals(10000, config.getPlannerIntervalMs());
    }
  }
}
