package com.yscope.metalog.common.config.db;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yscope.metalog.common.config.db.DatabaseTypeDetector.CompressionType;
import com.yscope.metalog.common.config.db.DatabaseTypeDetector.DatabaseInfo;
import com.yscope.metalog.common.config.db.DatabaseTypeDetector.DatabaseType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

@DisplayName("DatabaseTypeDetector Unit Tests")
class DatabaseTypeDetectorTest {

  // ==========================================
  // Pure static methods (no mocking)
  // ==========================================

  @Nested
  @DisplayName("toSimpleTypeName")
  class ToSimpleTypeName {

    @Test
    void testMariaDb_returnsMariadb() {
      assertEquals("mariadb", DatabaseTypeDetector.toSimpleTypeName(DatabaseType.MARIADB));
    }

    @Test
    void testVitess_returnsVitess() {
      assertEquals("vitess", DatabaseTypeDetector.toSimpleTypeName(DatabaseType.VITESS));
    }

    @ParameterizedTest
    @EnumSource(
        value = DatabaseType.class,
        names = {"MYSQL", "AURORA", "TIDB", "PERCONA", "UNKNOWN"})
    void testMySqlCompatible_returnsMysql(DatabaseType type) {
      assertEquals("mysql", DatabaseTypeDetector.toSimpleTypeName(type));
    }
  }

  @Nested
  @DisplayName("toDisplayName")
  class ToDisplayName {

    @ParameterizedTest
    @CsvSource({
      "MYSQL, MySQL",
      "MARIADB, MariaDB",
      "AURORA, Amazon Aurora",
      "TIDB, TiDB",
      "PERCONA, Percona Server",
      "VITESS, Vitess",
      "UNKNOWN, Unknown"
    })
    void testDisplayName(DatabaseType type, String expected) {
      assertEquals(expected, DatabaseTypeDetector.toDisplayName(type));
    }
  }

  @Nested
  @DisplayName("supportsPartitioning")
  class SupportsPartitioning {

    @Test
    void testVitess_doesNotSupportPartitioning() {
      assertFalse(DatabaseTypeDetector.supportsPartitioning(DatabaseType.VITESS));
    }

    @ParameterizedTest
    @EnumSource(value = DatabaseType.class, names = "VITESS", mode = EnumSource.Mode.EXCLUDE)
    void testAllOthers_supportPartitioning(DatabaseType type) {
      assertTrue(DatabaseTypeDetector.supportsPartitioning(type));
    }
  }

  // ==========================================
  // DatabaseInfo
  // ==========================================

  @Nested
  @DisplayName("DatabaseInfo")
  class DatabaseInfoTests {

    @Test
    void testMySql_hasLz4Compression() {
      DatabaseInfo info = new DatabaseInfo(DatabaseType.MYSQL, "8.0.33", 8, 0);
      assertEquals(CompressionType.LZ4, info.getRecommendedCompression());
      assertTrue(info.supportsLz4());
      assertTrue(info.supportsPartitioning());
    }

    @Test
    void testTiDb_hasNoCompression() {
      DatabaseInfo info = new DatabaseInfo(DatabaseType.TIDB, "5.7.25-TiDB", 5, 7);
      assertEquals(CompressionType.NONE, info.getRecommendedCompression());
      assertFalse(info.supportsLz4());
    }

    @Test
    void testVitess_hasNoCompression_noPartitioning() {
      DatabaseInfo info = new DatabaseInfo(DatabaseType.VITESS, "5.7.9-vitess", 5, 7);
      assertEquals(CompressionType.NONE, info.getRecommendedCompression());
      assertFalse(info.supportsLz4());
      assertFalse(info.supportsPartitioning());
    }

  }

  // ==========================================
  // Version detection (needs Connection mock)
  // ==========================================

  @Nested
  @DisplayName("detectInfo via Connection")
  class DetectInfo {

    private Connection mockConnectionWithVersion(String version) throws SQLException {
      Connection conn = mock(Connection.class);
      Statement stmt = mock(Statement.class);
      ResultSet rs = mock(ResultSet.class);

      when(conn.createStatement()).thenReturn(stmt);
      when(stmt.executeQuery("SELECT VERSION()")).thenReturn(rs);
      when(rs.next()).thenReturn(true);
      when(rs.getString(1)).thenReturn(version);
      return conn;
    }

    @ParameterizedTest
    @CsvSource({
      "'10.6.12-MariaDB', MARIADB, 10, 6",
      "'8.0.33', MYSQL, 8, 0",
      "'8.0.mysql_aurora.3.02.0', AURORA, 8, 0",
      "'5.7.25-TiDB-v6.1.0', TIDB, 5, 7",
      "'8.0.32-24-Percona Server', PERCONA, 8, 0",
      "'5.7.9-vitess-14.0.0', VITESS, 5, 7"
    })
    void testDetectInfo_versionStrings(
        String version, DatabaseType expectedType, int expectedMajor, int expectedMinor)
        throws SQLException {
      Connection conn = mockConnectionWithVersion(version);
      DatabaseInfo info = DatabaseTypeDetector.detectInfo(conn);

      assertEquals(expectedType, info.getType());
      assertEquals(expectedMajor, info.getMajorVersion());
      assertEquals(expectedMinor, info.getMinorVersion());
    }

    @Test
    void testDetectInfo_nullVersion_fallsBackToMetadata() throws SQLException {
      Connection conn = mock(Connection.class);
      Statement stmt = mock(Statement.class);
      ResultSet rs = mock(ResultSet.class);
      DatabaseMetaData meta = mock(DatabaseMetaData.class);

      // VERSION() returns null
      when(conn.createStatement()).thenReturn(stmt);
      when(stmt.executeQuery("SELECT VERSION()")).thenReturn(rs);
      when(rs.next()).thenReturn(false);

      // Fallback to metadata
      when(conn.getMetaData()).thenReturn(meta);
      when(meta.getDatabaseProductName()).thenReturn("MariaDB");
      when(meta.getDatabaseProductVersion()).thenReturn(null);

      DatabaseInfo info = DatabaseTypeDetector.detectInfo(conn);
      assertEquals(DatabaseType.MARIADB, info.getType());
    }

    @Test
    void testDetectInfo_versionQueryFails_fallsBackToMetadata() throws SQLException {
      Connection conn = mock(Connection.class);
      Statement stmt = mock(Statement.class);
      DatabaseMetaData meta = mock(DatabaseMetaData.class);

      // VERSION() throws
      when(conn.createStatement()).thenReturn(stmt);
      when(stmt.executeQuery("SELECT VERSION()")).thenThrow(new SQLException("denied"));

      // Fallback
      when(conn.getMetaData()).thenReturn(meta);
      when(meta.getDatabaseProductVersion()).thenReturn("10.11.4-MariaDB");

      DatabaseInfo info = DatabaseTypeDetector.detectInfo(conn);
      assertEquals(DatabaseType.MARIADB, info.getType());
    }

    @Test
    void testDetectInfo_allFallbacksFail_defaultsToMySql() throws SQLException {
      Connection conn = mock(Connection.class);
      Statement stmt = mock(Statement.class);
      DatabaseMetaData meta = mock(DatabaseMetaData.class);

      when(conn.createStatement()).thenReturn(stmt);
      when(stmt.executeQuery("SELECT VERSION()")).thenThrow(new SQLException("denied"));
      when(conn.getMetaData()).thenReturn(meta);
      when(meta.getDatabaseProductName()).thenReturn(null);
      when(meta.getDatabaseProductVersion()).thenReturn(null);

      DatabaseInfo info = DatabaseTypeDetector.detectInfo(conn);
      assertEquals(DatabaseType.MYSQL, info.getType());
    }
  }

  // ==========================================
  // DataSource-level detection
  // ==========================================

  @Nested
  @DisplayName("detect via DataSource")
  class DetectViaDataSource {

    @Test
    void testDetect_nullConnection_returnsUnknown() throws SQLException {
      DataSource ds = mock(DataSource.class);
      when(ds.getConnection()).thenReturn(null);

      assertEquals(DatabaseType.UNKNOWN, DatabaseTypeDetector.detect(ds));
    }

    @Test
    void testDetect_connectionFails_returnsUnknown() throws SQLException {
      DataSource ds = mock(DataSource.class);
      when(ds.getConnection()).thenThrow(new SQLException("connection refused"));

      assertEquals(DatabaseType.UNKNOWN, DatabaseTypeDetector.detect(ds));
    }

  }
}
