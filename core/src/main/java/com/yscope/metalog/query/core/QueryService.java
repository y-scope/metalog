package com.yscope.metalog.query.core;

import com.yscope.metalog.common.config.db.DSLContextFactory;
import com.yscope.metalog.metastore.SqlIdentifiers;
import com.yscope.metalog.metastore.schema.ColumnRegistry;
import com.yscope.metalog.metastore.schema.ColumnRegistryProvider;
import com.yscope.metalog.query.api.ApiServerConfig;
import com.yscope.metalog.query.core.splits.NoOpSketchEvaluator;
import com.yscope.metalog.query.core.splits.SplitQueryEngine;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared query logic for all API protocols.
 *
 * <p>Handles database queries with caching. All protocols (REST, gRPC, MCP) use this service—each
 * protocol handler is a thin adapter.
 */
public class QueryService {

  private static final Logger LOG = LoggerFactory.getLogger(QueryService.class);

  private final HikariDataSource dataSource;
  private final CacheService cacheService;
  private final ColumnRegistryProvider registryProvider;
  private final SplitQueryEngine splitQueryEngine;

  public QueryService(ApiServerConfig.DataSourceConfig config, CacheService cacheService) {
    this.dataSource = new HikariDataSource(config.toHikariConfig());
    this.cacheService = cacheService;
    DSLContext dsl = DSLContextFactory.create(dataSource);
    this.registryProvider = new ColumnRegistryProvider(dsl);
    this.splitQueryEngine =
        new SplitQueryEngine(
            dsl,
            registryProvider,
            cacheService,
            new NoOpSketchEvaluator());
  }

  /** Check if database connection is healthy. */
  public boolean isHealthy() {
    try (Connection conn = dataSource.getConnection()) {
      return conn.isValid(1);
    } catch (SQLException e) {
      LOG.warn("Health check failed", e);
      return false;
    }
  }

  /** Get file metadata by ID. */
  public Optional<FileMetadata> getFileById(String table, long fileId) {
    String cacheKey = "file:" + table + ":" + fileId;

    return cacheService.getFile(
        cacheKey,
        () -> {
          String sql =
              "SELECT * FROM " + SqlIdentifiers.requireValidTableName(table) + " WHERE id = ?";
          ColumnRegistry registry = getRegistry(table);

          try (Connection conn = dataSource.getConnection();
              PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, fileId);
            try (ResultSet rs = stmt.executeQuery()) {
              if (rs.next()) {
                return Optional.of(mapToFileMetadata(rs, registry));
              }
            }
          } catch (SQLException e) {
            LOG.error("Failed to get file by ID: {}", fileId, e);
          }
          return Optional.empty();
        });
  }

  /** Query files with filters. */
  public QueryResult queryFiles(FileQuery query) {
    // TODO: Implement with QueryBuilder from core module
    // For now, return empty result
    return new QueryResult(List.of(), null, true);
  }

  /** List all tables. */
  public List<String> listTables() {
    return cacheService.getSchema(
        "tables",
        () -> {
          List<String> tables = new ArrayList<>();
          String sql = "SHOW TABLES LIKE 'clp_%'";

          try (Connection conn = dataSource.getConnection();
              PreparedStatement stmt = conn.prepareStatement(sql);
              ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
              tables.add(rs.getString(1));
            }
          } catch (SQLException e) {
            LOG.error("Failed to list tables", e);
          }
          return tables;
        });
  }

  /** Get table schema (columns and types). */
  public Map<String, String> getTableSchema(String table) {
    String cacheKey = "schema:" + table;

    return cacheService.getSchema(
        cacheKey,
        () -> {
          Map<String, String> schema = new HashMap<>();
          String sql = "DESCRIBE " + SqlIdentifiers.requireValidTableName(table);

          try (Connection conn = dataSource.getConnection();
              PreparedStatement stmt = conn.prepareStatement(sql);
              ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
              schema.put(rs.getString("Field"), rs.getString("Type"));
            }
          } catch (SQLException e) {
            LOG.error("Failed to get table schema: {}", table, e);
          }
          return schema;
        });
  }

  /** Get dimension field names for a table. Uses registry to resolve actual names. */
  public List<String> getDimensions(String table) {
    ColumnRegistry registry = getRegistry(table);
    if (registry != null) {
      return registry.getActiveDimColumns().stream()
          .map(col -> resolveOrDefault(registry.dimColumnToKey(col), col))
          .toList();
    }
    return getTableSchema(table).keySet().stream().filter(col -> col.startsWith("dim_")).toList();
  }

  /** Get aggregate field names for a table. Uses registry to resolve actual names. */
  public List<String> getAggs(String table) {
    ColumnRegistry registry = getRegistry(table);
    if (registry != null) {
      return registry.getActiveAggColumns().stream()
          .map(col -> resolveOrDefault(registry.aggColumnToKey(col), col))
          .toList();
    }
    return getTableSchema(table).keySet().stream().filter(col -> col.startsWith("agg_")).toList();
  }

  /** List dimension fields with full metadata for a table. */
  public List<DimensionDetail> listDimensionDetails(String table) {
    ColumnRegistry registry = getRegistry(table);
    if (registry == null) {
      return List.of();
    }
    return registry.getActiveDimEntries().stream()
        .map(
            e ->
                new DimensionDetail(
                    e.dimKey(), e.baseType(), e.width() != null ? e.width() : 0, e.aliasColumn()))
        .sorted(Comparator.comparing(DimensionDetail::name))
        .toList();
  }

  /** List aggregate fields with full metadata for a table. */
  public List<AggDetail> listAggDetails(String table) {
    ColumnRegistry registry = getRegistry(table);
    if (registry == null) {
      return List.of();
    }
    return registry.getActiveAggEntries().stream()
        .map(
            e ->
                new AggDetail(
                    e.aggKey(), e.aggValue(), e.aggregationType(), e.valueType(), e.aliasColumn()))
        .sorted(Comparator.comparing(AggDetail::name).thenComparing(AggDetail::value))
        .toList();
  }

  /** List active sketch slots for a table. Returns empty list if sketch registry doesn't exist. */
  public List<SketchDetail> listSketches(String table) {
    return cacheService.getSketch("sketches:" + table, () -> fetchSketches(table));
  }

  private List<SketchDetail> fetchSketches(String table) {
    String validatedTable = SqlIdentifiers.requireValidTableName(table);
    String sql =
        "SELECT sketch_key FROM _sketch_registry WHERE table_name = ? AND status = 'ACTIVE' ORDER BY sketch_key";

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, validatedTable);
      try (ResultSet rs = stmt.executeQuery()) {
        List<SketchDetail> sketches = new ArrayList<>();
        while (rs.next()) {
          sketches.add(new SketchDetail(rs.getString(1)));
        }
        return sketches;
      }
    } catch (SQLException e) {
      // Table may not exist in lightweight test stacks
      LOG.debug("Failed to list sketches for table {}", table, e);
      return List.of();
    }
  }

  /** Get the query engine for direct use by gRPC service. */
  public SplitQueryEngine getSplitQueryEngine() {
    return splitQueryEngine;
  }

  public void close() {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
    }
  }

  private ColumnRegistry getRegistry(String table) {
    return registryProvider.get(table);
  }

  private FileMetadata mapToFileMetadata(ResultSet rs, ColumnRegistry registry)
      throws SQLException {
    return new FileMetadata(
        rs.getLong("id"),
        rs.getString("clp_ir_path"),
        rs.getLong("min_timestamp"),
        rs.getLong("max_timestamp"),
        rs.getString("state"),
        extractDimensions(rs, registry),
        extractAggs(rs, registry));
  }

  private Map<String, String> extractDimensions(ResultSet rs, ColumnRegistry registry)
      throws SQLException {
    Map<String, String> dimensions = new HashMap<>();
    var metaData = rs.getMetaData();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String colName = metaData.getColumnName(i);
      // Include dim_fXX placeholder columns plus any column registered as a dim alias.
      boolean isDim =
          colName.startsWith("dim_")
              || (registry != null && registry.dimColumnToKey(colName) != null);
      if (isDim) {
        String value = rs.getString(i);
        if (value != null) {
          String resolved = registry != null ? registry.dimColumnToKey(colName) : null;
          dimensions.put(resolveOrDefault(resolved, colName), value);
        }
      }
    }
    return dimensions;
  }

  private Map<String, Long> extractAggs(ResultSet rs, ColumnRegistry registry) throws SQLException {
    Map<String, Long> aggs = new HashMap<>();
    var metaData = rs.getMetaData();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String colName = metaData.getColumnName(i);
      if (colName.startsWith("agg_")) {
        long value = rs.getLong(i);
        if (!rs.wasNull()) {
          String resolved = registry != null ? registry.aggColumnToKey(colName) : null;
          aggs.put(resolveOrDefault(resolved, colName), value);
        }
      }
    }
    return aggs;
  }

  /** Return resolved if non-null, otherwise the default. */
  private static String resolveOrDefault(String resolved, String defaultValue) {
    return resolved != null ? resolved : defaultValue;
  }

  // --- DTOs ---

  public record DimensionDetail(String name, String type, int width, String aliasColumn) {}

  public record AggDetail(
      String name,
      String value,
      com.yscope.metalog.metastore.model.AggregationType aggregationType,
      com.yscope.metalog.metastore.model.AggValueType valueType,
      String aliasColumn) {}

  public record SketchDetail(String name) {}

  public record FileMetadata(
      long fileId,
      String objectPath,
      long tsMin,
      long tsMax,
      String lifecycleState,
      Map<String, String> dimensions,
      Map<String, Long> aggs) {}

  public record FileQuery(
      String table,
      Map<String, String> dimensionFilters,
      Long tsMin,
      Long tsMax,
      Map<String, Long> aggFilters,
      String orderBy,
      int limit,
      int offset) {}

  public record QueryResult(List<FileMetadata> results, String cursor, boolean done) {}
}
