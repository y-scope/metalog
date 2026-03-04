package com.yscope.clp.service.node;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for NodeConfig YAML parsing. */
class NodeConfigTest {

  private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  @Test
  void testParseBasicConfig() throws IOException {
    String yaml =
        """
            node:
              name: test-node
              database:
                host: db.example.com
                port: 3307
                database: mydb
                user: admin
                password: secret
                poolSize: 30
              storage:
                defaultBackend: minio
                backends:
                  minio:
                    endpoint: http://s3.example.com:9000
                    accessKey: access123
                    secretKey: secret456
            units:
              - name: coordinator-1
                type: coordinator
                enabled: true
                config:
                  table: logs
                  kafka:
                    bootstrapServers: kafka:9092
                    groupId: group1
                    topic: topic1
              - name: worker-1
                type: worker
                enabled: true
                config:
                  table: logs
                  threads: 8
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);

    // Verify node settings
    assertEquals("test-node", config.getNode().getName());
    assertEquals("db.example.com", config.getNode().getDatabase().getHost());
    assertEquals(3307, config.getNode().getDatabase().getPort());
    assertEquals("mydb", config.getNode().getDatabase().getDatabase());
    assertEquals("admin", config.getNode().getDatabase().getUser());
    assertEquals("secret", config.getNode().getDatabase().getPassword());
    assertEquals(30, config.getNode().getDatabase().getPoolSize());

    // Verify storage settings
    assertEquals("minio", config.getNode().getStorage().getDefaultBackend());
    assertNotNull(config.getNode().getStorage().getBackends().get("minio"));
    Map<String, Object> minioBackend = config.getNode().getStorage().getBackends().get("minio");
    assertEquals("http://s3.example.com:9000", minioBackend.get("endpoint"));
    assertEquals("access123", minioBackend.get("accessKey"));
    assertEquals("secret456", minioBackend.get("secretKey"));

    // Verify units
    assertEquals(2, config.getUnits().size());

    // Coordinator unit
    NodeConfig.UnitDefinition coordinator = config.getUnits().get(0);
    assertEquals("coordinator-1", coordinator.getName());
    assertEquals("coordinator", coordinator.getType());
    assertTrue(coordinator.isEnabled());
    assertNotNull(coordinator.getConfig());
    assertEquals("logs", coordinator.getConfig().get("table"));

    // Worker unit
    NodeConfig.UnitDefinition worker = config.getUnits().get(1);
    assertEquals("worker-1", worker.getName());
    assertEquals("worker", worker.getType());
    assertTrue(worker.isEnabled());
    assertEquals("logs", worker.getConfig().get("table"));
    assertEquals(8, worker.getConfig().get("threads"));
  }

  @Test
  void testDefaultValues() throws IOException {
    String yaml =
        """
            node:
              name: minimal-node
            units: []
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);

    // Verify default database values
    assertEquals("localhost", config.getNode().getDatabase().getHost());
    assertEquals(3306, config.getNode().getDatabase().getPort());
    assertEquals("metalog_metastore", config.getNode().getDatabase().getDatabase());
    assertEquals("root", config.getNode().getDatabase().getUser());
    assertEquals("", config.getNode().getDatabase().getPassword());
    assertEquals(20, config.getNode().getDatabase().getPoolSize());
    assertEquals(5, config.getNode().getDatabase().getPoolMinIdle());

    // Verify default storage values
    assertEquals("minio", config.getNode().getStorage().getDefaultBackend());
    assertTrue(config.getNode().getStorage().getBackends().isEmpty());
  }

  @Test
  void testParseTablesSection() throws IOException {
    String yaml =
        """
            node:
              name: test
            tables:
              - name: clp_table
                displayName: Spark Logs
                active: true
                kafka:
                  topic: spark-ir
                  bootstrapServers: kafka:29092
                  recordTransformer: spark
                kafkaPollerEnabled: true
                consolidationEnabled: true
                schemaEvolutionEnabled: false
                schemaEvolutionMaxDimColumns: 100
                loopIntervalMs: 10000
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);

    assertEquals(1, config.getTables().size());
    NodeConfig.TableDefinition table = config.getTables().get(0);
    assertEquals("clp_table", table.getName());
    assertEquals("Spark Logs", table.getDisplayName());
    assertEquals(true, table.getActive());

    assertNotNull(table.getKafka());
    assertEquals("spark-ir", table.getKafka().getTopic());
    assertEquals("kafka:29092", table.getKafka().getBootstrapServers());
    assertEquals("spark", table.getKafka().getRecordTransformer());

    assertEquals(true, table.getKafkaPollerEnabled());
    assertEquals(true, table.getConsolidationEnabled());
    assertEquals(false, table.getSchemaEvolutionEnabled());
    assertEquals(100, table.getSchemaEvolutionMaxDimColumns());
    assertEquals(10000, table.getLoopIntervalMs());
  }

  @Test
  void testParseMinimalTableDefinition() throws IOException {
    String yaml =
        """
            node:
              name: test
            tables:
              - name: logs
                kafka:
                  topic: log-topic
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);

    assertEquals(1, config.getTables().size());
    NodeConfig.TableDefinition table = config.getTables().get(0);
    assertEquals("logs", table.getName());
    assertNull(table.getDisplayName());
    assertNull(table.getActive());
    assertEquals("log-topic", table.getKafka().getTopic());
    assertNull(table.getKafka().getBootstrapServers());
    assertNull(table.getKafka().getRecordTransformer());

    // All feature flags should be null (not specified)
    assertNull(table.getKafkaPollerEnabled());
    assertNull(table.getConsolidationEnabled());
    assertNull(table.getSchemaEvolutionEnabled());
    assertNull(table.getLoopIntervalMs());
  }

  @Test
  void testOmittedTablesSection() throws IOException {
    String yaml =
        """
            node:
              name: minimal-node
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);

    assertNotNull(config.getTables());
    assertTrue(config.getTables().isEmpty());
  }

  @Test
  void testGrpcConfigDefaults() throws IOException {
    String yaml =
        """
            node:
              name: test
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);
    NodeConfig.GrpcConfig grpc = config.getNode().getGrpc();

    assertTrue(grpc.isEnabled());
    assertEquals(9090, grpc.getPort());
    assertTrue(grpc.isIngestionEnabled());
    assertTrue(grpc.isQueryEnabled());
    assertTrue(grpc.isCatalogEnabled());
    assertTrue(grpc.isAdminEnabled());
  }

  @Test
  void testGrpcConfigCustom() throws IOException {
    String yaml =
        """
            node:
              name: test
              grpc:
                enabled: true
                port: 8080
                services:
                  ingestion: false
                  query: true
                  catalog: false
                  admin: true
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);
    NodeConfig.GrpcConfig grpc = config.getNode().getGrpc();

    assertTrue(grpc.isEnabled());
    assertEquals(8080, grpc.getPort());
    assertFalse(grpc.isIngestionEnabled());
    assertTrue(grpc.isQueryEnabled());
    assertFalse(grpc.isCatalogEnabled());
    assertTrue(grpc.isAdminEnabled());
  }

  @Test
  void testJdbcUrlGeneration() throws IOException {
    String yaml =
        """
            node:
              name: test
              database:
                host: myhost
                port: 3308
                database: testdb
            units: []
            """;

    NodeConfig config = mapper.readValue(yaml, NodeConfig.class);
    String jdbcUrl = config.getNode().getDatabase().getJdbcUrl();

    assertTrue(jdbcUrl.startsWith("jdbc:mariadb://myhost:3308/testdb"));
    assertTrue(jdbcUrl.contains("rewriteBatchedStatements=true"));
    assertTrue(jdbcUrl.contains("cachePrepStmts=true"));
  }
}
