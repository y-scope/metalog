package com.yscope.clp.service.query.api.server.vertx.grpc;

import com.yscope.clp.service.coordinator.grpc.proto.CoordinatorServiceGrpc;
import com.yscope.clp.service.coordinator.grpc.proto.KafkaConfig;
import com.yscope.clp.service.coordinator.grpc.proto.RegisterTableRequest;
import com.yscope.clp.service.coordinator.grpc.proto.RegisterTableResponse;
import com.yscope.clp.service.metastore.schema.TableProvisioner;
import com.yscope.clp.service.node.CoordinatorRegistry;
import com.yscope.clp.service.node.NodeConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service for coordinator management operations.
 *
 * <p>Exposes runtime table registration so operators can add Kafka-ingested tables without editing
 * {@code node.yaml} or restarting the node. All operations are fully idempotent.
 *
 * <p>Example usage with grpcurl:
 *
 * <pre>
 * grpcurl -plaintext -d '{
 *   "table_name": "my_spark_logs",
 *   "kafka": {"topic": "spark-ir", "bootstrap_servers": "kafka:29092"}
 * }' localhost:9090 \
 *   com.yscope.clp.service.coordinator.grpc.CoordinatorService/RegisterTable
 * # → {"tableName":"my_spark_logs","created":true}
 * </pre>
 */
public class CoordinatorGrpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcService.class);

  private final DataSource dataSource;

  public CoordinatorGrpcService(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void registerTable(
      RegisterTableRequest request, StreamObserver<RegisterTableResponse> responseObserver) {
    try {
      // 1. Validate
      if (request.getTableName().isBlank()) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("table_name is required").asRuntimeException());
        return;
      }
      if (!request.hasKafka()) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("kafka config is required")
                .asRuntimeException());
        return;
      }
      KafkaConfig kafka = request.getKafka();
      if (kafka.getTopic().isBlank()) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("kafka.topic is required")
                .asRuntimeException());
        return;
      }
      if (kafka.getBootstrapServers().isBlank()) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT
                .withDescription("kafka.bootstrap_servers is required")
                .asRuntimeException());
        return;
      }

      String tableName = request.getTableName();

      // 2. Check pre-existence (before provisioning so `created` reflects true new-ness)
      boolean alreadyExists = tableExists(tableName);

      // 3. Convert proto → NodeConfig.TableDefinition
      NodeConfig.TableDefinition def = toTableDefinition(request);

      // 4. Write registry rows (idempotent via INSERT IGNORE / ON DUPLICATE KEY UPDATE)
      CoordinatorRegistry.upsertTables(dataSource, List.of(def));

      // 5. Provision physical table (no-op if already exists)
      TableProvisioner.ensureTable(dataSource, tableName);

      // 6. Return response
      responseObserver.onNext(
          RegisterTableResponse.newBuilder()
              .setTableName(tableName)
              .setCreated(!alreadyExists)
              .build());
      responseObserver.onCompleted();

    } catch (SQLException e) {
      LOG.error("RegisterTable failed for table={}", request.getTableName(), e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Database error: " + e.getMessage())
              .asRuntimeException());
    }
  }

  private boolean tableExists(String tableName) throws SQLException {
    String sql =
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES"
            + " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, tableName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  private static NodeConfig.TableDefinition toTableDefinition(RegisterTableRequest request) {
    NodeConfig.TableDefinition def = new NodeConfig.TableDefinition();
    def.setName(request.getTableName());

    String displayName = request.getDisplayName();
    def.setDisplayName(displayName.isBlank() ? request.getTableName() : displayName);

    KafkaConfig kafka = request.getKafka();
    NodeConfig.TableKafkaDefinition kafkaDef = new NodeConfig.TableKafkaDefinition();
    kafkaDef.setTopic(kafka.getTopic());
    kafkaDef.setBootstrapServers(kafka.getBootstrapServers());
    if (!kafka.getRecordTransformer().isBlank()) {
      kafkaDef.setRecordTransformer(kafka.getRecordTransformer());
    }
    def.setKafka(kafkaDef);

    // Default kafkaPollerEnabled to true when kafka config is supplied (mirrors YAML behaviour)
    def.setKafkaPollerEnabled(
        request.hasKafkaPollerEnabled() ? request.getKafkaPollerEnabled() : true);

    if (request.hasConsolidationEnabled()) {
      def.setConsolidationEnabled(request.getConsolidationEnabled());
    }
    if (request.hasSchemaEvolutionEnabled()) {
      def.setSchemaEvolutionEnabled(request.getSchemaEvolutionEnabled());
    }
    if (request.hasLoopIntervalMs()) {
      def.setLoopIntervalMs(request.getLoopIntervalMs());
    }

    return def;
  }
}
