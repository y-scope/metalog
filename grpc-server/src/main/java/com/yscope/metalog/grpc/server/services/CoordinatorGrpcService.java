package com.yscope.metalog.grpc.server.services;

import com.yscope.metalog.coordinator.TableRegistrationService;
import com.yscope.metalog.coordinator.TableRegistrationService.RegistrationResult;
import com.yscope.metalog.coordinator.grpc.proto.CoordinatorServiceGrpc;
import com.yscope.metalog.coordinator.grpc.proto.KafkaConfig;
import com.yscope.metalog.coordinator.grpc.proto.RegisterTableRequest;
import com.yscope.metalog.coordinator.grpc.proto.RegisterTableResponse;
import com.yscope.metalog.node.NodeConfig;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.SQLException;
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
 *   com.yscope.metalog.coordinator.grpc.CoordinatorService/RegisterTable
 * # → {"tableName":"my_spark_logs","created":true}
 * </pre>
 */
public class CoordinatorGrpcService extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcService.class);

  private final TableRegistrationService registrationService;

  public CoordinatorGrpcService(TableRegistrationService registrationService) {
    this.registrationService = registrationService;
  }

  @Override
  public void registerTable(
      RegisterTableRequest request, StreamObserver<RegisterTableResponse> responseObserver) {
    try {
      // Validate proto fields
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

      // Convert proto → domain, delegate to core service
      NodeConfig.TableDefinition def = toTableDefinition(request);
      RegistrationResult result = registrationService.register(def);

      responseObserver.onNext(
          RegisterTableResponse.newBuilder()
              .setTableName(result.tableName())
              .setCreated(result.created())
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
