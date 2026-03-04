package com.yscope.metalog.node;

import java.util.Map;

/**
 * Configuration specific to a CoordinatorUnit.
 *
 * <p>Parsed from the unit's config map in node.yaml:
 *
 * <pre>
 * units:
 *   - name: coordinator-spark
 *     type: coordinator
 *     config:
 *       kafka:
 *         bootstrapServers: localhost:9092
 *         groupId: clp-coordinator-spark
 *         topic: clp-metadata-spark
 *         recordTransformer: spark
 *       table: clp_table
 *       policyConfigPath: /etc/clp/policy.yaml
 *       indexConfigPath: /etc/clp/index.yaml
 *       kafkaPollerEnabled: true
 *       consolidationEnabled: false
 *       deletionEnabled: false
 *       retentionCleanupEnabled: true
 *       retentionCleanupIntervalMs: 60000
 *       partitionManagerEnabled: true
 *       policyHotReloadEnabled: true
 *       indexHotReloadEnabled: true
 *       loopIntervalMs: 5000
 * </pre>
 */
public class CoordinatorUnitConfig {

  private KafkaConfig kafka = new KafkaConfig();
  private String table = "clp_table";
  private String policyConfigPath;
  private String indexConfigPath;
  private boolean kafkaPollerEnabled = true;
  private boolean deletionEnabled = true;
  private boolean partitionManagerEnabled = true;
  private boolean policyHotReloadEnabled = true;
  private boolean indexHotReloadEnabled = true;
  private long loopIntervalMs = 5000;
  private long storageDeletionDelayMs = 100;
  private long partitionMaintenanceIntervalMs = 3600000;
  private boolean consolidationEnabled = true;
  private boolean retentionCleanupEnabled = true;
  private long retentionCleanupIntervalMs = 60000;
  private boolean schemaEvolutionEnabled = true;
  private int schemaEvolutionMaxDimColumns = 50;
  private int schemaEvolutionMaxCountColumns = 20;
  private int throughputLogIntervalSeconds = 60;

  /** Parse configuration from a map. */
  public static CoordinatorUnitConfig fromMap(Map<String, Object> config) {
    CoordinatorUnitConfig result = new CoordinatorUnitConfig();
    ConfigMapParser parser = new ConfigMapParser(config);

    // Parse Kafka config
    Map<String, Object> kafkaMap = parser.getMap("kafka");
    if (kafkaMap != null) {
      result.kafka = KafkaConfig.fromMap(kafkaMap);
    }

    // Parse other fields using type-safe parser
    result.table = parser.getString("table", result.table);
    result.policyConfigPath = parser.getString("policyConfigPath", result.policyConfigPath);
    result.indexConfigPath = parser.getString("indexConfigPath", result.indexConfigPath);
    result.kafkaPollerEnabled = parser.getBoolean("kafkaPollerEnabled", result.kafkaPollerEnabled);
    result.deletionEnabled = parser.getBoolean("deletionEnabled", result.deletionEnabled);
    result.partitionManagerEnabled =
        parser.getBoolean("partitionManagerEnabled", result.partitionManagerEnabled);
    result.policyHotReloadEnabled =
        parser.getBoolean("policyHotReloadEnabled", result.policyHotReloadEnabled);
    result.indexHotReloadEnabled =
        parser.getBoolean("indexHotReloadEnabled", result.indexHotReloadEnabled);
    result.loopIntervalMs = parser.getLong("loopIntervalMs", result.loopIntervalMs);
    result.storageDeletionDelayMs =
        parser.getLong("storageDeletionDelayMs", result.storageDeletionDelayMs);
    result.partitionMaintenanceIntervalMs =
        parser.getLong("partitionMaintenanceIntervalMs", result.partitionMaintenanceIntervalMs);
    result.consolidationEnabled =
        parser.getBoolean("consolidationEnabled", result.consolidationEnabled);
    result.retentionCleanupEnabled =
        parser.getBoolean("retentionCleanupEnabled", result.retentionCleanupEnabled);
    result.retentionCleanupIntervalMs =
        parser.getLong("retentionCleanupIntervalMs", result.retentionCleanupIntervalMs);
    result.schemaEvolutionEnabled =
        parser.getBoolean("schemaEvolutionEnabled", result.schemaEvolutionEnabled);
    result.schemaEvolutionMaxDimColumns =
        parser.getInt("schemaEvolutionMaxDimColumns", result.schemaEvolutionMaxDimColumns);
    result.schemaEvolutionMaxCountColumns =
        parser.getInt("schemaEvolutionMaxCountColumns", result.schemaEvolutionMaxCountColumns);
    result.throughputLogIntervalSeconds =
        parser.getInt("throughputLogIntervalSeconds", result.throughputLogIntervalSeconds);

    return result;
  }

  public KafkaConfig getKafka() {
    return kafka;
  }

  public void setKafka(KafkaConfig kafka) {
    this.kafka = kafka;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getPolicyConfigPath() {
    return policyConfigPath;
  }

  public void setPolicyConfigPath(String policyConfigPath) {
    this.policyConfigPath = policyConfigPath;
  }

  public String getIndexConfigPath() {
    return indexConfigPath;
  }

  public void setIndexConfigPath(String indexConfigPath) {
    this.indexConfigPath = indexConfigPath;
  }

  public boolean isKafkaPollerEnabled() {
    return kafkaPollerEnabled;
  }

  public void setKafkaPollerEnabled(boolean kafkaPollerEnabled) {
    this.kafkaPollerEnabled = kafkaPollerEnabled;
  }

  public boolean isDeletionEnabled() {
    return deletionEnabled;
  }

  public void setDeletionEnabled(boolean deletionEnabled) {
    this.deletionEnabled = deletionEnabled;
  }

  public boolean isPartitionManagerEnabled() {
    return partitionManagerEnabled;
  }

  public void setPartitionManagerEnabled(boolean partitionManagerEnabled) {
    this.partitionManagerEnabled = partitionManagerEnabled;
  }

  public boolean isPolicyHotReloadEnabled() {
    return policyHotReloadEnabled;
  }

  public void setPolicyHotReloadEnabled(boolean policyHotReloadEnabled) {
    this.policyHotReloadEnabled = policyHotReloadEnabled;
  }

  public boolean isIndexHotReloadEnabled() {
    return indexHotReloadEnabled;
  }

  public void setIndexHotReloadEnabled(boolean indexHotReloadEnabled) {
    this.indexHotReloadEnabled = indexHotReloadEnabled;
  }

  public long getLoopIntervalMs() {
    return loopIntervalMs;
  }

  public void setLoopIntervalMs(long loopIntervalMs) {
    this.loopIntervalMs = loopIntervalMs;
  }

  public long getStorageDeletionDelayMs() {
    return storageDeletionDelayMs;
  }

  public void setStorageDeletionDelayMs(long storageDeletionDelayMs) {
    this.storageDeletionDelayMs = storageDeletionDelayMs;
  }

  public long getPartitionMaintenanceIntervalMs() {
    return partitionMaintenanceIntervalMs;
  }

  public void setPartitionMaintenanceIntervalMs(long partitionMaintenanceIntervalMs) {
    this.partitionMaintenanceIntervalMs = partitionMaintenanceIntervalMs;
  }

  public boolean isConsolidationEnabled() {
    return consolidationEnabled;
  }

  public void setConsolidationEnabled(boolean consolidationEnabled) {
    this.consolidationEnabled = consolidationEnabled;
  }

  public boolean isRetentionCleanupEnabled() {
    return retentionCleanupEnabled;
  }

  public void setRetentionCleanupEnabled(boolean retentionCleanupEnabled) {
    this.retentionCleanupEnabled = retentionCleanupEnabled;
  }

  public long getRetentionCleanupIntervalMs() {
    return retentionCleanupIntervalMs;
  }

  public void setRetentionCleanupIntervalMs(long retentionCleanupIntervalMs) {
    this.retentionCleanupIntervalMs = retentionCleanupIntervalMs;
  }

  public boolean isSchemaEvolutionEnabled() {
    return schemaEvolutionEnabled;
  }

  public void setSchemaEvolutionEnabled(boolean schemaEvolutionEnabled) {
    this.schemaEvolutionEnabled = schemaEvolutionEnabled;
  }

  public int getSchemaEvolutionMaxDimColumns() {
    return schemaEvolutionMaxDimColumns;
  }

  public void setSchemaEvolutionMaxDimColumns(int schemaEvolutionMaxDimColumns) {
    this.schemaEvolutionMaxDimColumns = schemaEvolutionMaxDimColumns;
  }

  public int getSchemaEvolutionMaxCountColumns() {
    return schemaEvolutionMaxCountColumns;
  }

  public void setSchemaEvolutionMaxCountColumns(int schemaEvolutionMaxCountColumns) {
    this.schemaEvolutionMaxCountColumns = schemaEvolutionMaxCountColumns;
  }

  public int getThroughputLogIntervalSeconds() {
    return throughputLogIntervalSeconds;
  }

  public void setThroughputLogIntervalSeconds(int throughputLogIntervalSeconds) {
    this.throughputLogIntervalSeconds = throughputLogIntervalSeconds;
  }

  public String getRecordTransformer() {
    return kafka.getRecordTransformer();
  }

  /** Kafka consumer configuration. */
  public static class KafkaConfig {
    private String bootstrapServers = "localhost:9092";
    private String groupId = "clp-coordinator";
    private String topic = "clp-metadata";
    private String recordTransformer;

    public static KafkaConfig fromMap(Map<String, Object> config) {
      KafkaConfig result = new KafkaConfig();
      ConfigMapParser parser = new ConfigMapParser(config);
      result.bootstrapServers = parser.getString("bootstrapServers", result.bootstrapServers);
      result.groupId = parser.getString("groupId", result.groupId);
      result.topic = parser.getString("topic", result.topic);
      result.recordTransformer = parser.getString("recordTransformer", result.recordTransformer);
      return result;
    }

    public String getBootstrapServers() {
      return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
      return groupId;
    }

    public void setGroupId(String groupId) {
      this.groupId = groupId;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public String getRecordTransformer() {
      return recordTransformer;
    }

    public void setRecordTransformer(String recordTransformer) {
      this.recordTransformer = recordTransformer;
    }
  }
}
