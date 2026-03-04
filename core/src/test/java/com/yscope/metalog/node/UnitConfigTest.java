package com.yscope.metalog.node;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for CoordinatorUnitConfig and WorkerUnitConfig parsing. */
class UnitConfigTest {

  @Test
  void testCoordinatorUnitConfig_defaults() {
    CoordinatorUnitConfig config = CoordinatorUnitConfig.fromMap(null);

    assertEquals("clp_table", config.getTable());
    assertEquals("localhost:9092", config.getKafka().getBootstrapServers());
    assertEquals("clp-coordinator", config.getKafka().getGroupId());
    assertEquals("clp-metadata", config.getKafka().getTopic());
    assertTrue(config.isDeletionEnabled());
    assertTrue(config.isPartitionManagerEnabled());
    assertTrue(config.isPolicyHotReloadEnabled());
    assertTrue(config.isIndexHotReloadEnabled());
    assertEquals(5000, config.getLoopIntervalMs());
    assertEquals(100, config.getStorageDeletionDelayMs());
    assertEquals(3600000, config.getPartitionMaintenanceIntervalMs());
  }

  @Test
  void testCoordinatorUnitConfig_customValues() {
    Map<String, Object> kafkaMap = new HashMap<>();
    kafkaMap.put("bootstrapServers", "kafka1:9092,kafka2:9092");
    kafkaMap.put("groupId", "my-group");
    kafkaMap.put("topic", "my-topic");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("kafka", kafkaMap);
    configMap.put("table", "custom_table");
    configMap.put("policyConfigPath", "/etc/policy.yaml");
    configMap.put("indexConfigPath", "/etc/index.yaml");
    configMap.put("deletionEnabled", false);
    configMap.put("partitionManagerEnabled", false);
    configMap.put("policyHotReloadEnabled", false);
    configMap.put("indexHotReloadEnabled", false);
    configMap.put("loopIntervalMs", 10000L);
    configMap.put("storageDeletionDelayMs", 50L);
    configMap.put("partitionMaintenanceIntervalMs", 1800000L);

    CoordinatorUnitConfig config = CoordinatorUnitConfig.fromMap(configMap);

    assertEquals("custom_table", config.getTable());
    assertEquals("kafka1:9092,kafka2:9092", config.getKafka().getBootstrapServers());
    assertEquals("my-group", config.getKafka().getGroupId());
    assertEquals("my-topic", config.getKafka().getTopic());
    assertEquals("/etc/policy.yaml", config.getPolicyConfigPath());
    assertEquals("/etc/index.yaml", config.getIndexConfigPath());
    assertFalse(config.isDeletionEnabled());
    assertFalse(config.isPartitionManagerEnabled());
    assertFalse(config.isPolicyHotReloadEnabled());
    assertFalse(config.isIndexHotReloadEnabled());
    assertEquals(10000, config.getLoopIntervalMs());
    assertEquals(50, config.getStorageDeletionDelayMs());
    assertEquals(1800000, config.getPartitionMaintenanceIntervalMs());
  }

  @Test
  void testCoordinatorUnitConfig_partialKafkaConfig() {
    Map<String, Object> kafkaMap = new HashMap<>();
    kafkaMap.put("topic", "partial-topic");

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("kafka", kafkaMap);

    CoordinatorUnitConfig config = CoordinatorUnitConfig.fromMap(configMap);

    // Should have custom topic but default bootstrap servers and group
    assertEquals("partial-topic", config.getKafka().getTopic());
    assertEquals("localhost:9092", config.getKafka().getBootstrapServers());
    assertEquals("clp-coordinator", config.getKafka().getGroupId());
  }

  @Test
  void testWorkerUnitConfig_defaults() {
    WorkerUnitConfig config = WorkerUnitConfig.fromMap(null);

    assertEquals(4, config.getNumWorkers());
  }

  @Test
  void testWorkerUnitConfig_customValues() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("numWorkers", 16);

    WorkerUnitConfig config = WorkerUnitConfig.fromMap(configMap);

    assertEquals(16, config.getNumWorkers());
  }
}
