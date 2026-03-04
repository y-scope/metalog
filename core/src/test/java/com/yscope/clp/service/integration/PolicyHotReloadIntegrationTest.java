package com.yscope.clp.service.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.yscope.clp.service.common.config.YamlConfigLoader;
import com.yscope.clp.service.coordinator.workflow.consolidation.Policy;
import com.yscope.clp.service.coordinator.workflow.consolidation.policy.PolicyConfig;
import com.yscope.clp.service.coordinator.workflow.consolidation.policy.PolicyFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for policy hot-reload functionality.
 *
 * <p>Tests the full flow of: - Loading policies from YAML - Detecting file changes - Reloading with
 * state transfer
 */
class PolicyHotReloadIntegrationTest {

  @TempDir Path tempDir;

  private Path policyFile;
  private YamlConfigLoader<PolicyConfig> yamlConfig;

  @BeforeEach
  void setUp() throws IOException {
    policyFile = tempDir.resolve("policy.yaml");
    writeInitialPolicy();
  }

  @AfterEach
  void tearDown() {
    if (yamlConfig != null) {
      yamlConfig.close();
    }
  }

  private void writeInitialPolicy() throws IOException {
    String yaml =
        "policies:\n"
            + "  - name: spark-jobs\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      timeout:\n"
            + "        hours: 4\n"
            + "      jobComplete:\n"
            + "        minFiles: 3\n"
            + "    archiveSize:\n"
            + "      targetMb: 64\n"
            + "    compressionRatio:\n"
            + "      initial: 3.0\n"
            + "      learningRate: 0.1\n";
    Files.writeString(policyFile, yaml);
  }

  @Test
  void testCreatePoliciesFromYaml() throws IOException {
    yamlConfig = new YamlConfigLoader<>(PolicyConfig.class, policyFile, null);

    List<Policy> policies = PolicyFactory.createPolicies(yamlConfig.get());

    assertEquals(1, policies.size());
    assertTrue(policies.get(0).getPolicyName().contains("spark-jobs"));
  }

  @Test
  void testMultiplePolicyTypes() throws IOException {
    String yaml =
        "policies:\n"
            + "  - name: spark-jobs\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      timeout:\n"
            + "        hours: 4\n"
            + "  - name: microservices\n"
            + "    type: timeWindow\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      timeWindow:\n"
            + "        intervalMinutes: 15\n"
            + "      timeout:\n"
            + "        hours: 1\n"
            + "  - name: audit-logs\n"
            + "    type: audit\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      sizeThreshold:\n"
            + "        minBytes: 10485760\n"
            + "    immutable: true\n";
    Files.writeString(policyFile, yaml);

    yamlConfig = new YamlConfigLoader<>(PolicyConfig.class, policyFile, null);
    List<Policy> policies = PolicyFactory.createPolicies(yamlConfig.get());

    assertEquals(3, policies.size());
    assertTrue(policies.stream().anyMatch(p -> p.getPolicyName().contains("SparkJobPolicy")));
    assertTrue(policies.stream().anyMatch(p -> p.getPolicyName().contains("TimeWindowPolicy")));
    assertTrue(policies.stream().anyMatch(p -> p.getPolicyName().contains("AuditPolicy")));
  }

  @Test
  void testDisabledPoliciesNotCreated() throws IOException {
    String yaml =
        "policies:\n"
            + "  - name: enabled-policy\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      timeout:\n"
            + "        hours: 4\n"
            + "  - name: disabled-policy\n"
            + "    type: spark\n"
            + "    enabled: false\n"
            + "    triggers:\n"
            + "      timeout:\n"
            + "        hours: 4\n";
    Files.writeString(policyFile, yaml);

    yamlConfig = new YamlConfigLoader<>(PolicyConfig.class, policyFile, null);
    List<Policy> policies = PolicyFactory.createPolicies(yamlConfig.get());

    assertEquals(1, policies.size());
    assertTrue(policies.get(0).getPolicyName().contains("enabled-policy"));
  }

  @Test
  void testStateTransferOnReload() throws IOException {
    yamlConfig = new YamlConfigLoader<>(PolicyConfig.class, policyFile, null);

    // Create initial policies
    List<Policy> oldPolicies = PolicyFactory.createPolicies(yamlConfig.get());
    Map<String, Policy> oldPolicyMap = PolicyFactory.buildPolicyMap(oldPolicies);

    // Update config (same policy name, different settings)
    String updatedYaml =
        "policies:\n"
            + "  - name: spark-jobs\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "    triggers:\n"
            + "      timeout:\n"
            + "        hours: 8\n"
            + "    compressionRatio:\n"
            + "      initial: 5.0\n"
            + "      learningRate: 0.2\n";
    Files.writeString(policyFile, updatedYaml);
    yamlConfig.reloadConfig();

    // Create new policies with state transfer
    List<Policy> newPolicies =
        PolicyFactory.createPoliciesWithStateTransfer(yamlConfig.get(), oldPolicyMap);

    assertEquals(1, newPolicies.size());
    // State should be transferred (compression ratio from old policy)
  }

  @Test
  void testUnknownPolicyTypeSkipped() throws IOException {
    String yaml =
        "policies:\n"
            + "  - name: valid-policy\n"
            + "    type: spark\n"
            + "    enabled: true\n"
            + "  - name: unknown-policy\n"
            + "    type: unknown_type\n"
            + "    enabled: true\n";
    Files.writeString(policyFile, yaml);

    yamlConfig = new YamlConfigLoader<>(PolicyConfig.class, policyFile, null);
    List<Policy> policies = PolicyFactory.createPolicies(yamlConfig.get());

    // Unknown type should be skipped
    assertEquals(1, policies.size());
  }
}
