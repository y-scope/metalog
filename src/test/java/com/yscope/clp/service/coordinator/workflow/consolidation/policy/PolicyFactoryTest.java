package com.yscope.clp.service.coordinator.workflow.consolidation.policy;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.coordinator.workflow.consolidation.Policy;
import com.yscope.clp.service.coordinator.workflow.consolidation.policy.PolicyConfig.PolicyDefinition;
import com.yscope.clp.service.metastore.model.ConsolidationTask;
import java.util.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("PolicyFactory")
class PolicyFactoryTest {

  // ========================================================================
  // createPolicy(ServiceConfig) — legacy single-policy creation
  // ========================================================================

  @Nested
  @DisplayName("createPolicy (legacy)")
  class CreatePolicyLegacy {

    @Test
    void testCreatePolicy_sparkType_returnsSparkJobPolicy() {
      ServiceConfig config = mock(ServiceConfig.class);
      when(config.getPolicyType()).thenReturn("spark");
      when(config.getSparkTimeoutHours()).thenReturn(4L);
      when(config.getSparkTargetSizeMb()).thenReturn(64L);

      Policy policy = PolicyFactory.createPolicy(config);

      assertInstanceOf(SparkJobPolicy.class, policy);
    }

    @Test
    void testCreatePolicy_unknownType_throwsIllegalArgument() {
      ServiceConfig config = mock(ServiceConfig.class);
      when(config.getPolicyType()).thenReturn("unknown");

      assertThrows(IllegalArgumentException.class, () -> PolicyFactory.createPolicy(config));
    }
  }

  // ========================================================================
  // createPolicyFromDefinition
  // ========================================================================

  @Nested
  @DisplayName("createPolicyFromDefinition")
  class CreatePolicyFromDefinition {

    @Test
    void testSpark_returnsSparkJobPolicy() {
      PolicyDefinition def = definition("test-spark", "spark");

      Policy policy = PolicyFactory.createPolicyFromDefinition(def);

      assertInstanceOf(SparkJobPolicy.class, policy);
    }

    @Test
    void testTimeWindow_returnsTimeWindowPolicy() {
      PolicyDefinition def = definition("test-tw", "timewindow");

      Policy policy = PolicyFactory.createPolicyFromDefinition(def);

      assertInstanceOf(TimeWindowPolicy.class, policy);
    }

    @Test
    void testAudit_returnsAuditPolicy() {
      PolicyDefinition def = definition("test-audit", "audit");

      Policy policy = PolicyFactory.createPolicyFromDefinition(def);

      assertInstanceOf(AuditPolicy.class, policy);
    }

    @Test
    void testNullType_returnsNull() {
      PolicyDefinition def = definition("test-null", null);

      Policy policy = PolicyFactory.createPolicyFromDefinition(def);

      assertNull(policy);
    }

    @Test
    void testUnknownType_returnsNull() {
      PolicyDefinition def = definition("test-unknown", "flink");

      Policy policy = PolicyFactory.createPolicyFromDefinition(def);

      assertNull(policy);
    }
  }

  // ========================================================================
  // createPolicies
  // ========================================================================

  @Nested
  @DisplayName("createPolicies")
  class CreatePolicies {

    @Test
    void testEmptyEnabledList_returnsEmpty() {
      PolicyConfig config = new PolicyConfig();
      // No policies added — getEnabledPolicies() returns empty list

      List<Policy> policies = PolicyFactory.createPolicies(config);

      assertTrue(policies.isEmpty());
    }

    @Test
    void testFiltersOutNullResultsFromUnknownTypes() {
      PolicyConfig config = new PolicyConfig();
      config.setPolicies(
          List.of(
              definition("good", "spark"),
              definition("bad", "unknown_type"),
              definition("also-good", "audit")));

      List<Policy> policies = PolicyFactory.createPolicies(config);

      assertEquals(2, policies.size());
      assertInstanceOf(SparkJobPolicy.class, policies.get(0));
      assertInstanceOf(AuditPolicy.class, policies.get(1));
    }
  }

  // ========================================================================
  // createPoliciesWithStateTransfer
  // ========================================================================

  @Nested
  @DisplayName("createPoliciesWithStateTransfer")
  class CreatePoliciesWithStateTransfer {

    @Test
    void testTransfersSparkJobPolicyCompressionRatio() {
      // Train an old SparkJobPolicy so it has a non-default compression ratio
      SparkJobPolicy oldSpark = new SparkJobPolicy(4, 64);
      oldSpark.setDefaultCompressionRatio(5.0);

      Map<String, Policy> oldPolicies = Map.of("my-spark", oldSpark);

      PolicyConfig newConfig = new PolicyConfig();
      newConfig.setPolicies(List.of(definition("my-spark", "spark")));

      List<Policy> newPolicies =
          PolicyFactory.createPoliciesWithStateTransfer(newConfig, oldPolicies);

      assertEquals(1, newPolicies.size());
      SparkJobPolicy newSpark = (SparkJobPolicy) newPolicies.get(0);
      assertEquals(5.0, newSpark.getDefaultCompressionRatio(), 0.01);
    }

    @Test
    void testTransfersTimeWindowPolicyState() {
      // Train an old TimeWindowPolicy
      TimeWindowPolicy oldTw = new TimeWindowPolicy("my-tw", "dim", 15, true, 1, 64);
      ConsolidationTask task =
          new ConsolidationTask("/path.clp", List.of("/ir/1"), "svc");
      task.markCompleted(20 * 1024 * 1024);
      oldTw.learnFromResult(task, 100 * 1024 * 1024);
      double oldRatio = oldTw.getCompressionRatio();

      Map<String, Policy> oldPolicies = Map.of("my-tw", oldTw);

      PolicyConfig newConfig = new PolicyConfig();
      newConfig.setPolicies(List.of(definition("my-tw", "timewindow")));

      List<Policy> newPolicies =
          PolicyFactory.createPoliciesWithStateTransfer(newConfig, oldPolicies);

      assertEquals(1, newPolicies.size());
      TimeWindowPolicy newTw = (TimeWindowPolicy) newPolicies.get(0);
      assertEquals(oldRatio, newTw.getCompressionRatio(), 0.01);
    }

    @Test
    void testNoMatchingOldPolicy_noTransfer() {
      Map<String, Policy> oldPolicies = Map.of("other-name", new SparkJobPolicy(4, 64));

      PolicyConfig newConfig = new PolicyConfig();
      newConfig.setPolicies(List.of(definition("new-spark", "spark")));

      List<Policy> newPolicies =
          PolicyFactory.createPoliciesWithStateTransfer(newConfig, oldPolicies);

      assertEquals(1, newPolicies.size());
      // Default compression ratio (3.0) since no state was transferred
      SparkJobPolicy newSpark = (SparkJobPolicy) newPolicies.get(0);
      assertEquals(3.0, newSpark.getDefaultCompressionRatio(), 0.01);
    }
  }

  // ========================================================================
  // buildPolicyMap
  // ========================================================================

  @Nested
  @DisplayName("buildPolicyMap")
  class BuildPolicyMap {

    @Test
    void testExtractsNameAfterColon() {
      TimeWindowPolicy tw = new TimeWindowPolicy("my-policy", "dim", 15, true, 1, 64);
      // getPolicyName() returns "TimeWindowPolicy:my-policy"

      Map<String, Policy> map = PolicyFactory.buildPolicyMap(List.of(tw));

      assertTrue(map.containsKey("my-policy"));
      assertSame(tw, map.get("my-policy"));
    }

    @Test
    void testUsesFullNameIfNoColon() {
      // Create a mock policy with no colon in name
      Policy mockPolicy = mock(Policy.class);
      when(mockPolicy.getPolicyName()).thenReturn("simple-name");

      Map<String, Policy> map = PolicyFactory.buildPolicyMap(List.of(mockPolicy));

      assertTrue(map.containsKey("simple-name"));
    }
  }

  // ========================================================================
  // getRegisteredTypes
  // ========================================================================

  @Nested
  @DisplayName("getRegisteredTypes")
  class GetRegisteredTypes {

    @Test
    void testContainsAllBuiltInTypes() {
      Set<String> types = PolicyFactory.getRegisteredTypes();

      assertTrue(types.contains("spark"), "should contain spark");
      assertTrue(types.contains("timewindow"), "should contain timewindow");
      assertTrue(types.contains("audit"), "should contain audit");
      assertEquals(3, types.size(), "should have exactly 3 built-in types");
    }
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private static PolicyDefinition definition(String name, String type) {
    PolicyDefinition def = new PolicyDefinition();
    def.setName(name);
    def.setType(type);
    def.setGroupingKey("dim/str128/test");
    return def;
  }
}
