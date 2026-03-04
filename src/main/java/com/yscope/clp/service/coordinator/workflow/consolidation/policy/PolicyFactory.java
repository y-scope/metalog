package com.yscope.clp.service.coordinator.workflow.consolidation.policy;

import com.yscope.clp.service.common.config.ServiceConfig;
import com.yscope.clp.service.coordinator.workflow.consolidation.Policy;
import com.yscope.clp.service.coordinator.workflow.consolidation.policy.PolicyConfig.PolicyDefinition;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating aggregation policies based on configuration.
 *
 * <p>Policies are auto-discovered via classpath scanning. To add a custom policy:
 *
 * <ol>
 *   <li>Implement {@link Policy}
 *   <li>Annotate with {@code @PolicyType("your-type")}
 *   <li>Provide a constructor that accepts {@link PolicyDefinition}
 *   <li>Done — no other changes needed!
 * </ol>
 *
 * <p>Additional packages can be scanned by setting the {@code CLP_POLICY_PACKAGES} environment
 * variable to a comma-separated list of package names.
 *
 * <p>Supports two modes:
 *
 * <ul>
 *   <li>Legacy: Single policy from ServiceConfig (for backward compatibility)
 *   <li>YAML-based: Multiple policies from PolicyConfig
 * </ul>
 */
public class PolicyFactory {
  private static final Logger logger = LoggerFactory.getLogger(PolicyFactory.class);

  /** Default package to scan for policies (includes subpackages). */
  private static final String DEFAULT_POLICY_PACKAGE =
      "com.yscope.clp.service.coordinator.workflow.consolidation.policy";

  /** Environment variable for additional policy packages. */
  private static final String POLICY_PACKAGES_ENV = "CLP_POLICY_PACKAGES";

  /** Registry of policy type name -> factory function. */
  private static final Map<String, Function<PolicyDefinition, Policy>> REGISTRY =
      new ConcurrentHashMap<>();

  static {
    List<String> packagesToScan = new ArrayList<>();
    packagesToScan.add(DEFAULT_POLICY_PACKAGE);

    String extraPackages = System.getenv(POLICY_PACKAGES_ENV);
    if (extraPackages != null && !extraPackages.isBlank()) {
      for (String pkg : extraPackages.split(",")) {
        String trimmed = pkg.trim();
        if (!trimmed.isEmpty()) {
          packagesToScan.add(trimmed);
        }
      }
    }

    logger.info("Scanning for policies in packages: {}", packagesToScan);

    try (ScanResult scanResult =
        new ClassGraph()
            .acceptPackages(packagesToScan.toArray(new String[0]))
            .enableClassInfo()
            .enableAnnotationInfo()
            .scan()) {

      for (ClassInfo classInfo : scanResult.getClassesWithAnnotation(PolicyType.class)) {
        if (classInfo.implementsInterface(Policy.class)) {
          try {
            @SuppressWarnings("unchecked")
            Class<? extends Policy> clazz = (Class<? extends Policy>) classInfo.loadClass();

            PolicyType annotation = clazz.getAnnotation(PolicyType.class);
            String typeName = annotation.value().toLowerCase();

            Constructor<? extends Policy> ctor = clazz.getConstructor(PolicyDefinition.class);
            REGISTRY.put(typeName, def -> newInstance(ctor, def));
            logger.debug("Registered policy: {} -> {}", typeName, clazz.getSimpleName());

          } catch (Exception e) {
            logger.error("Failed to load policy class: {}", classInfo.getName(), e);
          }
        }
      }
    } catch (Exception e) {
      logger.error(
          "ClassGraph scanning failed for policy discovery (unsupported platform?). "
              + "Policy creation will fall back to an empty registry.",
          e);
    }

    logger.info(
        "PolicyFactory initialized with {} policies: {}", REGISTRY.size(), REGISTRY.keySet());
  }

  /**
   * Create a single policy based on the application configuration. (Legacy method for backward
   * compatibility)
   */
  public static Policy createPolicy(ServiceConfig config) {
    String policyType = config.getPolicyType().toLowerCase();

    Function<PolicyDefinition, Policy> factory = REGISTRY.get(policyType);
    if (factory == null) {
      throw new IllegalArgumentException(
          "Unknown policy type: '" + policyType + "'. Available: " + REGISTRY.keySet());
    }

    // For legacy mode, only "spark" was supported — build a minimal definition
    if ("spark".equals(policyType)) {
      return new SparkJobPolicy(config.getSparkTimeoutHours(), config.getSparkTargetSizeMb());
    }

    throw new IllegalArgumentException("Legacy createPolicy only supports 'spark', got: " + policyType);
  }

  /**
   * Create multiple policies from YAML configuration.
   *
   * @param policyConfig The YAML policy configuration
   * @return List of enabled policies
   */
  public static List<Policy> createPolicies(PolicyConfig policyConfig) {
    List<Policy> policies = new ArrayList<>();

    for (PolicyDefinition definition : policyConfig.getEnabledPolicies()) {
      Policy policy = createPolicyFromDefinition(definition);
      if (policy != null) {
        policies.add(policy);
        logger.info("Created policy: {} (type={})", definition.getName(), definition.getType());
      }
    }

    if (policies.isEmpty()) {
      logger.warn("No enabled policies found in configuration");
    }

    return policies;
  }

  /** Create a single policy from a YAML definition. */
  public static Policy createPolicyFromDefinition(PolicyDefinition definition) {
    String type = definition.getType();
    if (type == null) {
      logger.error("Policy {} has no type specified", definition.getName());
      return null;
    }

    Function<PolicyDefinition, Policy> factory = REGISTRY.get(type.toLowerCase());
    if (factory == null) {
      logger.error("Unknown policy type: {} for policy {}", type, definition.getName());
      return null;
    }

    return factory.apply(definition);
  }

  /**
   * Create policies with state transfer from previous instances.
   *
   * <p>This method is used during hot-reload to preserve learned state (e.g., compression ratios)
   * from the previous policy instances.
   *
   * @param newConfig The new YAML policy configuration
   * @param oldPolicies Map of previous policies by name
   * @return List of new policies with transferred state
   */
  public static List<Policy> createPoliciesWithStateTransfer(
      PolicyConfig newConfig, Map<String, Policy> oldPolicies) {

    List<Policy> newPolicies = new ArrayList<>();

    for (PolicyDefinition definition : newConfig.getEnabledPolicies()) {
      Policy newPolicy = createPolicyFromDefinition(definition);
      if (newPolicy == null) {
        continue;
      }

      Policy oldPolicy = oldPolicies.get(definition.getName());
      if (oldPolicy != null) {
        newPolicy.transferStateFrom(oldPolicy);
      }

      newPolicies.add(newPolicy);
    }

    return newPolicies;
  }

  /** Build a map of policies by name for state transfer. */
  public static Map<String, Policy> buildPolicyMap(List<Policy> policies) {
    Map<String, Policy> map = new HashMap<>();
    for (Policy policy : policies) {
      String fullName = policy.getPolicyName();
      String name =
          fullName.contains(":") ? fullName.substring(fullName.indexOf(':') + 1) : fullName;
      map.put(name, policy);
    }
    return map;
  }

  /** Get all registered policy type names. */
  public static Set<String> getRegisteredTypes() {
    return Collections.unmodifiableSet(REGISTRY.keySet());
  }

  private static Policy newInstance(Constructor<? extends Policy> ctor, PolicyDefinition def) {
    try {
      return ctor.newInstance(def);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to instantiate policy: " + ctor.getDeclaringClass().getName(), e);
    }
  }
}
