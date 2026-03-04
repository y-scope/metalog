package com.yscope.clp.service.coordinator.workflow.consolidation.policy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the type name used to look up a {@link
 * com.yscope.clp.service.coordinator.workflow.consolidation.Policy} implementation.
 *
 * <p>The factory scans for classes with this annotation and registers them automatically. To add a
 * new policy:
 *
 * <ol>
 *   <li>Implement {@link com.yscope.clp.service.coordinator.workflow.consolidation.Policy}
 *   <li>Annotate with {@code @PolicyType("your-type")}
 *   <li>Provide a constructor that accepts {@link PolicyConfig.PolicyDefinition}
 *   <li>Set {@code type: 'your-type'} in the YAML policy configuration
 * </ol>
 *
 * <p>Example:
 *
 * <pre>{@code
 * @PolicyType("spark")
 * public class SparkJobPolicy implements Policy { ... }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PolicyType {
  /** The policy type name used in YAML configuration. Case-insensitive during lookup. */
  String value();
}
