package com.yscope.clp.service.coordinator.workflow.consolidation.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * YAML configuration model for aggregation policies.
 *
 * <p>Example policy.yaml:
 *
 * <pre>
 * policies:
 *   - name: spark-jobs
 *     type: spark
 *     enabled: true
 *     groupingKey: application_id
 *     triggers:
 *       jobComplete:
 *         minFiles: 3
 *       timeout:
 *         hours: 4
 *     archiveSize:
 *       targetMb: 64
 *       maxMb: 128
 *     compressionRatio:
 *       initial: 3.0
 *       learningRate: 0.1
 *
 *   - name: microservices
 *     type: timeWindow
 *     enabled: true
 *     groupingKey: service_name
 *     triggers:
 *       timeWindow:
 *         intervalMinutes: 15
 *         alignToWallClock: true
 *       timeout:
 *         hours: 1
 *
 *   - name: audit-logs
 *     type: audit
 *     enabled: true
 *     triggers:
 *       sizeThreshold:
 *         minBytes: 10485760
 *       timeout:
 *         hours: 24
 *     immutable: true
 * </pre>
 */
public class PolicyConfig {

  @JsonProperty("policies")
  private List<PolicyDefinition> policies = new ArrayList<>();

  public List<PolicyDefinition> getPolicies() {
    return policies;
  }

  public void setPolicies(List<PolicyDefinition> policies) {
    this.policies = policies;
  }

  /** Find a policy by name. */
  public PolicyDefinition findByName(String name) {
    return policies.stream().filter(p -> p.getName().equals(name)).findFirst().orElse(null);
  }

  /** Get all enabled policies. */
  public List<PolicyDefinition> getEnabledPolicies() {
    List<PolicyDefinition> enabled = new ArrayList<>();
    for (PolicyDefinition p : policies) {
      if (p.isEnabled()) {
        enabled.add(p);
      }
    }
    return enabled;
  }

  /** Individual policy definition. */
  public static class PolicyDefinition {
    private String name;
    private String type; // spark, timeWindow, audit
    private boolean enabled = true;
    private String groupingKey;
    private TriggerConfig triggers = new TriggerConfig();
    private ArchiveSizeConfig archiveSize = new ArchiveSizeConfig();
    private CompressionRatioConfig compressionRatio = new CompressionRatioConfig();
    private boolean immutable = false; // For audit policy

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public String getGroupingKey() {
      return groupingKey;
    }

    public void setGroupingKey(String groupingKey) {
      this.groupingKey = groupingKey;
    }

    public TriggerConfig getTriggers() {
      return triggers;
    }

    public void setTriggers(TriggerConfig triggers) {
      this.triggers = triggers;
    }

    public ArchiveSizeConfig getArchiveSize() {
      return archiveSize;
    }

    public void setArchiveSize(ArchiveSizeConfig archiveSize) {
      this.archiveSize = archiveSize;
    }

    public CompressionRatioConfig getCompressionRatio() {
      return compressionRatio;
    }

    public void setCompressionRatio(CompressionRatioConfig compressionRatio) {
      this.compressionRatio = compressionRatio;
    }

    public boolean isImmutable() {
      return immutable;
    }

    public void setImmutable(boolean immutable) {
      this.immutable = immutable;
    }
  }

  /** Trigger configuration for policies. */
  public static class TriggerConfig {
    private JobCompleteTrigger jobComplete;
    private TimeoutTrigger timeout;
    private TimeWindowTrigger timeWindow;
    private SizeThresholdTrigger sizeThreshold;

    public JobCompleteTrigger getJobComplete() {
      return jobComplete;
    }

    public void setJobComplete(JobCompleteTrigger jobComplete) {
      this.jobComplete = jobComplete;
    }

    public TimeoutTrigger getTimeout() {
      return timeout;
    }

    public void setTimeout(TimeoutTrigger timeout) {
      this.timeout = timeout;
    }

    public TimeWindowTrigger getTimeWindow() {
      return timeWindow;
    }

    public void setTimeWindow(TimeWindowTrigger timeWindow) {
      this.timeWindow = timeWindow;
    }

    public SizeThresholdTrigger getSizeThreshold() {
      return sizeThreshold;
    }

    public void setSizeThreshold(SizeThresholdTrigger sizeThreshold) {
      this.sizeThreshold = sizeThreshold;
    }
  }

  /** Job complete trigger (for Spark policy). */
  public static class JobCompleteTrigger {
    private int minFiles = 3;

    public int getMinFiles() {
      return minFiles;
    }

    public void setMinFiles(int minFiles) {
      this.minFiles = minFiles;
    }
  }

  /** Timeout trigger (common to all policies). */
  public static class TimeoutTrigger {
    private int hours = 4;

    public int getHours() {
      return hours;
    }

    public void setHours(int hours) {
      this.hours = hours;
    }
  }

  /** Time window trigger (for TimeWindowPolicy). */
  public static class TimeWindowTrigger {
    private int intervalMinutes = 15;
    private boolean alignToWallClock = true;

    public int getIntervalMinutes() {
      return intervalMinutes;
    }

    public void setIntervalMinutes(int intervalMinutes) {
      this.intervalMinutes = intervalMinutes;
    }

    public boolean isAlignToWallClock() {
      return alignToWallClock;
    }

    public void setAlignToWallClock(boolean alignToWallClock) {
      this.alignToWallClock = alignToWallClock;
    }
  }

  /** Size threshold trigger (for AuditPolicy). */
  public static class SizeThresholdTrigger {
    private long minBytes = 10 * 1024 * 1024; // 10MB default

    public long getMinBytes() {
      return minBytes;
    }

    public void setMinBytes(long minBytes) {
      this.minBytes = minBytes;
    }
  }

  /** Archive size configuration. */
  public static class ArchiveSizeConfig {
    private long targetMb = 64;
    private long maxMb = 128;

    public long getTargetMb() {
      return targetMb;
    }

    public void setTargetMb(long targetMb) {
      this.targetMb = targetMb;
    }

    public long getMaxMb() {
      return maxMb;
    }

    public void setMaxMb(long maxMb) {
      this.maxMb = maxMb;
    }
  }

  /** Compression ratio learning configuration. */
  public static class CompressionRatioConfig {
    @JsonProperty("initial")
    private double initialRatio = 3.0;

    private double learningRate = 0.1;

    public double getInitialRatio() {
      return initialRatio;
    }

    public void setInitialRatio(double initialRatio) {
      this.initialRatio = initialRatio;
    }

    public double getLearningRate() {
      return learningRate;
    }

    public void setLearningRate(double learningRate) {
      this.learningRate = learningRate;
    }
  }
}
