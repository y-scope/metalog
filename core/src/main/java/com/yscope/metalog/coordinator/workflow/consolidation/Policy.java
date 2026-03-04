package com.yscope.metalog.coordinator.workflow.consolidation;

import com.yscope.metalog.metastore.model.ConsolidationTask;
import com.yscope.metalog.metastore.model.FileRecord;
import java.util.List;

/**
 * Policy interface for determining when and how to aggregate IR files.
 *
 * <p>Different implementations support different use cases:
 *
 * <ul>
 *   <li>SparkJobPolicy: Groups by application_id, triggers on job completion
 *   <li>TimeWindowPolicy: Groups by time windows with configurable intervals
 *   <li>AuditPolicy: Groups audit logs with retention-based triggers
 * </ul>
 *
 * <p>See: docs/design/ir-archive-consolidation.md - Aggregation Policy section
 */
public interface Policy {

  /**
   * Determine if the given IR files should be aggregated.
   *
   * @param files List of IR files in CONSOLIDATION_PENDING state
   * @return List of aggregation tasks to create (may be empty)
   */
  List<ConsolidationTask> shouldAggregate(List<FileRecord> files);

  /**
   * Learn from completed aggregation to improve future estimates.
   *
   * @param task Completed aggregation task
   * @param totalIrSizeBytes Total size of input IR files
   */
  void learnFromResult(ConsolidationTask task, long totalIrSizeBytes);

  /** Get the policy name for logging/debugging. */
  String getPolicyName();

  /**
   * Transfer learned state from a previous policy instance during hot-reload.
   *
   * <p>Implementations should check whether {@code oldPolicy} is of the same concrete type and
   * transfer any learned parameters (e.g., compression ratios). The default implementation is a
   * no-op, suitable for policies with no mutable state (like {@code AuditPolicy}).
   *
   * @param oldPolicy the previous policy instance to transfer state from
   */
  default void transferStateFrom(Policy oldPolicy) {}
}
