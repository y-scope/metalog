package com.yscope.metalog.metastore.model;

import java.util.EnumSet;
import java.util.Set;

/**
 * File lifecycle states based on docs/concepts/metadata-schema.md
 *
 * <p>Supports three entry types with distinct workflows:
 *
 * <ul>
 *   <li><b>IR-only:</b> Ephemeral data for real-time tailing (test environments, debugging)
 *   <li><b>Archive-only:</b> Batch/offline ingestion (Kafka consumers, historical imports)
 *   <li><b>IR+Archive:</b> Real-time ingestion with long-term analytics (primary use case)
 * </ul>
 *
 * <p>State transitions:
 *
 * <pre>
 * IR-only workflow:
 *   IR_BUFFERING → IR_CLOSED → IR_PURGING → (deleted)
 *
 * Archive-only workflow:
 *   ARCHIVE_CLOSED → ARCHIVE_PURGING → (deleted)
 *
 * IR+Archive workflow:
 *   IR_ARCHIVE_BUFFERING → IR_ARCHIVE_CONSOLIDATION_PENDING → ARCHIVE_CLOSED
 *         → ARCHIVE_PURGING → (deleted)
 *
 * IR cleanup: After consolidation (state = ARCHIVE_CLOSED), if clp_ir_path IS NOT NULL,
 * the IR file can be deleted and clp_ir_path set to NULL.
 * </pre>
 */
public enum FileState {
  // ==================
  // IR-only workflow
  // ==================

  /** IR-only: File is being actively written to. Transitions to IR_CLOSED when file is closed. */
  IR_BUFFERING,

  /** IR-only: File is closed and queryable. Transitions to IR_PURGING when retention expires. */
  IR_CLOSED,

  /** IR-only: File deletion in progress. Terminal state before row deletion. */
  IR_PURGING,

  // ==================
  // Archive-only workflow (and IR+Archive after consolidation)
  // ==================

  /**
   * Archive is ready and queryable.
   *
   * <p>For archive-only: created directly from batch ingestion.
   *
   * <p>For IR+Archive: reached after consolidation completes.
   *
   * <p>IR cleanup: if clp_ir_path IS NOT NULL, the IR file can be deleted. Transitions to
   * ARCHIVE_PURGING when retention expires.
   */
  ARCHIVE_CLOSED,

  /** Archive deletion in progress. Terminal state before row deletion. */
  ARCHIVE_PURGING,

  // ==================
  // IR+Archive workflow
  // ==================

  /**
   * IR+Archive: File is being actively written to (buffering). Transitions to CONSOLIDATION_PENDING
   * when file is closed (log rotation).
   */
  IR_ARCHIVE_BUFFERING,

  /**
   * IR+Archive: File is closed and ready for consolidation. Transitions directly to ARCHIVE_CLOSED
   * when consolidation completes.
   */
  IR_ARCHIVE_CONSOLIDATION_PENDING;

  /**
   * Check if this state can transition to the target state.
   *
   * <p>Valid transitions by workflow:
   *
   * <ul>
   *   <li>IR-only: BUFFERING → CLOSED → PURGING
   *   <li>Archive-only: CLOSED → PURGING
   *   <li>IR+Archive: BUFFERING → PENDING → ARCHIVE_CLOSED → PURGING
   * </ul>
   *
   * @param target The target state to transition to
   * @return true if the transition is allowed, false otherwise
   */
  public boolean canTransitionTo(FileState target) {
    if (target == null) {
      return false;
    }
    Set<FileState> validTargets = getValidTransitions();
    return validTargets.contains(target);
  }

  /**
   * Get the set of valid target states from this state.
   *
   * @return Set of states this state can transition to
   */
  public Set<FileState> getValidTransitions() {
    switch (this) {
      // IR-only workflow
      case IR_BUFFERING:
        return EnumSet.of(IR_CLOSED);
      case IR_CLOSED:
        return EnumSet.of(IR_PURGING);
      case IR_PURGING:
        return EnumSet.noneOf(FileState.class); // Terminal

      // Archive-only workflow (and post-consolidation IR+Archive)
      case ARCHIVE_CLOSED:
        return EnumSet.of(ARCHIVE_PURGING);
      case ARCHIVE_PURGING:
        return EnumSet.noneOf(FileState.class); // Terminal

      // IR+Archive workflow
      case IR_ARCHIVE_BUFFERING:
        return EnumSet.of(IR_ARCHIVE_CONSOLIDATION_PENDING);
      case IR_ARCHIVE_CONSOLIDATION_PENDING:
        // Transitions directly to ARCHIVE_CLOSED when consolidation completes
        return EnumSet.of(ARCHIVE_CLOSED);

      default:
        return EnumSet.noneOf(FileState.class);
    }
  }

  /** Check if this state represents a file ready for consolidation. */
  public boolean isReadyForConsolidation() {
    return this == IR_ARCHIVE_CONSOLIDATION_PENDING;
  }

  /** Check if this state represents a file that is still being written. */
  public boolean isBuffering() {
    return this == IR_BUFFERING || this == IR_ARCHIVE_BUFFERING;
  }

  /** Get the next state after file is closed. */
  public FileState onFileClosed() {
    switch (this) {
      case IR_BUFFERING:
        return IR_CLOSED;
      case IR_ARCHIVE_BUFFERING:
        return IR_ARCHIVE_CONSOLIDATION_PENDING;
      default:
        throw new IllegalStateException("Cannot close file in state: " + this);
    }
  }

  /** Get the next state after consolidation completes. Transitions directly to ARCHIVE_CLOSED. */
  public FileState onConsolidationComplete() {
    if (this == IR_ARCHIVE_CONSOLIDATION_PENDING) {
      return ARCHIVE_CLOSED;
    }
    throw new IllegalStateException("Cannot complete consolidation in state: " + this);
  }

  /** Get the next state when retention expires (for purging). */
  public FileState onRetentionExpired() {
    switch (this) {
      case IR_CLOSED:
        return IR_PURGING;
      case ARCHIVE_CLOSED:
        return ARCHIVE_PURGING;
      default:
        throw new IllegalStateException("Cannot expire in state: " + this);
    }
  }

  /** Check if this is an IR-only workflow state. */
  public boolean isIROnly() {
    return this == IR_BUFFERING || this == IR_CLOSED || this == IR_PURGING;
  }

  /** Check if this is an archive-only workflow state (or post-consolidation). */
  public boolean isArchiveOnly() {
    return this == ARCHIVE_CLOSED || this == ARCHIVE_PURGING;
  }

  /** Check if this is an IR+Archive workflow state (before consolidation completes). */
  public boolean isIRArchive() {
    return this == IR_ARCHIVE_BUFFERING || this == IR_ARCHIVE_CONSOLIDATION_PENDING;
  }

  /** Check if this is a purging state (file about to be deleted). */
  public boolean isPurging() {
    return this == IR_PURGING || this == ARCHIVE_PURGING;
  }
}
