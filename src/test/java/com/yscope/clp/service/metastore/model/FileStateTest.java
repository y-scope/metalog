package com.yscope.clp.service.metastore.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for FileState enum and state machine transitions.
 *
 * <p>Validates: - State check methods (isBuffering, isReadyForConsolidation, etc.) - Valid
 * transitions for each workflow - Invalid transitions throw IllegalStateException -
 * canTransitionTo() validation method
 *
 * <p>State machine (7 states): - IR-only: IR_BUFFERING → IR_CLOSED → IR_PURGING - Archive-only:
 * ARCHIVE_CLOSED → ARCHIVE_PURGING - IR+Archive: IR_ARCHIVE_BUFFERING →
 * IR_ARCHIVE_CONSOLIDATION_PENDING → ARCHIVE_CLOSED
 */
class FileStateTest {

  // ===================
  // State check methods
  // ===================

  @Nested
  class StateCheckMethods {

    @Test
    void testIsBuffering_bufferingState_returnsTrue() {
      assertTrue(FileState.IR_ARCHIVE_BUFFERING.isBuffering());
      assertTrue(FileState.IR_BUFFERING.isBuffering());
    }

    @Test
    void testIsBuffering_nonBufferingStates_returnsFalse() {
      assertFalse(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.isBuffering());
      assertFalse(FileState.ARCHIVE_CLOSED.isBuffering());
    }

    @Test
    void testIsReadyForConsolidation_pendingState_returnsTrue() {
      assertTrue(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.isReadyForConsolidation());
    }

    @Test
    void testIsReadyForConsolidation_nonPendingStates_returnsFalse() {
      assertFalse(FileState.IR_ARCHIVE_BUFFERING.isReadyForConsolidation());
      assertFalse(FileState.ARCHIVE_CLOSED.isReadyForConsolidation());
    }
  }

  // ===================
  // canTransitionTo() tests
  // ===================

  @Nested
  class CanTransitionToTests {

    @Test
    void testCanTransitionTo_bufferingToPending_allowed() {
      assertTrue(
          FileState.IR_ARCHIVE_BUFFERING.canTransitionTo(
              FileState.IR_ARCHIVE_CONSOLIDATION_PENDING));
    }

    @Test
    void testCanTransitionTo_pendingToArchiveClosed_allowed() {
      // PENDING transitions directly to ARCHIVE_CLOSED when consolidation completes
      assertTrue(
          FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.canTransitionTo(FileState.ARCHIVE_CLOSED));
    }

    @Test
    void testCanTransitionTo_bufferingToArchiveClosed_notAllowed() {
      assertFalse(FileState.IR_ARCHIVE_BUFFERING.canTransitionTo(FileState.ARCHIVE_CLOSED));
    }

    @Test
    void testCanTransitionTo_pendingToBuffering_notAllowed() {
      assertFalse(
          FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.canTransitionTo(
              FileState.IR_ARCHIVE_BUFFERING));
    }

    @Test
    void testCanTransitionTo_archiveClosedToPurging_allowed() {
      assertTrue(FileState.ARCHIVE_CLOSED.canTransitionTo(FileState.ARCHIVE_PURGING));
    }

    @Test
    void testCanTransitionTo_archiveClosedToOthers_notAllowed() {
      assertFalse(FileState.ARCHIVE_CLOSED.canTransitionTo(FileState.IR_ARCHIVE_BUFFERING));
      assertFalse(
          FileState.ARCHIVE_CLOSED.canTransitionTo(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING));
    }

    @Test
    void testCanTransitionTo_sameState_notAllowed() {
      assertFalse(FileState.IR_ARCHIVE_BUFFERING.canTransitionTo(FileState.IR_ARCHIVE_BUFFERING));
      assertFalse(
          FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.canTransitionTo(
              FileState.IR_ARCHIVE_CONSOLIDATION_PENDING));
    }

    @Test
    void testCanTransitionTo_nullTarget_notAllowed() {
      for (FileState state : FileState.values()) {
        assertFalse(
            state.canTransitionTo(null),
            "Expected " + state + ".canTransitionTo(null) to be false");
      }
    }
  }

  // ===================
  // getValidTransitions() tests
  // ===================

  @Nested
  class GetValidTransitionsTests {

    @Test
    void testGetValidTransitions_buffering_returnsPendingOnly() {
      Set<FileState> validTargets = FileState.IR_ARCHIVE_BUFFERING.getValidTransitions();

      assertEquals(1, validTargets.size());
      assertTrue(validTargets.contains(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING));
    }

    @Test
    void testGetValidTransitions_pending_returnsArchiveClosedOnly() {
      Set<FileState> validTargets =
          FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.getValidTransitions();

      assertEquals(1, validTargets.size());
      assertTrue(validTargets.contains(FileState.ARCHIVE_CLOSED));
    }

    @Test
    void testGetValidTransitions_archiveClosed_returnsPurgingOnly() {
      Set<FileState> validTargets = FileState.ARCHIVE_CLOSED.getValidTransitions();

      assertEquals(1, validTargets.size());
      assertTrue(validTargets.contains(FileState.ARCHIVE_PURGING));
    }

    @Test
    void testGetValidTransitions_archivePurging_terminal() {
      Set<FileState> validTargets = FileState.ARCHIVE_PURGING.getValidTransitions();

      assertTrue(validTargets.isEmpty());
    }
  }

  // ===================
  // Valid state transitions (event methods)
  // ===================

  @Nested
  class ValidStateTransitions {

    @Test
    void testOnFileClosed_bufferingState_transitionsToPending() {
      FileState initial = FileState.IR_ARCHIVE_BUFFERING;
      FileState next = initial.onFileClosed();

      assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, next);
      assertTrue(next.isReadyForConsolidation());
    }

    @Test
    void testOnConsolidationComplete_pendingState_transitionsToArchiveClosed() {
      FileState initial = FileState.IR_ARCHIVE_CONSOLIDATION_PENDING;
      FileState next = initial.onConsolidationComplete();

      assertEquals(FileState.ARCHIVE_CLOSED, next);
    }
  }

  // ===================
  // Invalid state transitions
  // ===================

  @Nested
  class InvalidStateTransitions {

    @Test
    void testOnFileClosed_pendingState_throwsException() {
      FileState state = FileState.IR_ARCHIVE_CONSOLIDATION_PENDING;

      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> state.onFileClosed());

      assertTrue(exception.getMessage().contains("Cannot close file in state"));
    }

    @Test
    void testOnFileClosed_archiveClosedState_throwsException() {
      FileState state = FileState.ARCHIVE_CLOSED;

      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> state.onFileClosed());

      assertTrue(exception.getMessage().contains("Cannot close file in state"));
    }

    @Test
    void testOnConsolidationComplete_bufferingState_throwsException() {
      FileState state = FileState.IR_ARCHIVE_BUFFERING;

      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> state.onConsolidationComplete());

      assertTrue(exception.getMessage().contains("Cannot complete consolidation in state"));
    }

    @Test
    void testOnConsolidationComplete_archiveClosedState_throwsException() {
      FileState state = FileState.ARCHIVE_CLOSED;

      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> state.onConsolidationComplete());

      assertTrue(exception.getMessage().contains("Cannot complete consolidation in state"));
    }
  }

  // ===================
  // Workflow type checks
  // ===================

  @Nested
  class WorkflowTypeChecks {

    @Test
    void testIsIROnly_correctClassification() {
      assertTrue(FileState.IR_BUFFERING.isIROnly());
      assertTrue(FileState.IR_CLOSED.isIROnly());
      assertTrue(FileState.IR_PURGING.isIROnly());
      assertFalse(FileState.ARCHIVE_CLOSED.isIROnly());
      assertFalse(FileState.IR_ARCHIVE_BUFFERING.isIROnly());
    }

    @Test
    void testIsArchiveOnly_correctClassification() {
      assertTrue(FileState.ARCHIVE_CLOSED.isArchiveOnly());
      assertTrue(FileState.ARCHIVE_PURGING.isArchiveOnly());
      assertFalse(FileState.IR_ARCHIVE_BUFFERING.isArchiveOnly());
    }

    @Test
    void testIsIRArchive_correctClassification() {
      assertTrue(FileState.IR_ARCHIVE_BUFFERING.isIRArchive());
      assertTrue(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING.isIRArchive());
      // After consolidation completes, ARCHIVE_CLOSED is archive-only, not IR+Archive
      assertFalse(FileState.ARCHIVE_CLOSED.isIRArchive());
    }

    @Test
    void testIsPurging_correctClassification() {
      assertTrue(FileState.IR_PURGING.isPurging());
      assertTrue(FileState.ARCHIVE_PURGING.isPurging());
      assertFalse(FileState.IR_CLOSED.isPurging());
    }
  }

  // ===================
  // IR-only workflow transitions
  // ===================

  @Nested
  class IROnlyWorkflow {

    @Test
    void testIROnlyWorkflow_bufferingToClosedToPurging() {
      FileState state = FileState.IR_BUFFERING;
      assertTrue(state.isBuffering());
      assertTrue(state.isIROnly());

      state = state.onFileClosed();
      assertEquals(FileState.IR_CLOSED, state);

      state = state.onRetentionExpired();
      assertEquals(FileState.IR_PURGING, state);
      assertTrue(state.isPurging());
    }
  }

  // ===================
  // Archive-only workflow transitions
  // ===================

  @Nested
  class ArchiveOnlyWorkflow {

    @Test
    void testArchiveOnlyWorkflow_closedToPurging() {
      FileState state = FileState.ARCHIVE_CLOSED;
      assertTrue(state.isArchiveOnly());

      state = state.onRetentionExpired();
      assertEquals(FileState.ARCHIVE_PURGING, state);
      assertTrue(state.isPurging());
    }
  }

  // ===================
  // IR+Archive workflow to ARCHIVE_CLOSED
  // ===================

  @Nested
  class IRArchiveToArchiveClosed {

    @Test
    void testCompleteIRArchiveLifecycle_bufferingToArchiveClosed() {
      FileState state = FileState.IR_ARCHIVE_BUFFERING;

      state = state.onFileClosed();
      assertEquals(FileState.IR_ARCHIVE_CONSOLIDATION_PENDING, state);

      // Consolidation completes: goes directly to ARCHIVE_CLOSED
      state = state.onConsolidationComplete();
      assertEquals(FileState.ARCHIVE_CLOSED, state);

      state = state.onRetentionExpired();
      assertEquals(FileState.ARCHIVE_PURGING, state);
    }
  }
}
