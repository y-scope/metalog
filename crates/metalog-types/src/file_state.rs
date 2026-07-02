use std::fmt;

use serde::{Deserialize, Serialize};

/// File lifecycle state. Four independent chains, all forward-only.
///
/// IR-only:       `IrBuffering` -> `IrClosed` -> `IrPurging` -> [deleted]
/// IR→Archive:    `IrArchiveBuffering` -> `IrArchiveConsolidationPending`
///                -> `ArchiveClosed` -> `ArchivePurging` -> [deleted]
/// File→Archive:  `FileArchiveBuffering` -> `FileArchiveConsolidationPending`
///                -> `ArchiveClosed` -> `ArchivePurging` -> [deleted]
/// Archive-only:  `ArchiveClosed` -> `ArchivePurging` -> [deleted]
///
/// The IR→Archive and File→Archive chains differ in that IR files are
/// searchable (CLP-encoded) while plain files are opaque blobs. Both
/// converge at `ArchiveClosed` after consolidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileState {
    IrBuffering,
    IrClosed,
    IrPurging,
    IrArchiveBuffering,
    IrArchiveConsolidationPending,
    FileArchiveBuffering,
    FileArchiveConsolidationPending,
    ArchiveClosed,
    ArchivePurging,
}

impl FileState {
    /// Returns the SQL ENUM string value stored in the database.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::IrBuffering => "IR_BUFFERING",
            Self::IrClosed => "IR_CLOSED",
            Self::IrPurging => "IR_PURGING",
            Self::IrArchiveBuffering => "IR_ARCHIVE_BUFFERING",
            Self::IrArchiveConsolidationPending => "IR_ARCHIVE_CONSOLIDATION_PENDING",
            Self::FileArchiveBuffering => "FILE_ARCHIVE_BUFFERING",
            Self::FileArchiveConsolidationPending => "FILE_ARCHIVE_CONSOLIDATION_PENDING",
            Self::ArchiveClosed => "ARCHIVE_CLOSED",
            Self::ArchivePurging => "ARCHIVE_PURGING",
        }
    }

    /// Parses a database ENUM string into a `FileState`.
    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "IR_BUFFERING" => Some(Self::IrBuffering),
            "IR_CLOSED" => Some(Self::IrClosed),
            "IR_PURGING" => Some(Self::IrPurging),
            "IR_ARCHIVE_BUFFERING" => Some(Self::IrArchiveBuffering),
            "IR_ARCHIVE_CONSOLIDATION_PENDING" => Some(Self::IrArchiveConsolidationPending),
            "FILE_ARCHIVE_BUFFERING" => Some(Self::FileArchiveBuffering),
            "FILE_ARCHIVE_CONSOLIDATION_PENDING" => Some(Self::FileArchiveConsolidationPending),
            "ARCHIVE_CLOSED" => Some(Self::ArchiveClosed),
            "ARCHIVE_PURGING" => Some(Self::ArchivePurging),
            _ => None,
        }
    }

    /// Returns true if this state has no further transitions (pending deletion).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::IrPurging | Self::ArchivePurging)
    }

    /// Returns true if `target` is a valid forward transition from `self`.
    pub fn can_transition_to(&self, target: Self) -> bool {
        matches!(
            (self, target),
            (Self::IrBuffering, Self::IrClosed)
                | (Self::IrClosed, Self::IrPurging)
                | (
                    Self::IrArchiveBuffering,
                    Self::IrArchiveConsolidationPending
                )
                | (Self::IrArchiveConsolidationPending, Self::ArchiveClosed)
                | (
                    Self::FileArchiveBuffering,
                    Self::FileArchiveConsolidationPending
                )
                | (Self::FileArchiveConsolidationPending, Self::ArchiveClosed)
                | (Self::ArchiveClosed, Self::ArchivePurging)
        )
    }

    /// States that the guarded UPSERT must not overwrite.
    pub fn upsert_guard_states() -> &'static [Self] {
        &[
            Self::IrPurging,
            Self::IrArchiveConsolidationPending,
            Self::FileArchiveConsolidationPending,
            Self::ArchiveClosed,
            Self::ArchivePurging,
        ]
    }

    /// All valid states.
    pub fn all() -> &'static [Self] {
        &[
            Self::IrBuffering,
            Self::IrClosed,
            Self::IrPurging,
            Self::IrArchiveBuffering,
            Self::IrArchiveConsolidationPending,
            Self::FileArchiveBuffering,
            Self::FileArchiveConsolidationPending,
            Self::ArchiveClosed,
            Self::ArchivePurging,
        ]
    }
}

impl fmt::Display for FileState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_db_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_db_str() {
        for state in FileState::all() {
            let s = state.as_db_str();
            let parsed = FileState::from_db_str(s).unwrap();
            assert_eq!(*state, parsed);
        }
    }

    #[test]
    fn invalid_db_str() {
        assert!(FileState::from_db_str("INVALID").is_none());
    }

    #[test]
    fn terminal_states() {
        assert!(FileState::IrPurging.is_terminal());
        assert!(FileState::ArchivePurging.is_terminal());
        assert!(!FileState::IrBuffering.is_terminal());
        assert!(!FileState::ArchiveClosed.is_terminal());
    }

    #[test]
    fn valid_transitions() {
        assert!(FileState::IrBuffering.can_transition_to(FileState::IrClosed));
        assert!(FileState::IrClosed.can_transition_to(FileState::IrPurging));
        assert!(FileState::IrArchiveBuffering
            .can_transition_to(FileState::IrArchiveConsolidationPending));
        assert!(
            FileState::IrArchiveConsolidationPending.can_transition_to(FileState::ArchiveClosed)
        );
        assert!(FileState::FileArchiveBuffering
            .can_transition_to(FileState::FileArchiveConsolidationPending));
        assert!(
            FileState::FileArchiveConsolidationPending.can_transition_to(FileState::ArchiveClosed)
        );
        assert!(FileState::ArchiveClosed.can_transition_to(FileState::ArchivePurging));
    }

    #[test]
    fn invalid_transitions() {
        assert!(!FileState::IrBuffering.can_transition_to(FileState::IrPurging));
        assert!(!FileState::IrPurging.can_transition_to(FileState::IrBuffering));
        assert!(!FileState::ArchiveClosed.can_transition_to(FileState::IrBuffering));
        // No cross-chain transitions.
        assert!(!FileState::IrArchiveBuffering
            .can_transition_to(FileState::FileArchiveConsolidationPending));
        assert!(!FileState::FileArchiveBuffering
            .can_transition_to(FileState::IrArchiveConsolidationPending));
    }

    #[test]
    fn upsert_guard_states_count() {
        assert_eq!(FileState::upsert_guard_states().len(), 5);
    }

    #[test]
    fn display() {
        assert_eq!(FileState::IrBuffering.to_string(), "IR_BUFFERING");
        assert_eq!(
            FileState::IrArchiveConsolidationPending.to_string(),
            "IR_ARCHIVE_CONSOLIDATION_PENDING"
        );
    }
}
