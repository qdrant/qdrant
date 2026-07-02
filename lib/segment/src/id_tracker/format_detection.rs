//! Single source of truth for detecting which on-disk id-tracker format a
//! *local* segment is persisted in (used by the writable segment loader).
//!
//! The read-only / object-storage path does not detect by file name — it uses
//! [`ReadOnlyIdTrackerEnum::detect_and_load`](crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum::detect_and_load),
//! which loads by *attempting* each format and avoids per-file `exists` round-trips.

use std::path::Path;

use crate::id_tracker::disk_id_tracker::on_disk_format::i2e_path;
use crate::id_tracker::immutable_id_tracker::mappings_path;

/// The on-disk id-tracker format a segment uses.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IdTrackerFormat {
    /// Appendable / mutable format (append-only mapping + version logs).
    Mutable,
    /// Immutable format: a single sequential `id_tracker.mappings` file, loaded
    /// fully into RAM.
    Immutable,
    /// Disk-resident format: `id_tracker.i2e` + `id_tracker.e2i`, served lazily.
    Disk,
}

impl IdTrackerFormat {
    /// Resolve the format from the segment's appendability and the presence of
    /// the immutable/disk mapping files.
    ///
    /// An appendable segment is always mutable. A non-appendable segment uses the
    /// disk format when its `i2e` file is present, the immutable format when the
    /// v1 `mappings` file is present, and otherwise defaults to mutable (a fresh
    /// segment with no mapping file written yet).
    fn from_presence(is_appendable: bool, has_mappings: bool, has_disk: bool) -> Self {
        if is_appendable {
            Self::Mutable
        } else if has_disk {
            Self::Disk
        } else if has_mappings {
            Self::Immutable
        } else {
            Self::Mutable
        }
    }

    /// Detect the format using the local filesystem.
    pub fn detect_local(segment_path: &Path, is_appendable: bool) -> Self {
        Self::from_presence(
            is_appendable,
            mappings_path(segment_path).is_file(),
            i2e_path(segment_path).is_file(),
        )
    }

    pub fn is_mutable(self) -> bool {
        matches!(self, Self::Mutable)
    }
}
