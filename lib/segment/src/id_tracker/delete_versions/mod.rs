//! Versioned deletes: a delete that carries the version of the batch that wrote it.
//!
//! The serverless updater moves a point by storing a new copy in the write target and deleting the
//! old copy in its source segment, two writes that a follower can read in either order. A follower
//! that applies the source's delete before it sees the target's new copy serves neither. With a
//! version on every delete, a follower applies a delete only once it knows every write up to that
//! version is in view, so the new copy of a moved point is visible before the old one goes.
//!
//! Where the version is stored depends on the id tracker format:
//! - appendable trackers record it in the delete entry of their mappings log;
//! - immutable and disk-resident trackers keep their deleted mask, and next to it an append-only
//!   [delete log](DELETE_LOG_FILE) with one entry per deleted slot. Every writer of the mask writes
//!   the log first, so a reader can take the segment's deletes from the log alone. A reader that
//!   gates deletes has to: the mask and the log are separate files, and a fresh mask bit can be
//!   read before the log entry that gives its version.

mod buffered;
mod gated_log;
mod held;
#[cfg(test)]
mod held_tests;
mod log_format;
mod log_reader;
mod log_writer;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tracker_tests;

use std::path::{Path, PathBuf};

use common::types::PointOffsetType;

pub use self::buffered::BufferedDeletedSlots;
pub use self::gated_log::GatedDeleteLog;
pub use self::held::HeldDeletes;
pub use self::log_reader::DeleteLogReader;
pub use self::log_writer::DeleteLogWriter;
use crate::types::SeqNumberType;

/// File name of the delete log of immutable and disk-resident id trackers, in the segment
/// directory.
pub const DELETE_LOG_FILE: &str = "id_tracker.deletes";

pub fn delete_log_path(segment_path: &Path) -> PathBuf {
    segment_path.join(DELETE_LOG_FILE)
}

/// The delete log of the local segment at `segment_path`, if it has one: for the file lists of
/// regular trackers, which snapshots and copies of the segment go by.
pub fn existing_delete_log(segment_path: &Path) -> Option<PathBuf> {
    let path = delete_log_path(segment_path);
    path.exists().then_some(path)
}

/// One delete as the delete log records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteLogEntry {
    pub slot: PointOffsetType,
    /// Version of the batch that deleted the point, 0 for a delete with no version to give.
    pub version: SeqNumberType,
}

/// Whether a writer records versioned deletes in formats that older readers cannot parse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteVersioning {
    /// Write versioned delete entries in appendable mappings logs, and start the delete log of a
    /// segment that has none.
    Record,
    /// Write plain delete entries in appendable mappings logs. A delete log is still appended to
    /// where one exists, so it keeps holding every delete of its segment.
    Legacy,
}

impl DeleteVersioning {
    /// [`Record`](Self::Record) when the `versioned_deletes` feature flag is set.
    pub fn from_feature_flags() -> Self {
        if common::flags::feature_flags().versioned_deletes {
            Self::Record
        } else {
            Self::Legacy
        }
    }
}

/// How a read-only id tracker treats deletes that carry a version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteGating {
    /// Apply every delete as soon as it is read, versioned or not, as a writer's own lookups do.
    Immediate,
    /// Apply a versioned delete once its version is at or below `gate`, and hold a newer one until
    /// a release with a high enough gate. A held point stays visible, with its own version.
    ///
    /// Deletes without a version, written before versioned deletes, apply at once.
    Held { gate: SeqNumberType },
}
