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
//! - immutable and disk-resident trackers have their deleted mask, and next to it an append-only
//!   [delete log](DELETE_LOG_FILE) with one entry per deleted slot. A delete goes to one of them,
//!   never both: a writer that records versions appends to the log, any other sets the bit in the
//!   mask. The segment's deletes are the union of the two, and every reader reads both.
//!
//! A reader that gates deletes applies the mask's deletes at once, and the log's by their version.
//! Writers that record versions never set a mask bit after the segment is built, so no delete
//! reaches a gated reader through the mask unversioned.

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
use common::universal_io::UniversalReadFs;

pub use self::buffered::BufferedDeletes;
pub use self::gated_log::GatedDeleteLog;
pub use self::held::HeldDeletes;
pub use self::log_reader::DeleteLogReader;
pub use self::log_writer::DeleteLogWriter;
use crate::common::operation_error::OperationResult;
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

/// The slots the delete log of the segment at `segment_path` holds, for a regular tracker that
/// applies the union of its mask and the log at open. Empty when the segment has no log.
pub fn read_delete_log<Fs: UniversalReadFs>(
    fs: &Fs,
    segment_path: &Path,
) -> OperationResult<Vec<PointOffsetType>> {
    let mut reader = DeleteLogReader::<Fs::File>::new(segment_path);
    Ok(reader
        .read_new(fs)?
        .into_iter()
        .map(|entry| entry.slot)
        .collect())
}

/// One delete as the delete log records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeleteLogEntry {
    pub slot: PointOffsetType,
    /// Version of the batch that deleted the point, 0 for a delete with no version to give.
    pub version: SeqNumberType,
}

/// Whether a writer records the version of each delete.
///
/// [`Unversioned`](Self::Unversioned) is the default, and what regular deployments use.
/// [`Versioned`](Self::Versioned) is for serverless, where followers gate deletes by version; its
/// formats cannot be parsed by readers that predate versioned deletes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeleteVersioning {
    /// Write versioned delete entries in appendable mappings logs, and append the deletes of
    /// immutable and disk-resident segments to their delete log, leaving the mask as it is.
    Versioned,
    /// Write plain delete entries in appendable mappings logs, and set the deletes of immutable and
    /// disk-resident segments in their mask.
    Unversioned,
}

impl DeleteVersioning {
    /// [`Versioned`](Self::Versioned) when the `versioned_deletes` feature flag is set.
    pub fn from_feature_flags() -> Self {
        if common::flags::feature_flags().versioned_deletes {
            Self::Versioned
        } else {
            Self::Unversioned
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
    /// Deletes without a version apply at once: plain delete entries of appendable mappings logs,
    /// and the deleted mask of immutable and disk-resident segments.
    Held { gate: SeqNumberType },
}

impl DeleteGating {
    /// The gate deletes are held above: none are under [`Immediate`](Self::Immediate).
    pub fn gate(self) -> SeqNumberType {
        match self {
            Self::Immediate => SeqNumberType::MAX,
            Self::Held { gate } => gate,
        }
    }
}
