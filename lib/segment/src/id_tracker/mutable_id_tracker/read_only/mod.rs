pub mod id_tracker_read;
mod lifecycle;
mod live_reload;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use common::universal_io::UniversalRead;

pub use self::live_reload::LiveReloadResult;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::SeqNumberType;

/// Implementation of read-only ID tracker which operates
/// on top of appendable data format.
///
/// Structure can't modify data itself, but can consume appends from external entity by
/// doing live-reload.
///
/// Backed by [`UniversalRead`] file handles so it works over any storage backend (mmap, io_uring,
/// object storage, ...). The handles are retained between reloads and refreshed via
/// [`UniversalRead::reopen`] to pick up data appended by the writer.
pub struct ReadOnlyAppendableIdTracker<S: UniversalRead> {
    segment_path: PathBuf,
    internal_to_version: Vec<SeqNumberType>,
    mappings: PointMappings,

    /// Byte offset up to which the mappings log has been consumed.
    ///
    /// New mapping changes are appended after this offset by the mutable tracker. On live-reload
    /// we read the file from here onwards. It always points to the end of the last fully-read
    /// entry, so a partial trailing entry (a flush in progress) is re-read on the next reload.
    mappings_read_to: u64,

    /// Backing handle for the append-only mappings log. Refreshed on live-reload.
    mappings_file: S,

    /// Backing handle for the random-access versions array. Refreshed on live-reload.
    versions_file: S,
}
