pub mod id_tracker_read;
mod lifecycle;
mod live_reload;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::PathBuf;

use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

pub use self::live_reload::LiveReloadResult;
use crate::id_tracker::point_mappings::PointMappings;
use crate::types::{PointIdType, SeqNumberType};

/// Implementation of read-only ID tracker which operates
/// on top of appendable data format.
///
/// Structure can't modify data itself, but can consume appends from external entity by
/// doing live-reload.
///
/// Backed by [`UniversalRead`] file handles so it works over any storage backend (mmap, io_uring,
/// object storage, ...). The handles are retained between reloads and refreshed via
/// [`UniversalRead::reopen`] to pick up data appended by the writer.
///
/// The mappings and versions files may be absent: the writer only creates them once it flushes the
/// first point (an empty file is never written), exactly as
/// [`MutableIdTracker::open`](crate::id_tracker::mutable_id_tracker::MutableIdTracker::open)
/// tolerates. A missing file is treated as an empty storage; the retained [`Self::fs`] lets the
/// handle be opened lazily once the file appears.
///
/// The mapping only ever contains *committed* points. The writer flushes mappings before data
/// before versions, so a point is fully written only once its version is present. An insert read
/// from the mappings log is therefore held in [`Self::pending_inserts`] until its version is
/// flushed, and only then linked into [`Self::mappings`].
pub struct ReadOnlyAppendableIdTracker<S: UniversalRead> {
    segment_path: PathBuf,
    /// Filesystem handle, retained so the mappings/versions files can be opened lazily once the
    /// writer creates them (they are absent while empty).
    fs: S::Fs,
    internal_to_version: Vec<SeqNumberType>,
    mappings: PointMappings,

    /// Inserts read from the mappings log whose version is not flushed yet, keyed by external id.
    ///
    /// These points are intentionally absent from [`Self::mappings`] (their data may be partially
    /// written). Each is linked in once its offset is covered by the versions file, or dropped if
    /// a delete for it arrives first.
    pending_inserts: HashMap<PointIdType, PointOffsetType>,

    /// Highest slot any insert in the mappings log has ever claimed, `None` while the log has
    /// claimed none.
    ///
    /// Counts a slot from the moment the log claims it and never lets it go, which is what makes it
    /// the only sound answer to "which slot may a writer hand out next". Neither of the two
    /// structures above can answer that: [`Self::mappings`] holds only committed points, and
    /// [`Self::pending_inserts`] drops an entry the moment a delete for its external id arrives —
    /// so an `Insert(p, n)` followed by a `Delete(p)` leaves slot `n` claimed on disk yet named
    /// nowhere else. Its data may be half-written by the writer that claimed it, so handing it out
    /// again would write a second point over the remains of the first.
    max_claimed_internal_id: Option<PointOffsetType>,

    /// Byte offset up to which the mappings log has been consumed.
    ///
    /// New mapping changes are appended after this offset by the mutable tracker. On live-reload
    /// we read the file from here onwards. It always points to the end of the last fully-read
    /// entry, so a partial trailing entry (a flush in progress) is re-read on the next reload.
    mappings_read_to: u64,

    /// Backing handle for the append-only mappings log. `None` until the file exists; opened lazily
    /// and refreshed on live-reload.
    mappings_file: Option<S>,

    /// Backing handle for the random-access versions array. `None` until the file exists; opened
    /// lazily and refreshed on live-reload.
    versions_file: Option<S>,
}
