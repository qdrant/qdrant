pub mod id_tracker_read;
mod lifecycle;
mod live_reload;
#[cfg(test)]
mod moves_tests;

use std::path::PathBuf;

use common::stored_bitslice::StoredBitSlice;
use common::universal_io::UniversalRead;

use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::id_tracker::point_moves::SlotMoves;

/// Implementation of Read-Only ID Tracker compatible with
/// [`ImmutableIdTracker`] data format.
///
/// Supports live-reload of externally deleted points.
pub struct ReadOnlyImmutableIdTracker<S: UniversalRead> {
    path: PathBuf,
    deleted: StoredBitSlice<S>,
    internal_to_version: CompressedVersions,
    /// Deletes from the mask and from point moves, see [`SlotMoves::moved_away`].
    mappings: CompressedPointMappings,
    /// The segment's move log, read with every reload, see
    /// [`point_moves`](crate::id_tracker::point_moves).
    moves: Box<SlotMoves<S>>,
}
