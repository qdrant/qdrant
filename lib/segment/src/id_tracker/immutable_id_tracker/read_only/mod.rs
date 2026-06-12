pub mod id_tracker_read;
mod lifecycle;

use std::path::PathBuf;

use common::stored_bitslice::StoredBitSlice;
use common::universal_io::UniversalRead;

use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::versions_store::CompressedVersions;

/// Implementation of Read-Only ID Tracker compatible with
/// [`ImmutableIdTracker`] data format.
///
/// Supports live-reload of externally deleted points.
pub struct ReadOnlyImmutableIdTracker<S: UniversalRead> {
    path: PathBuf,
    deleted: StoredBitSlice<S>,
    internal_to_version: CompressedVersions,
    mappings: CompressedPointMappings,
}
