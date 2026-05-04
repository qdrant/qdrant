pub mod id_tracker_read;

use std::path::PathBuf;

use common::stored_bitslice::StoredBitSlice;
use common::universal_io::MmapFile;

use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::compressed::versions_store::CompressedVersions;

/// Implementation of Read-Only ID Tracker compatible with
/// [`ImmutableIdTracker`] data format.
///
/// Supports live-reload of externally deleted points.
pub struct ReadOnlyImmutableIdTracker {
    path: PathBuf,
    deleted: StoredBitSlice<MmapFile>,
    internal_to_version: CompressedVersions,
    mappings: CompressedPointMappings,
}
