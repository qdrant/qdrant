//! Disk-resident id tracker: keeps the point-id mapping on disk (the
//! [on-disk format](on_disk_format) `i2e`/`e2i` files) instead of loading it into
//! RAM, so resident memory does not scale with point count. Only the small
//! `is_uuid` sidecar file is always loaded whole into RAM (as a roaring
//! bitmap inside the [`reader`] core).
//!
//! Two trackers share the [`reader`] core:
//!
//! - [`DiskIdTracker`] — writable, deletion-only (non-appendable, R6): usable in a
//!   regular [`Segment`](crate::segment::Segment) so ordinary deployments also
//!   avoid the full RAM load. `deleted` and `versions` are kept resident (small,
//!   mutated in place); only the mapping stays on disk.
//! - [`ReadOnlyDiskIdTracker`](read_only::ReadOnlyDiskIdTracker) — read-only,
//!   live-reload mirror for object-storage followers.
//!
//! Both are produced from the same on-disk files, written from a
//! [`CompressedPointMappings`] at build time when `serverless_compatible` is set.
//!
//! [`CompressedPointMappings`]:
//!   crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings

mod id_tracker;
mod id_tracker_read;
mod lifecycle;
pub mod mappings;
pub mod on_disk_format;
pub mod read_only;
mod reader;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use common::bitvec::BitVec;
use common::universal_io::{SliceBufferedUpdateWrapper, UniversalWrite};

pub use self::mappings::DiskMappingsSource;
use self::on_disk_format::{e2i_path, i2e_path, is_uuid_path};
pub use self::read_only::ReadOnlyDiskIdTracker;
use self::reader::DiskMappingReader;
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::id_tracker::compressed::versions_store::CompressedVersions;
use crate::types::SeqNumberType;

/// Writable, deletion-only disk-resident id tracker.
///
/// The mapping (`i2e`/`e2i`) is immutable and served from disk via
/// [`DiskMappingReader`]; the `deleted` bitvec and `versions` are kept resident
/// and mutated in place, mirroring [`ImmutableIdTracker`] minus the RAM mapping.
///
/// [`ImmutableIdTracker`]: crate::id_tracker::immutable_id_tracker::ImmutableIdTracker
#[derive(Debug)]
pub struct DiskIdTracker<S: UniversalWrite> {
    path: PathBuf,

    /// Lazy mapping read core (resident: headers, sparse index, `is_uuid`).
    reader: DiskMappingReader<S>,

    /// Resident deleted set (read source for lookups/search); persisted via the wrapper.
    deleted: BitVec,
    deleted_wrapper: BufferedUpdateBitSlice<S>,

    /// Resident per-point versions; persisted via the wrapper.
    internal_to_version: CompressedVersions,
    internal_to_version_wrapper: SliceBufferedUpdateWrapper<S, SeqNumberType>,
}

impl<S: UniversalWrite> DiskIdTracker<S> {
    /// Approximate resident RAM: versions + deleted bitvec + the reader's
    /// resident parts (sparse index, `is_uuid` bitmap). The mapping stays on
    /// disk and is not counted here.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            reader,
            deleted,
            deleted_wrapper: _, // mmap-backed, accounted via files
            internal_to_version,
            internal_to_version_wrapper: _, // mmap-backed, accounted via files
        } = self;
        internal_to_version.ram_usage_bytes()
            + deleted.capacity().div_ceil(u8::BITS as usize)
            + reader.ram_usage_bytes()
    }

    fn mapping_files(&self) -> Vec<PathBuf> {
        vec![
            i2e_path(&self.path),
            e2i_path(&self.path),
            is_uuid_path(&self.path),
        ]
    }
}
