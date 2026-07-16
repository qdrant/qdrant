//! Disk-resident id tracker: the point-id mapping stays on disk (the
//! [on-disk format](on_disk_format)), so resident memory does not scale with
//! point count. The mapping is immutable once written.
//!
//! Two trackers share the [`reader`] core:
//!
//! - [`DiskIdTracker`] — writable but deletion-only (non-appendable, R6);
//!   `deleted` and `versions` are resident and mutated in place.
//! - [`ReadOnlyDiskIdTracker`] — fully read-only; picks up external deletions
//!   via live-reload.

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

/// Writable, deletion-only disk-resident id tracker: the mapping is immutable
/// and served from disk; only the `deleted` bitvec and `versions` are resident
/// and mutable.
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
    /// Approximate resident RAM: versions, deleted bitvec, and the reader's
    /// resident parts. The on-disk mapping is not counted.
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
