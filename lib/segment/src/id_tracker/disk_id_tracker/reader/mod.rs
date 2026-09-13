//! Lazy read core over the [on-disk format](super::on_disk_format) mapping
//! files (`i2e` + `e2i` + `is_uuid`).
//!
//! Guarantees:
//!
//! - resident RAM is small and fixed after open: headers, the e2i sparse
//!   block index, the `is_uuid` bitmap, and the positive `e2i` cache;
//! - a point lookup reads at most one data block;
//! - deletion is deliberately NOT applied: lookups and iteration return
//!   build-time-live entries, and callers filter with their own deleted
//!   source.

mod e2i_cache;
mod iter;
mod lifecycle;
mod lookup;

use common::universal_io::UniversalRead;
use roaring::RoaringBitmap;

use self::e2i_cache::E2iCache;
pub use self::iter::iter_random;
use super::on_disk_format::{E2iHeader, I2eHeader};

/// Lazy read core over the `i2e`/`e2i` files.
#[derive(Debug)]
pub struct DiskMappingReader<S: UniversalRead> {
    i2e: S,
    e2i: S,
    i2e_header: I2eHeader,
    e2i_header: E2iHeader,
    /// First numeric key of every numeric block.
    num_sparse: Vec<u64>,
    /// First UUID key (`as_u128`) of every UUID block.
    uuid_sparse: Vec<u128>,
    /// Offsets of the UUID-typed i2e slots, resident since open: decoding a
    /// slot never goes to disk for the flag.
    is_uuid: RoaringBitmap,
    /// Pairs resolved by earlier `i2e` reads, consulted before an `e2i` read.
    e2i_cache: E2iCache,
}

impl<S: UniversalRead> DiskMappingReader<S> {
    /// Total number of internal ids (including build-time-deleted slots).
    pub fn total_point_count(&self) -> u64 {
        self.i2e_header.total
    }

    /// Lookups served from the positive `e2i` cache since open.
    pub fn e2i_cache_hits(&self) -> u64 {
        self.e2i_cache.hits()
    }

    /// Resident RAM: the e2i sparse block index, the `is_uuid` bitmap and the
    /// `e2i` cache. The mapping data itself is not counted — it stays on disk.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            i2e: _,        // on-disk handle
            e2i: _,        // on-disk handle
            i2e_header: _, // constant-size
            e2i_header: _, // constant-size
            num_sparse,
            uuid_sparse,
            is_uuid,
            e2i_cache,
        } = self;
        num_sparse.capacity() * size_of::<u64>()
            + uuid_sparse.capacity() * size_of::<u128>()
            + is_uuid.serialized_size()
            + e2i_cache.ram_usage_bytes()
    }
}
