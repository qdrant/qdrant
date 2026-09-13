//! Lazy read core over the [on-disk format](super::on_disk_format) mapping
//! files (`i2e` + `e2i` + `is_uuid`).
//!
//! Guarantees:
//!
//! - resident RAM is small and bounded: headers, the e2i sparse block index,
//!   the `is_uuid` bitmap, and a fixed-capacity positive `e2i` cache;
//! - a point lookup reads at most one data block;
//! - deletion is deliberately NOT applied: lookups and iteration return
//!   build-time-live entries, and callers filter with their own deleted
//!   source.

mod iter;
mod lifecycle;
mod lookup;

use std::sync::atomic::{AtomicU64, Ordering};

use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use roaring::RoaringBitmap;

pub use self::iter::iter_random;
use super::on_disk_format::{E2iHeader, I2eHeader};
use crate::types::PointIdType;

/// Capacity of the positive `e2i` cache, in entries.
///
/// The cache only has to bridge the stages of the queries in flight over one
/// segment — roughly a hundred candidates each. At ~64 bytes per entry this is
/// ~256 KiB per segment.
const E2I_CACHE_ENTRIES: usize = 4096;

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
    /// Positive `e2i` entries seen by earlier reads, so a later stage of the
    /// same query resolves its ids from RAM instead of one ~16 KiB block read
    /// each.
    ///
    /// Only hits are stored, and the mapping is immutable after build —
    /// deletion is a separate source the caller applies *after* the lookup — so
    /// an entry can never go stale. Absent ids are not cached: a fresh query
    /// brings fresh ids, so negative entries would never be hit.
    e2i_cache: quick_cache::sync::Cache<PointIdType, PointOffsetType>,
    /// Lookups served by `e2i_cache`, i.e. the `e2i` block reads it saved.
    e2i_cache_hits: AtomicU64,
}

impl<S: UniversalRead> DiskMappingReader<S> {
    /// Total number of internal ids (including build-time-deleted slots).
    pub fn total_point_count(&self) -> u64 {
        self.i2e_header.total
    }

    /// Lookups served from the positive `e2i` cache since open.
    pub fn e2i_cache_hits(&self) -> u64 {
        self.e2i_cache_hits.load(Ordering::Relaxed)
    }

    /// Remember a resolved `(external id, offset)` pair. Safe for any pair the
    /// mapping itself yielded, deleted or not: the cache mirrors the mapping,
    /// which is immutable, and deletion is applied by the caller afterwards.
    pub(super) fn cache_e2i(&self, external_id: PointIdType, offset: PointOffsetType) {
        self.e2i_cache.insert(external_id, offset);
    }

    /// Cached offset of `external_id`, counting the hit.
    pub(super) fn cached_e2i(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        let offset = self.e2i_cache.get(&external_id)?;
        self.e2i_cache_hits.fetch_add(1, Ordering::Relaxed);
        Some(offset)
    }

    /// Resident RAM: the e2i sparse block index, the `is_uuid` bitmap and the
    /// filled part of the `e2i` cache. The mapping data itself is not counted —
    /// it stays on disk.
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
            e2i_cache_hits: _, // inline counter
        } = self;
        num_sparse.capacity() * size_of::<u64>()
            + uuid_sparse.capacity() * size_of::<u128>()
            + is_uuid.serialized_size()
            + e2i_cache.len() * (size_of::<PointIdType>() + size_of::<PointOffsetType>())
    }
}
