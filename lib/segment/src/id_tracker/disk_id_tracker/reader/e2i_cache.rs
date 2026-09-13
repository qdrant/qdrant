//! Positive `external id -> offset` cache over the immutable `e2i` mapping.

use std::sync::atomic::{AtomicU64, Ordering};

use ahash::AHashMap;
use common::types::PointOffsetType;
use parking_lot::{Mutex, MutexGuard};

use crate::types::PointIdType;

/// Independently locked shards, so concurrent searches over one segment don't
/// queue up on a single lock.
const SHARDS: usize = 16;

/// Entries per shard. Together ~4k pairs, ~130 KiB per segment: enough to
/// bridge the stages of the queries in flight, which carry a few hundred
/// candidates each.
const ENTRIES_PER_SHARD: usize = 256;

/// Bytes per entry: the 24-byte [`PointIdType`] (tag plus a `u64`/`Uuid`
/// payload, 8-aligned) and the 4-byte offset, padded, plus one hashbrown
/// control byte.
const ENTRY_BYTES: usize = size_of::<(PointIdType, PointOffsetType)>() + 1;

/// Pairs seen by an earlier `i2e` read, so the stages after a search resolve
/// its result ids from RAM instead of one ~16 KiB `e2i` block read each.
///
/// Entries cannot go stale: the mapping is immutable after build, and deletion
/// is a separate source the caller applies *after* the lookup. Nothing is
/// remembered from the `e2i` side — resolving the same id twice re-reads a
/// block the disk cache still holds, so there is nothing to save there.
#[derive(Debug)]
pub(super) struct E2iCache {
    /// Boxed, not inline: the reader is held by value inside the id tracker
    /// enums.
    shards: Box<[Mutex<AHashMap<PointIdType, PointOffsetType>>]>,
    hits: AtomicU64,
}

impl Default for E2iCache {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
            hits: AtomicU64::new(0),
        }
    }
}

impl E2iCache {
    /// Remember a pair the mapping yielded, deleted points included: the cache
    /// mirrors the mapping, not the live set.
    pub(super) fn insert(&self, external_id: PointIdType, offset: PointOffsetType) {
        let mut shard = self.shard(external_id);
        // A full shard is dropped wholesale: an entry only has to outlive the
        // query that inserted it, and a scroll drains far more ids than fit.
        if shard.len() >= ENTRIES_PER_SHARD {
            shard.clear();
        }
        shard.insert(external_id, offset);
    }

    /// Cached offset of `external_id`, counting the hit.
    pub(super) fn get(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        let offset = self.shard(external_id).get(&external_id).copied()?;
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(offset)
    }

    /// Lookups served from RAM since open, i.e. the `e2i` block reads saved.
    pub(super) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Resident RAM of the filled part.
    pub(super) fn ram_usage_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().capacity() * ENTRY_BYTES)
            .sum()
    }

    /// Ids spread over the shards by their own low bits; distribution within a
    /// shard is the map hasher's job.
    fn shard(
        &self,
        external_id: PointIdType,
    ) -> MutexGuard<'_, AHashMap<PointIdType, PointOffsetType>> {
        let key = match external_id {
            PointIdType::NumId(num) => num as usize,
            PointIdType::Uuid(uuid) => uuid.as_u128() as usize,
        };
        self.shards[key % self.shards.len()].lock()
    }
}
