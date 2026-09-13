//! Positive `external id -> offset` cache over the immutable `e2i` mapping.

use std::sync::atomic::{AtomicU64, Ordering};

use common::types::PointOffsetType;
use crossbeam_utils::atomic::AtomicCell;

use crate::types::PointIdType;

/// Direct-mapped slots; a colliding insert overwrites. Together ~128 KiB per
/// segment: enough to bridge the stages of the queries in flight, which carry
/// a few hundred candidates each.
const SLOTS: usize = 4096;
const _: () = assert!(SLOTS.is_power_of_two());

type Slot = Option<(PointIdType, PointOffsetType)>;

/// Pairs seen by an earlier `i2e` read, so the stages after a search resolve
/// its result ids from RAM instead of one ~16 KiB `e2i` block read each.
///
/// Entries cannot go stale: the mapping is immutable after build, and deletion
/// is a separate source the caller applies *after* the lookup. Nothing is
/// remembered from the `e2i` side — resolving the same id twice re-reads a
/// block the disk cache still holds, so there is nothing to save there.
///
/// A lost slot costs the block read it would have saved, never a wrong answer:
/// the full id is stored and compared on every hit.
#[derive(Debug)]
pub(super) struct E2iCache {
    /// A 32-byte slot exceeds the native atomics, so `AtomicCell` reads it
    /// optimistically under a striped seqlock: readers never block, a writer
    /// racing another on the same stripe spins briefly. Boxed, not inline: the
    /// reader is held by value inside the id tracker enums.
    slots: Box<[AtomicCell<Slot>]>,
    hits: AtomicU64,
}

impl Default for E2iCache {
    fn default() -> Self {
        Self {
            slots: (0..SLOTS).map(|_| AtomicCell::new(None)).collect(),
            hits: AtomicU64::new(0),
        }
    }
}

impl E2iCache {
    /// Remember a pair the mapping yielded, deleted points included: the cache
    /// mirrors the mapping, not the live set.
    pub(super) fn insert(&self, external_id: PointIdType, offset: PointOffsetType) {
        self.slots[Self::slot(external_id)].store(Some((external_id, offset)));
    }

    /// Cached offset of `external_id`, counting the hit.
    pub(super) fn get(&self, external_id: PointIdType) -> Option<PointOffsetType> {
        let (id, offset) = self.slots[Self::slot(external_id)].load()?;
        if id != external_id {
            return None;
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(offset)
    }

    /// Lookups served from RAM since open, i.e. the `e2i` block reads saved.
    pub(super) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Fixed at open: every slot is allocated up front.
    pub(super) fn ram_usage_bytes(&self) -> usize {
        self.slots.len() * size_of::<AtomicCell<Slot>>()
    }

    /// Fibonacci hash of the raw id, so strided numeric ids spread over the
    /// slots too.
    fn slot(external_id: PointIdType) -> usize {
        let raw = match external_id {
            PointIdType::NumId(num) => num,
            PointIdType::Uuid(uuid) => {
                let value = uuid.as_u128();
                (value as u64) ^ ((value >> 64) as u64)
            }
        };
        (raw.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - SLOTS.trailing_zeros())) as usize
    }
}
