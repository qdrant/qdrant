//! What a resolving read-only id tracker knows about the moves of its segment.

use ahash::AHashMap;
use common::types::PointOffsetType;
use roaring::RoaringBitmap;
use uuid::Uuid;

use super::{MoveEntry, MoveKind, SlotRef};

/// Why a tombstone read from disk is not applied yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hold {
    /// No moved-out record names the slot in the part of the log read so far, but that part may be
    /// older than the tombstone: a plain delete or a move, not known yet. Settled by a tail read of
    /// the log that started after the tombstone was read.
    Unclassified,
    /// A moved-out record names the slot: held until one of its targets settles.
    Move,
    /// A plain delete, applied by the next resolution.
    Plain,
}

/// The slots of one segment the shard decided to delete now, see
/// [`ReadOnlyIdTrackerEnum::resolve_point_moves`].
///
/// [`ReadOnlyIdTrackerEnum::resolve_point_moves`]:
///     crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum::resolve_point_moves
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MoveResolution {
    /// Held tombstones known to be plain deletes. They depend on no other segment.
    pub plain: Vec<PointOffsetType>,
    /// Copies a settled move supersedes: held tombstones of moves whose target settled, and live
    /// copies a move record names. Deleting one is only safe once no read can still miss the new
    /// copy.
    pub superseded: Vec<PointOffsetType>,
}

impl MoveResolution {
    pub fn is_empty(&self) -> bool {
        self.plain.is_empty() && self.superseded.is_empty()
    }

    /// Sort and deduplicate both lists; a slot in both counts as plain.
    pub fn normalize(&mut self) {
        self.plain.sort_unstable();
        self.plain.dedup();
        self.superseded.sort_unstable();
        self.superseded.dedup();
        let plain = &self.plain;
        self.superseded
            .retain(|slot| plain.binary_search(slot).is_err());
    }

    /// Keep plain deletes only, for a pass that could not wait for older reads to finish.
    pub fn without_superseded(mut self) -> Self {
        self.superseded.clear();
        self
    }
}

/// The move records a resolving tracker has read from its segment's log.
///
/// Records name slots on both sides. The source side is kept per local slot, until that slot is
/// deleted in the view. The target side is kept until its local slot settles, then folded into a
/// compact per-source index, which the shard hands to the source segments as masks.
#[derive(Debug, Default)]
pub struct PointMoves {
    /// Local slots a moved-out record names, with the target of the last record naming them.
    ///
    /// A slot is named twice only when a move was interrupted before its tombstone, and a later
    /// rewrite of the point retired the copy again. The writer publishes a target slot before it
    /// writes the record naming it, so the last target always settles: waiting for it may mask
    /// the copy later than an earlier target would, but never hides the point.
    moved_out: AHashMap<PointOffsetType, SlotRef>,
    /// Moved-in pairs whose local slot has not settled in this view yet: `(local slot, source)`.
    moved_in_unsettled: Vec<(PointOffsetType, SlotRef)>,
    /// Per source segment, the source slots of moved-in pairs whose local slot has settled.
    moved_in_settled: AHashMap<Uuid, RoaringBitmap>,
}

impl PointMoves {
    /// Take in entries read from this segment's log.
    pub fn ingest(&mut self, entries: impl IntoIterator<Item = MoveEntry>) {
        for MoveEntry { kind, peer, pairs } in entries {
            for (local, peer_slot) in pairs {
                let peer = SlotRef {
                    segment: peer,
                    slot: peer_slot,
                };
                match kind {
                    MoveKind::MovedOut => {
                        // The last record wins, see `moved_out`
                        self.moved_out.insert(local, peer);
                    }
                    MoveKind::MovedIn => self.moved_in_unsettled.push((local, peer)),
                }
            }
        }
    }

    /// Fold the moved-in pairs whose local slot has settled into the per-source index.
    pub fn settle(&mut self, is_settled: impl Fn(PointOffsetType) -> bool) {
        let settled = self
            .moved_in_unsettled
            .extract_if(.., |(local, _)| is_settled(*local));
        for (_, source) in settled {
            self.moved_in_settled
                .entry(source.segment)
                .or_default()
                .insert(source.slot);
        }
    }

    /// Whether a moved-out record names `slot`, so its tombstone belongs to a move.
    pub fn names(&self, slot: PointOffsetType) -> bool {
        self.moved_out.contains_key(&slot)
    }

    /// Whether the copy on local `slot` is superseded by a copy the shard can see: the last
    /// moved-out record naming it has a target that has settled, or `masked` holds the slot.
    /// `masked` are the slots of this segment that settled moved-in records of other segments name.
    pub fn is_superseded(
        &self,
        slot: PointOffsetType,
        settled: &impl Fn(SlotRef) -> bool,
        masked: Option<&RoaringBitmap>,
    ) -> bool {
        masked.is_some_and(|masked| masked.contains(slot))
            || self
                .moved_out
                .get(&slot)
                .is_some_and(|&target| settled(target))
    }

    /// Local slots a moved-out record names.
    pub fn moved_out_slots(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.moved_out.keys().copied()
    }

    /// The target of every local slot a moved-out record names.
    pub fn moved_out_targets(&self) -> impl Iterator<Item = SlotRef> + '_ {
        self.moved_out.values().copied()
    }

    /// Per source segment, the source slots of settled moved-in records.
    pub fn settled_moved_in(&self) -> &AHashMap<Uuid, RoaringBitmap> {
        &self.moved_in_settled
    }

    /// Forget the moved-out records of `slots`, which are deleted in the view: they cannot decide
    /// anything anymore.
    pub fn forget(&mut self, slots: impl IntoIterator<Item = PointOffsetType>) {
        for slot in slots {
            self.moved_out.remove(&slot);
        }
    }
}
