//! What a resolving read-only id tracker knows about the moves of its segment.

use std::collections::HashMap;

use ahash::AHashMap;
use common::types::PointOffsetType;
use roaring::RoaringBitmap;
use uuid::Uuid;

use super::{MoveEntry, MoveKind, SlotRef};

/// The slots of one segment the shard decided to delete now, see
/// [`ReadOnlyIdTrackerEnum::resolve_point_moves`].
///
/// [`ReadOnlyIdTrackerEnum::resolve_point_moves`]:
///     crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum::resolve_point_moves
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MoveResolution {
    /// Live copies a settled move supersedes, sorted. Deleting one is only safe once no read can
    /// still miss the new copy.
    pub superseded: Vec<PointOffsetType>,
}

impl MoveResolution {
    pub fn is_empty(&self) -> bool {
        self.superseded.is_empty()
    }

    /// Sort and deduplicate.
    pub fn normalize(&mut self) {
        self.superseded.sort_unstable();
        self.superseded.dedup();
    }
}

/// The move records a resolving tracker has read from its segment's log.
///
/// Records name slots on both sides. The source side is kept per local slot, until that slot is
/// deleted in the view. The target side is kept until its local slot settles, then folded into a
/// compact per-source index, which the shard hands to the source segments as masks, until each
/// source has deleted the slot for good.
#[derive(Debug, Default)]
pub struct PointMoves {
    /// Local slots a moved-out record names, with the target of the last record naming them. The
    /// writer publishes a target slot before it writes the record naming it, so whichever record
    /// comes last, its target settles.
    moved_out: AHashMap<PointOffsetType, SlotRef>,
    /// Moved-in pairs whose local slot has not settled in this view yet: `(local slot, source)`.
    moved_in_unsettled: Vec<(PointOffsetType, SlotRef)>,
    /// Per source segment, the source slots of moved-in pairs whose local slot has settled, until
    /// the source deleted them for good, see [`Self::prune_settled`].
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

    /// Local slots of moved-in pairs that have not settled yet.
    pub fn unsettled_slots(&self) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.moved_in_unsettled.iter().map(|&(local, _)| local)
    }

    /// Fold the moved-in pairs whose local slot is in `settled` into the per-source index.
    pub fn settle(&mut self, settled: &RoaringBitmap) {
        let settled = self
            .moved_in_unsettled
            .extract_if(.., |(local, _)| settled.contains(*local));
        for (_, source) in settled {
            self.moved_in_settled
                .entry(source.segment)
                .or_default()
                .insert(source.slot);
        }
    }

    /// Fold every moved-in pair into the per-source index, for a format whose slots are all
    /// settled.
    pub fn settle_all(&mut self) {
        for (_, source) in self.moved_in_unsettled.drain(..) {
            self.moved_in_settled
                .entry(source.segment)
                .or_default()
                .insert(source.slot);
        }
    }

    /// Every local slot a moved-out record names, with the target of the last record naming it.
    /// The shard decides from these which local copies a settled move supersedes.
    pub fn moved_out(&self) -> impl Iterator<Item = (PointOffsetType, SlotRef)> + '_ {
        self.moved_out.iter().map(|(&slot, &target)| (slot, target))
    }

    /// Whether a moved-out record names a local slot, so a copy may still wait for its move to
    /// settle.
    pub fn has_moved_out(&self) -> bool {
        !self.moved_out.is_empty()
    }

    /// Per source segment, the source slots of settled moved-in records.
    pub fn settled_moved_in(&self) -> &AHashMap<Uuid, RoaringBitmap> {
        &self.moved_in_settled
    }

    /// Forget the settled moved-in pairs whose source slot `retired` names, per source segment: the
    /// source deleted those slots for good, so masking them again would delete nothing.
    pub fn prune_settled(&mut self, retired: &HashMap<Uuid, RoaringBitmap>) {
        self.moved_in_settled.retain(|source, slots| {
            if let Some(retired) = retired.get(source) {
                *slots -= retired;
            }
            !slots.is_empty()
        });
    }

    /// Forget the moved-out records of `slots`, which are deleted in the view: they cannot decide
    /// anything anymore.
    pub fn forget(&mut self, slots: impl IntoIterator<Item = PointOffsetType>) {
        for slot in slots {
            self.moved_out.remove(&slot);
        }
    }
}
