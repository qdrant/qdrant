//! Point move logs: where copy-on-write moved a point in or out of a segment.
//!
//! The serverless writer moves a point by appending it to the write target and tombstoning its
//! old copy. A searcher refreshes segments independently, so its view of the source can be newer
//! than its view of the target, or the other way around. Each move is therefore recorded on both
//! sides: the target's log gets a moved-in record, the source's log a moved-out record, and both
//! name the same pair of slots.
//!
//! A searcher that resolves moves applies one rule to every pair it knows, from either log: the
//! old copy is masked once the new slot has settled in its view of the target, and not before. A
//! slot has settled once the view has read the insert that claimed it and a non-zero version for
//! it. Other readers delete the old copy as soon as they read its moved-out record, see
//! [`PointMovesMode`].
//!
//! The log is `id_tracker.moves` in the segment directory, for every id tracker format. The first
//! record creates it, it is only ever appended to, and it goes away with its segment. See
//! [`format`] for the entry layout.

mod format;
mod regular;
mod slot_moves;
mod state;
mod view;
mod writer;

#[cfg(test)]
mod regular_tests;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

use ahash::AHashMap;
use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use uuid::Uuid;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

pub use self::regular::apply_unexpected_point_moves;
pub use self::slot_moves::SlotMoves;
pub use self::state::{MoveResolution, PointMoves};
pub use self::view::PointMovesView;
#[cfg(test)]
pub use self::view::read_point_moves_tail;
pub use self::writer::PointMovesWriter;
use crate::types::PointIdType;

/// File name of the move log, in the segment directory.
pub const POINT_MOVES_FILE: &str = "id_tracker.moves";

/// Path of the move log of the segment at `segment_path`.
pub fn point_moves_path(segment_path: &Path) -> PathBuf {
    segment_path.join(POINT_MOVES_FILE)
}

/// How a read-only id tracker treats point moves. Either way, tombstones apply as soon as they are
/// read, and the move log is read along with them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PointMovesMode {
    /// A moved-out record deletes the copy it names as soon as it is read. For the writer's
    /// lookups, which must see exactly what is durable, and for readers that do not resolve moves.
    #[default]
    Apply,
    /// Keep the records, and delete the copy a moved-out record names once the shard finds its
    /// target settled, see [`PointMoves`].
    Resolve,
}

impl PointMovesMode {
    pub fn is_resolve(self) -> bool {
        self == Self::Resolve
    }
}

/// One slot of one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotRef {
    pub segment: Uuid,
    pub slot: PointOffsetType,
}

/// Which side of a move a record describes.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(u8)]
pub enum MoveKind {
    /// The local slot holds a point whose previous copy sits at the peer slot.
    MovedIn = 1,
    /// The point on the local slot has its new copy at the peer slot.
    MovedOut = 2,
}

/// One entry of a move log: the pairs one batch shares between this segment and one peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveEntry {
    pub kind: MoveKind,
    /// The other segment of every pair: the source of a moved-in entry, the target of a
    /// moved-out entry.
    pub peer: Uuid,
    /// `(local slot, peer slot)` per moved point.
    pub pairs: Vec<(PointOffsetType, PointOffsetType)>,
}

/// A point copy an update retires: the point, the slot it occupies, and where its new copy went if
/// the update moved it rather than deleted it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retirement {
    pub id: PointIdType,
    pub slot: PointOffsetType,
    pub moved_to: Option<SlotRef>,
}

/// `entries` without the moved-out pairs whose local slot is deleted for good in the reader's
/// view, by an applied mask or tombstone: set in `deleted`, or past its end. No record can decide
/// anything about such a slot anymore, and remembering one would only cost memory.
pub fn skip_deleted_moved_out(entries: Vec<MoveEntry>, deleted: &BitSlice) -> Vec<MoveEntry> {
    entries
        .into_iter()
        .filter_map(|mut entry| {
            if entry.kind == MoveKind::MovedOut {
                entry
                    .pairs
                    .retain(|&(local, _)| !is_deleted(deleted, local));
                if entry.pairs.is_empty() {
                    return None;
                }
            }
            Some(entry)
        })
        .collect()
}

/// The local slots the moved-out entries among `entries` name, sorted and without duplicates.
pub fn moved_out_slots(entries: &[MoveEntry]) -> Vec<PointOffsetType> {
    let mut slots: Vec<PointOffsetType> = entries
        .iter()
        .filter(|entry| entry.kind == MoveKind::MovedOut)
        .flat_map(|entry| entry.pairs.iter().map(|&(local, _)| local))
        .collect();
    slots.sort_unstable();
    slots.dedup();
    slots
}

/// Whether `slot` is deleted in a view whose deleted state is `deleted`: set, or past its end.
pub fn is_deleted(deleted: &BitSlice, slot: PointOffsetType) -> bool {
    deleted.get(slot as usize).is_none_or(|bit| *bit)
}

/// The moved-out entries recording the moves among `retirements`, one per target segment.
pub fn moved_out_entries(retirements: &[Retirement]) -> Vec<MoveEntry> {
    let mut by_target: AHashMap<Uuid, Vec<(PointOffsetType, PointOffsetType)>> = AHashMap::new();
    for retirement in retirements {
        if let Some(target) = retirement.moved_to {
            by_target
                .entry(target.segment)
                .or_default()
                .push((retirement.slot, target.slot));
        }
    }
    let mut entries: Vec<MoveEntry> = by_target
        .into_iter()
        .map(|(peer, pairs)| MoveEntry {
            kind: MoveKind::MovedOut,
            peer,
            pairs,
        })
        .collect();
    // Deterministic order, for tests and log inspection
    entries.sort_unstable_by_key(|entry| entry.peer);
    entries
}

/// The moved-in entries for points stored on a slot, each moved from the copies listed with it,
/// one entry per source segment.
pub fn moved_in_entries<'a>(
    stored: impl IntoIterator<Item = (PointOffsetType, &'a [SlotRef])>,
) -> Vec<MoveEntry> {
    let mut by_source: AHashMap<Uuid, Vec<(PointOffsetType, PointOffsetType)>> = AHashMap::new();
    for (slot, sources) in stored {
        for source in sources {
            by_source
                .entry(source.segment)
                .or_default()
                .push((slot, source.slot));
        }
    }
    let mut entries: Vec<MoveEntry> = by_source
        .into_iter()
        .map(|(peer, pairs)| MoveEntry {
            kind: MoveKind::MovedIn,
            peer,
            pairs,
        })
        .collect();
    entries.sort_unstable_by_key(|entry| entry.peer);
    entries
}
