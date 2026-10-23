//! Point moves end to end: the writer records a copy-on-write move in the move logs of both
//! segments, and a store that retires nothing records nothing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::universal_io::{MmapFile, MmapFs, Populate};
use segment::entry::ReadSegmentEntry as _;
use segment::id_tracker::IdTrackerRead as _;
use segment::id_tracker::point_moves::{
    MoveEntry, MoveKind, PointMovesMode, PointMovesView, point_moves_path,
};
use segment::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use segment::segment::read_only::ReadOnlySegment;
use segment::types::ExtendedPointId;
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointOperations::DeletePoints;
use shard::operations::point_ops::PointStructPersisted;
use uuid::Uuid;

use super::store::{recreate_payload_storages_append_only, store_batch};
use super::vacuumed_leader;
use crate::read_only::LocalSegmentEnumerator;
use crate::read_only::tests::point;
use crate::update_only::UpdateOnlyEdgeShard;

/// The point the fixture moves; it lives in an immutable segment after the vacuum.
const MOVED: u64 = 500;

/// The vector the move gives the point, to tell its new copy from its old one.
const NEW_VECTOR: f32 = 5000.0;

fn segment_path(shard: &Path, uuid: Uuid) -> PathBuf {
    shard.join(SEGMENTS_PATH).join(uuid.to_string())
}

fn delete_batch(
    op_num: u64,
    ids: impl IntoIterator<Item = u64>,
) -> [(u64, CollectionUpdateOperations); 1] {
    let ids = ids.into_iter().map(ExtendedPointId::NumId).collect();
    [(op_num, PointOperation(DeletePoints { ids }))]
}

/// The writer records the move in both logs, with the same pair of slots, and tombstones nothing
/// for it. A plain delete is a tombstone, and records nothing.
#[test]
fn writer_records_the_move_on_both_sides() {
    let leader = vacuumed_leader("edge-moves-writer");
    recreate_payload_storages_append_only(leader.path());
    for entry in fs_err::read_dir(leader.path().join(SEGMENTS_PATH)).unwrap() {
        let segment = entry.unwrap().path();
        assert!(
            !point_moves_path(&segment).exists(),
            "no move log before a move"
        );
    }
    let read_log = |segment: Uuid| {
        PointMovesView::<MmapFile>::new(&segment_path(leader.path(), segment))
            .read_new(&MmapFs)
            .unwrap()
    };

    let writer = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(leader.path()).unwrap();
    let target = writer.write_target().expect("a write target");
    let moved = PointStructPersisted {
        vector: point(NEW_VECTOR as u64).vector,
        ..point(MOVED)
    };
    let (writer, outcome) = writer.apply_batch(store_batch(2000, vec![moved])).unwrap();
    let record = &outcome.points[0];
    let [(source, source_slot)] = record.tombstoned[..] else {
        panic!("expected one old copy, got {:?}", record.tombstoned);
    };
    assert_ne!(source, target, "the point must move between segments");
    let (stored_in, target_slot) = record.stored_at.expect("a stored point has a slot");
    assert_eq!(stored_in, target);

    let logs = || (read_log(source), read_log(target));
    let after_move = logs();
    assert_eq!(
        after_move,
        (
            vec![MoveEntry {
                kind: MoveKind::MovedOut,
                peer: target,
                pairs: vec![(source_slot, target_slot)],
            }],
            vec![MoveEntry {
                kind: MoveKind::MovedIn,
                peer: source,
                pairs: vec![(target_slot, source_slot)],
            }],
        ),
        "the source records the move out, the target the move in",
    );

    // The record retires the old copy, without a tombstone: only a reader that applies moves
    // drops it
    let deleted_in = |segment: Uuid, slot, mode| {
        ReadOnlyIdTrackerEnum::<MmapFile>::detect_and_load_with_moves(
            &MmapFs,
            &segment_path(leader.path(), segment),
            None,
            None,
            Populate::No,
            mode,
        )
        .unwrap()
        .is_deleted_point(slot)
    };
    assert!(deleted_in(source, source_slot, PointMovesMode::Apply));
    assert!(!deleted_in(source, source_slot, PointMovesMode::Resolve));

    // A plain delete of the moved point is a tombstone, and records nothing
    let (_writer, outcome) = writer.apply_batch(delete_batch(2001, [MOVED])).unwrap();
    assert_eq!(outcome.deleted, 1);
    assert_eq!(logs(), after_move);
    assert!(deleted_in(target, target_slot, PointMovesMode::Resolve));
}

/// The copies moved out of a segment count as deleted in it, for the vacuum optimizer to find: at
/// once applying moves, and not before they are masked resolving them.
#[test]
fn moved_copies_count_as_deleted() {
    let leader = vacuumed_leader("edge-moves-count");
    recreate_payload_storages_append_only(leader.path());

    let writer = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(leader.path()).unwrap();
    let moved = (MOVED..MOVED + 10)
        .map(|id| PointStructPersisted {
            vector: point(NEW_VECTOR as u64).vector,
            ..point(id)
        })
        .collect();
    let (_writer, outcome) = writer.apply_batch(store_batch(2000, moved)).unwrap();
    let mut moved_out: HashMap<Uuid, usize> = HashMap::new();
    for record in &outcome.points {
        for &(source, _) in &record.tombstoned {
            *moved_out.entry(source).or_default() += 1;
        }
    }
    assert_eq!(moved_out.values().sum::<usize>(), 10);

    for (source, count) in moved_out {
        let path = segment_path(leader.path(), source);
        let open = |mode| {
            ReadOnlySegment::<MmapFile>::open_with_moves(&MmapFs, &path, source, None, None, mode)
                .unwrap()
        };
        let applying = open(PointMovesMode::Apply);
        let resolving = open(PointMovesMode::Resolve);
        assert_eq!(
            applying.deleted_point_count(),
            resolving.deleted_point_count() + count,
        );
        assert_eq!(
            applying.available_point_count() + count,
            resolving.available_point_count(),
        );
    }
}

/// A point stored past the deferred-points threshold keeps its old copy, which the readers that
/// hide deferred points still need, so the store is recorded as a move on neither side.
#[test]
fn deferred_store_records_no_move() {
    let leader = vacuumed_leader("edge-moves-deferred");
    recreate_payload_storages_append_only(leader.path());

    // 1 KB of 1-dim f32 vectors: slots from 256 on are deferred
    let writer = UpdateOnlyEdgeShard::open(
        MmapFs,
        leader.path(),
        LocalSegmentEnumerator::new(leader.path()),
        Some(1),
    )
    .unwrap();
    let target = writer.write_target().expect("a write target");
    let filler = (2001..=2256).map(point).collect();
    let (writer, _) = writer.apply_batch(store_batch(2001, filler)).unwrap();

    let moved = PointStructPersisted {
        vector: point(NEW_VECTOR as u64).vector,
        ..point(MOVED)
    };
    let (_writer, outcome) = writer.apply_batch(store_batch(2002, vec![moved])).unwrap();
    let record = &outcome.points[0];
    assert!(record.tombstoned.is_empty());
    let [(source, _)] = record.shadowed[..] else {
        panic!("expected one shadowed copy, got {:?}", record.shadowed);
    };

    for segment in [source, target] {
        let mut log = PointMovesView::<MmapFile>::new(&segment_path(leader.path(), segment));
        assert_eq!(log.read_new(&MmapFs).unwrap(), [], "move log of {segment}");
    }
}
