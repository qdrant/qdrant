//! End-to-end checks of the writer against a shard directory a leader
//! [`EdgeShard`] built.
//!
//! Only deletes can run the whole way through today: storing points still
//! needs the append-only vector, payload and index storages.

use common::universal_io::MmapFile;
use segment::types::ExtendedPointId;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointOperations::DeletePoints;

use crate::EdgeShard;
use crate::read_only::tests::{exact_count, open_follower, scrolled_ids, test_config, upsert};
use crate::update_only::UpdateOnlyEdgeShard;

/// A batch of deletes against the appendable segment: every phase runs for
/// real — the points are located and resolved through the `LookupSegment`s, a
/// writer resumes the write target from what that read saw, and the deletes
/// land in its mappings log.
#[test]
fn delete_batch_retires_points_and_leaves_the_rest() {
    let dir = tempfile::Builder::new()
        .prefix("edge-update-delete")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush().unwrap();
    drop(leader);

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let deleted_ids = vec![ExtendedPointId::NumId(3), ExtendedPointId::NumId(7)];
    let outcome = writer
        .apply_batch([(100, PointOperation(DeletePoints { ids: deleted_ids }))])
        .unwrap();

    assert_eq!(outcome.deleted, 2);
    assert_eq!(outcome.stored, 0);
    assert_eq!(outcome.skipped, 0);
    assert_eq!(outcome.missing, 0);

    // A reader opened afterwards must see the deletes, and nothing else: a
    // writer retiring the wrong slots would take neighbouring points with it.
    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 8);
    assert_eq!(
        scrolled_ids(&follower),
        [1, 2, 4, 5, 6, 8, 9, 10]
            .map(ExtendedPointId::NumId)
            .to_vec(),
    );
}

/// A retried invocation — a fresh writer over the same directory — replays the
/// batch harmlessly: the points it deletes are already gone, so they resolve
/// to nothing.
#[test]
fn replayed_delete_batch_is_a_no_op() {
    let dir = tempfile::Builder::new()
        .prefix("edge-update-delete-replay")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush().unwrap();
    drop(leader);

    let batch = || {
        [(
            100,
            PointOperation(DeletePoints {
                ids: vec![ExtendedPointId::NumId(3)],
            }),
        )]
    };

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    assert_eq!(writer.apply_batch(batch()).unwrap().deleted, 1);
    drop(writer);

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let replayed = writer.apply_batch(batch()).unwrap();
    assert_eq!(replayed.deleted, 0);
    assert_eq!(replayed.missing, 1);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 9);
}

/// A second batch through one writer is refused rather than applied: it would
/// resume the appendable segment from a log position the first batch has moved
/// past, cutting that batch's writes off.
#[test]
fn second_batch_through_one_writer_is_refused() {
    let dir = tempfile::Builder::new()
        .prefix("edge-update-second-batch")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush().unwrap();
    drop(leader);

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let delete = |id| {
        [(
            100,
            PointOperation(DeletePoints {
                ids: vec![ExtendedPointId::NumId(id)],
            }),
        )]
    };

    writer.apply_batch(delete(3)).unwrap();
    writer.apply_batch(delete(7)).unwrap_err();

    // The refused batch changed nothing, and the first one still stands.
    let follower = open_follower(dir.path());
    assert_eq!(
        scrolled_ids(&follower),
        [1, 2, 4, 5, 6, 7, 8, 9, 10]
            .map(ExtendedPointId::NumId)
            .to_vec(),
    );
}
