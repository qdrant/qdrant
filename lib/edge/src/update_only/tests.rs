//! End-to-end checks of the writer against a shard directory a leader
//! [`EdgeShard`] built.
//!
//! Only deletes can run the whole way through today: storing points still
//! needs the append-only vector, payload and index storages.

use common::universal_io::MmapFile;
use segment::types::{ExtendedPointId, SeqNumberType};
use shard::operations::CollectionUpdateOperations;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointOperations::DeletePoints;
use tempfile::TempDir;

use crate::EdgeShard;
use crate::read_only::tests::{exact_count, open_follower, scrolled_ids, test_config, upsert};
use crate::update_only::UpdateOnlyEdgeShard;

/// A flushed shard directory holding points 1 to 10, with the leader that
/// wrote it closed — a writer opens a directory, not a running shard.
fn leader_with_ten_points(prefix: &str) -> TempDir {
    let dir = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush().unwrap();

    dir
}

fn delete_batch(
    ids: impl IntoIterator<Item = u64>,
) -> [(SeqNumberType, CollectionUpdateOperations); 1] {
    let ids = ids.into_iter().map(ExtendedPointId::NumId).collect();

    [(100, PointOperation(DeletePoints { ids }))]
}

/// A batch of deletes against the appendable segment: every phase runs for
/// real — the points are located and resolved through the `LookupSegment`s, a
/// writer resumes the write target from what that read saw, and the deletes
/// land in its mappings log.
#[test]
fn delete_batch_retires_points_and_leaves_the_rest() {
    let dir = leader_with_ten_points("edge-update-delete");

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let outcome = writer.apply_batch(delete_batch([3, 7])).unwrap();

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
///
/// Replaying through the *same* writer needs no test:
/// [`apply_batch`](UpdateOnlyEdgeShard::apply_batch) consumes it, so a second
/// batch does not compile.
#[test]
fn replayed_delete_batch_is_a_no_op() {
    let dir = leader_with_ten_points("edge-update-delete-replay");

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    assert_eq!(writer.apply_batch(delete_batch([3])).unwrap().deleted, 1);

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let replayed = writer.apply_batch(delete_batch([3])).unwrap();
    assert_eq!(replayed.deleted, 0);
    assert_eq!(replayed.missing, 1);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 9);
}
