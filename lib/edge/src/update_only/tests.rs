//! End-to-end checks of the writer against a shard directory a leader
//! [`EdgeShard`] built.
//!
//! Every phase runs for real: the points are located and resolved through the
//! `LookupSegment`s, a writer resumes the write target from what that read
//! saw, and what it appends is verified through an ordinary follower opened
//! afterwards.

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

/// The store tests cannot run on Windows: the leader's writable storage
/// preallocates chunk files, the append-only writer cuts them back to end at
/// the data (its append offset is a compare-and-swap token), and Windows
/// refuses to replace a file while the writer's own `LookupSegment`s hold it
/// memory-mapped. The production target is object storage, where neither
/// preallocation nor mmap exists.
#[cfg(not(windows))]
mod store {
    use std::path::Path;

    use common::universal_io::MmapFs;
    use segment::payload_json;
    use segment::payload_storage::update_only::UpdateOnlyPayloadStorage;
    use segment::types::{WithPayloadInterface, WithVector};
    use shard::files::SEGMENTS_PATH;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::UpsertPoints;
    use shard::operations::point_ops::PointStructPersisted;

    use super::*;
    use crate::RetrieveRequestBuilder;
    use crate::read_only::tests::{assert_follower_vectors, point};
    use crate::read_view::EdgeShardRead as _;

    /// The leader writes its payload storage in mutable (Gridstore) mode, which an
    /// append-only writer refuses to append to. Recreate each segment's payload
    /// storage empty in append-only mode — the fixture points carry no payloads,
    /// so nothing is lost. Stands in for creating segments with append-only
    /// components from the start, which nothing does yet.
    fn recreate_payload_storages_append_only(shard_dir: &Path) {
        for segment in fs_err::read_dir(shard_dir.join(SEGMENTS_PATH)).unwrap() {
            let segment_path = segment.unwrap().path();
            let payload_storage = segment_path.join("payload_storage");
            if payload_storage.is_dir() {
                fs_err::remove_dir_all(&payload_storage).unwrap();
                // Opening the writer creates the storage in append-only mode.
                UpdateOnlyPayloadStorage::<MmapFile>::open(MmapFs, &segment_path).unwrap();
            }
        }
    }

    fn store_batch(
        op_num: SeqNumberType,
        points: Vec<PointStructPersisted>,
    ) -> [(SeqNumberType, CollectionUpdateOperations); 1] {
        [(op_num, PointOperation(UpsertPoints(PointsList(points))))]
    }

    /// A batch of stores against the appendable segment: a new point (with a
    /// payload) and a rewrite of an existing one, appended by the writer and read
    /// back through an ordinary follower.
    #[test]
    fn store_batch_appends_points_end_to_end() {
        let dir = leader_with_ten_points("edge-update-store");
        recreate_payload_storages_append_only(dir.path());

        let new_point = PointStructPersisted {
            payload: Some(payload_json! { "kind": "fresh" }),
            ..point(11)
        };
        // The rewrite must win over the old copy, so give it a vector the fixture
        // would never produce.
        let rewritten = PointStructPersisted {
            vector: point(50).vector,
            ..point(5)
        };

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let outcome = writer
            .apply_batch(store_batch(100, vec![new_point, rewritten]))
            .unwrap();

        assert_eq!(outcome.stored, 2);
        assert_eq!(outcome.deleted, 0);
        assert_eq!(outcome.skipped, 0);
        assert_eq!(outcome.missing, 0);

        let follower = open_follower(dir.path());
        assert_eq!(exact_count(&follower), 11);
        assert_eq!(
            scrolled_ids(&follower),
            (1..=11).map(ExtendedPointId::NumId).collect::<Vec<_>>(),
        );

        // The untouched points read back unchanged...
        assert_follower_vectors(&follower, &[1, 2, 3, 4, 6, 7, 8, 9, 10]);

        // ...the rewritten one serves the batch's vector, not the old copy's, and
        // the new one carries its payload.
        let results = follower
            .retrieve(
                RetrieveRequestBuilder::new(vec![
                    ExtendedPointId::NumId(5),
                    ExtendedPointId::NumId(11),
                ])
                .with_payload(WithPayloadInterface::Bool(true))
                .with_vector(WithVector::Bool(true))
                .build(),
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].vector,
            Some(point(50).vector.try_into().unwrap())
        );
        assert_eq!(results[1].payload, Some(payload_json! { "kind": "fresh" }),);
    }

    /// A retried store batch — a fresh writer over the same directory — is a
    /// no-op: every point already carries the batch's version, which only a
    /// published versions array can tell it.
    #[test]
    fn replayed_store_batch_is_a_no_op() {
        let dir = leader_with_ten_points("edge-update-store-replay");
        recreate_payload_storages_append_only(dir.path());

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let batch = || store_batch(100, vec![point(11)]);
        assert_eq!(writer.apply_batch(batch()).unwrap().stored, 1);

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let replayed = writer.apply_batch(batch()).unwrap();
        assert_eq!(replayed.stored, 0);
        assert_eq!(replayed.skipped, 1);

        let follower = open_follower(dir.path());
        assert_eq!(exact_count(&follower), 11);
    }

    /// A second batch through a fresh writer resumes every component where the
    /// first ended: the mappings log, the vector chunks and the payload pages all
    /// carry on rather than starting over — and a store and a delete land in one
    /// batch through one writer.
    #[test]
    fn second_batch_resumes_where_the_first_ended() {
        let dir = leader_with_ten_points("edge-update-store-resume");
        recreate_payload_storages_append_only(dir.path());

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        assert_eq!(
            writer
                .apply_batch(store_batch(100, vec![point(11)]))
                .unwrap()
                .stored,
            1,
        );

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let second = writer
            .apply_batch([
                (
                    101,
                    PointOperation(UpsertPoints(PointsList(vec![point(12)]))),
                ),
                (
                    102,
                    PointOperation(DeletePoints {
                        ids: vec![ExtendedPointId::NumId(2)],
                    }),
                ),
            ])
            .unwrap();
        assert_eq!(second.stored, 1);
        assert_eq!(second.deleted, 1);

        let follower = open_follower(dir.path());
        assert_eq!(exact_count(&follower), 11);
        assert_eq!(
            scrolled_ids(&follower),
            [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
                .map(ExtendedPointId::NumId)
                .to_vec(),
        );
        assert_follower_vectors(&follower, &[11, 12]);
    }
}
