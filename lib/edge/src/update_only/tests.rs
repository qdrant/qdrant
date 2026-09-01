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

use crate::read_only::tests::{
    delete as leader_delete, exact_count, init_serverless_feature_flags, open_follower, point,
    scrolled_ids, test_config, upsert,
};
use crate::update_only::{PointApplyKind, UpdateOnlyEdgeShard};
use crate::{EdgeConfig, EdgeOptimizersConfig, EdgeShard};

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
    let (_writer, outcome) = writer.apply_batch(delete_batch([3, 7])).unwrap();

    assert_eq!(outcome.deleted, 2);
    assert_eq!(outcome.stored, 0);
    assert_eq!(outcome.skipped, 0);
    assert_eq!(outcome.missing, 0);

    // Each record names the one slot its point vacated.
    assert_eq!(
        outcome
            .points
            .iter()
            .map(|record| record.id)
            .collect::<Vec<_>>(),
        [3, 7].map(ExtendedPointId::NumId).to_vec(),
    );
    for record in &outcome.points {
        assert_eq!(record.kind, PointApplyKind::Deleted);
        assert_eq!(record.tombstoned.len(), 1);
        assert_eq!(record.superseded, None);
    }

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

/// A retried batch replays harmlessly, through the same writer (whose lookups
/// were reloaded after the first batch) and through a fresh one over the same
/// directory alike: the points it deletes are already gone, so they resolve
/// to nothing.
#[test]
fn replayed_delete_batch_is_a_no_op() {
    let dir = leader_with_ten_points("edge-update-delete-replay");

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let (writer, outcome) = writer.apply_batch(delete_batch([3])).unwrap();
    assert_eq!(outcome.deleted, 1);

    let (_writer, replayed) = writer.apply_batch(delete_batch([3])).unwrap();
    assert_eq!(replayed.deleted, 0);
    assert_eq!(replayed.missing, 1);

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let (_writer, replayed) = writer.apply_batch(delete_batch([3])).unwrap();
    assert_eq!(replayed.deleted, 0);
    assert_eq!(replayed.missing, 1);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 9);
}

/// A flushed shard directory whose points 301 to 1000 live in an immutable
/// segment: deleting 30% triggers a vacuum, and the lowered indexing
/// threshold makes the rebuild non-appendable.
fn vacuumed_leader(prefix: &str) -> TempDir {
    let dir = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();

    let config = EdgeConfig {
        optimizers: Some(EdgeOptimizersConfig {
            deleted_threshold: Some(0.2),
            vacuum_min_vector_number: Some(1),
            indexing_threshold: Some(1),
            ..EdgeOptimizersConfig::default()
        }),
        ..test_config()
    };
    let leader = EdgeShard::new(dir.path(), config).unwrap();
    upsert(&leader, 1..=1000);
    leader_delete(&leader, 1..=300);
    leader.flush().unwrap();
    assert!(leader.optimize().unwrap(), "expected a vacuum to run");
    leader.flush().unwrap();

    dir
}

/// A delete against an immutable segment rewrites its deleted-points bitmask,
/// visibly to a fresh writer and to an ordinary follower alike.
#[cfg_attr(
    windows,
    ignore = "the tombstone rewrite replaces id_tracker.deleted while the writer's own \
              LookupSegment holds it memory-mapped, which Windows refuses"
)]
#[test]
fn delete_batch_tombstones_points_in_immutable_segments() {
    let dir = vacuumed_leader("edge-update-delete-immutable");

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();

    // The segment holding point 500 must not be the write target, or the
    // test dodges the delete-only path.
    let preview = writer.preview_batch(delete_batch([500])).unwrap();
    let holder = preview.points[0].current.as_ref().unwrap().segment;
    assert!(
        !writer
            .segment_configs()
            .iter()
            .any(|info| info.uuid == holder && info.is_write_target),
        "point 500 should live in a non-appendable segment",
    );

    let (writer, outcome) = writer.apply_batch(delete_batch([500])).unwrap();
    assert_eq!(outcome.deleted, 1);
    // The record names the immutable segment the point was deleted from.
    assert_eq!(outcome.points[0].kind, PointApplyKind::Deleted);
    assert_eq!(outcome.points[0].tombstoned.len(), 1);
    assert_eq!(outcome.points[0].tombstoned[0].0, holder);

    // The same writer resolves against the rewritten mask through its
    // reloaded lookup, and a fresh writer through its own open.
    let (_writer, replayed) = writer.apply_batch(delete_batch([500])).unwrap();
    assert_eq!(replayed.deleted, 0);
    assert_eq!(replayed.missing, 1);

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    let (_writer, replayed) = writer.apply_batch(delete_batch([500])).unwrap();
    assert_eq!(replayed.deleted, 0);
    assert_eq!(replayed.missing, 1);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 699);
    let ids = scrolled_ids(&follower);
    assert_eq!(ids.len(), 699);
    assert!(!ids.contains(&ExtendedPointId::NumId(500)));
    assert!(ids.contains(&ExtendedPointId::NumId(499)));
    assert!(ids.contains(&ExtendedPointId::NumId(501)));
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
    use segment::types::{Filter, Payload, WithPayloadInterface, WithVector};
    use shard::files::SEGMENTS_PATH;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::{UpsertPoints, UpsertPointsConditional};
    use shard::operations::point_ops::{
        ConditionalInsertOperationInternal, PointStructPersisted, UpdateMode,
    };

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

    /// A batch of one conditional upsert with the empty condition — existence
    /// is the whole gate, which is what the writer accepts.
    fn conditional_batch(
        op_num: SeqNumberType,
        points: Vec<PointStructPersisted>,
        update_mode: UpdateMode,
    ) -> [(SeqNumberType, CollectionUpdateOperations); 1] {
        [(
            op_num,
            PointOperation(UpsertPointsConditional(
                ConditionalInsertOperationInternal {
                    points_op: PointsList(points),
                    condition: Filter::default(),
                    update_mode: Some(update_mode),
                },
            )),
        )]
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
        let (_writer, outcome) = writer
            .apply_batch(store_batch(100, vec![new_point, rewritten]))
            .unwrap();

        assert_eq!(outcome.stored, 2);
        assert_eq!(outcome.deleted, 0);
        assert_eq!(outcome.skipped, 0);
        assert_eq!(outcome.missing, 0);

        // The records tell a fresh insert from an overwrite: the new point
        // vacated nothing, the rewritten one superseded its old write-target
        // slot in place (no tombstone needed there).
        let [fresh, overwrite] = &outcome.points[..] else {
            panic!("expected one record per touched point");
        };
        assert_eq!(fresh.id, ExtendedPointId::NumId(11));
        assert_eq!(fresh.kind, PointApplyKind::Stored);
        assert!(fresh.tombstoned.is_empty());
        assert_eq!(fresh.superseded, None);
        assert_eq!(overwrite.id, ExtendedPointId::NumId(5));
        assert_eq!(overwrite.kind, PointApplyKind::Stored);
        assert!(overwrite.tombstoned.is_empty());
        assert!(overwrite.superseded.is_some());

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

    /// A retried store batch is a no-op — through the same writer and through
    /// a fresh one over the same directory alike: every point already carries
    /// the batch's version, which only a published versions array can tell
    /// it. The same-writer replay is what the post-batch lookup reload buys:
    /// without it, the resolve would not see the version the first batch
    /// published and would store the point again.
    #[test]
    fn replayed_store_batch_is_a_no_op() {
        let dir = leader_with_ten_points("edge-update-store-replay");
        recreate_payload_storages_append_only(dir.path());

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let batch = || store_batch(100, vec![point(11)]);
        let (writer, outcome) = writer.apply_batch(batch()).unwrap();
        assert_eq!(outcome.stored, 1);

        let (_writer, replayed) = writer.apply_batch(batch()).unwrap();
        assert_eq!(replayed.stored, 0);
        assert_eq!(replayed.skipped, 1);

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let (_writer, replayed) = writer.apply_batch(batch()).unwrap();
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
        let (_writer, outcome) = writer
            .apply_batch(store_batch(100, vec![point(11)]))
            .unwrap();
        assert_eq!(outcome.stored, 1);

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let (_writer, second) = writer
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

    /// Sequential batches through one writer: the second batch stores and
    /// deletes through the same held writers the first one used, resolving
    /// against the lookups reloaded after it — no re-open in between.
    #[test]
    fn sequential_batches_through_one_writer() {
        let dir = leader_with_ten_points("edge-update-store-sequential");
        recreate_payload_storages_append_only(dir.path());

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let (writer, outcome) = writer
            .apply_batch(store_batch(100, vec![point(11)]))
            .unwrap();
        assert_eq!(outcome.stored, 1);

        let (_writer, second) = writer
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

    /// An `insert_only` batch over a mix of taken and free ids: the free ones
    /// are created, the taken ones keep the leader's point untouched — no
    /// rewrite, no tombstone, no version bump — and the run reports which was
    /// which.
    #[test]
    fn insert_only_batch_creates_only_the_free_ids() {
        let dir = leader_with_ten_points("edge-update-insert-only");
        recreate_payload_storages_append_only(dir.path());

        // Point 5 is taken; giving the rejected upsert a vector the fixture
        // would never produce makes an accidental overwrite visible.
        let taken = PointStructPersisted {
            vector: point(50).vector,
            payload: Some(payload_json! { "kind": "rejected" }),
            ..point(5)
        };
        let free = PointStructPersisted {
            payload: Some(payload_json! { "kind": "fresh" }),
            ..point(11)
        };

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let (writer, outcome) = writer
            .apply_batch(conditional_batch(
                100,
                vec![taken.clone(), free],
                UpdateMode::InsertOnly,
            ))
            .unwrap();

        assert_eq!(outcome.stored, 1);
        assert_eq!(outcome.rejected, 1);
        assert_eq!(outcome.deleted, 0);
        assert_eq!(outcome.skipped, 0);
        assert_eq!(outcome.missing, 0);

        let [rejected, created] = &outcome.points[..] else {
            panic!("expected one record per touched point");
        };
        assert_eq!(rejected.id, ExtendedPointId::NumId(5));
        assert_eq!(rejected.kind, PointApplyKind::Rejected);
        // A rejected point keeps every slot it had: nothing was written that
        // could supersede them.
        assert!(rejected.tombstoned.is_empty());
        assert_eq!(rejected.superseded, None);
        assert_eq!(created.id, ExtendedPointId::NumId(11));
        assert_eq!(created.kind, PointApplyKind::Stored);

        let follower = open_follower(dir.path());
        assert_eq!(exact_count(&follower), 11);
        // Point 5 still reads as the leader wrote it, payload included.
        assert_follower_vectors(&follower, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let results = follower
            .retrieve(
                RetrieveRequestBuilder::new(vec![ExtendedPointId::NumId(5)])
                    .with_payload(WithPayloadInterface::Bool(true))
                    .build(),
            )
            .unwrap();
        assert_eq!(results[0].payload, Some(Payload::default()));

        // The id it just created is now taken, so replaying the same batch
        // through the same writer rejects both.
        let (_writer, replayed) = writer
            .apply_batch(conditional_batch(
                101,
                vec![taken, point(11)],
                UpdateMode::InsertOnly,
            ))
            .unwrap();
        assert_eq!(replayed.stored, 0);
        assert_eq!(replayed.rejected, 2);
    }

    /// An `update_only` batch is the mirror image: ids no segment holds are
    /// reported missing rather than created.
    #[test]
    fn update_only_batch_does_not_create_missing_ids() {
        let dir = leader_with_ten_points("edge-update-update-only");
        recreate_payload_storages_append_only(dir.path());

        let existing = PointStructPersisted {
            payload: Some(payload_json! { "kind": "updated" }),
            ..point(5)
        };

        let writer = UpdateOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
        let (_writer, outcome) = writer
            .apply_batch(conditional_batch(
                100,
                vec![existing, point(11)],
                UpdateMode::UpdateOnly,
            ))
            .unwrap();

        assert_eq!(outcome.stored, 1);
        assert_eq!(outcome.missing, 1);
        assert_eq!(outcome.rejected, 0);

        let follower = open_follower(dir.path());
        assert_eq!(exact_count(&follower), 10);
        assert_eq!(
            scrolled_ids(&follower),
            (1..=10).map(ExtendedPointId::NumId).collect::<Vec<_>>(),
        );
        let results = follower
            .retrieve(
                RetrieveRequestBuilder::new(vec![ExtendedPointId::NumId(5)])
                    .with_payload(WithPayloadInterface::Bool(true))
                    .build(),
            )
            .unwrap();
        assert_eq!(
            results[0].payload,
            Some(payload_json! { "kind": "updated" })
        );
    }
}

/// A claimed target opens non-writable; a created appendable takes the writes.
#[test]
fn optimizing_target_gets_a_created_appendable() {
    use std::collections::HashMap;

    use common::universal_io::MmapFs;
    use shard::files::segment_manifest_path;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::UpsertPoints;
    use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
    use uuid::Uuid;

    use crate::read_only::ManifestSegmentEnumerator;

    init_serverless_feature_flags();
    let dir = leader_with_ten_points("edge-update-roll");

    let manifest_path = segment_manifest_path(dir.path());
    let mut manifest: SegmentsManifest =
        serde_json::from_slice(&fs_err::read(&manifest_path).unwrap()).unwrap();
    let old: Uuid = *manifest.iter().next().unwrap().0;
    manifest.set(
        old,
        SegmentManifestState::Optimizing {
            holder: "idx".to_string(),
            lease_until: u64::MAX,
        },
    );
    fs_err::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open(
        MmapFs,
        dir.path(),
        ManifestSegmentEnumerator::new(MmapFs, dir.path()),
    )
    .unwrap();
    assert_eq!(
        writer.write_target(),
        None,
        "a claimed target must not take appends",
    );

    let (writer, fresh) = writer.create_appendable_from(old, &HashMap::new()).unwrap();
    assert_ne!(fresh, old);
    assert_eq!(writer.write_target(), Some(fresh));

    let batch = [(
        100,
        PointOperation(UpsertPoints(PointsList(vec![point(42)]))),
    )];
    let (_writer, outcome) = writer.apply_batch(batch).unwrap();
    assert_eq!(outcome.stored, 1);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 11);
    assert!(scrolled_ids(&follower).contains(&ExtendedPointId::NumId(42)));
}

/// A segmentless shard (empty manifest) opens with no target; `create_appendable`
/// bootstraps the first one.
#[test]
fn empty_manifest_shard_bootstraps_an_appendable() {
    use std::collections::HashMap;

    use common::universal_io::MmapFs;
    use shard::files::segment_manifest_path;
    use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
    use shard::operations::point_ops::PointOperations::UpsertPoints;

    use crate::read_only::ManifestSegmentEnumerator;
    use crate::read_only::tests::{init_serverless_feature_flags, point, test_config};

    init_serverless_feature_flags();
    let dir = tempfile::Builder::new()
        .prefix("edge-update-bootstrap")
        .tempdir()
        .unwrap();
    fs_err::write(segment_manifest_path(dir.path()), "{}").unwrap();

    let writer = UpdateOnlyEdgeShard::<MmapFile>::open(
        MmapFs,
        dir.path(),
        ManifestSegmentEnumerator::new(MmapFs, dir.path()),
    )
    .unwrap();
    assert_eq!(writer.segments_count(), 0);
    assert_eq!(writer.write_target(), None);

    let config = test_config().plain_segment_config();
    let (writer, fresh) = writer.create_appendable(&config, &HashMap::new()).unwrap();
    assert_eq!(writer.write_target(), Some(fresh));

    let batch = [(7, PointOperation(UpsertPoints(PointsList(vec![point(1)]))))];
    let (_writer, outcome) = writer.apply_batch(batch).unwrap();
    assert_eq!(outcome.stored, 1);

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), 1);
}
