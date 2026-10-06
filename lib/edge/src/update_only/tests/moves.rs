//! Point moves end to end: the writer records a copy-on-write move in the move logs of both
//! segments, and a follower whose views of the two segments are torn in any way keeps serving the
//! point, never twice and never a deleted one.
//!
//! The torn views are real directories: the shard is snapshotted before a move, after it, and after
//! a later plain delete of the moved point, and each follower opens a directory composed from those
//! snapshots per segment, down to single files for the states between two writer steps.

use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::universal_io::{MmapFile, MmapFs};
use segment::id_tracker::point_moves::{MoveEntry, MoveKind, PointMovesView, point_moves_path};
use segment::types::{ExtendedPointId, WithPayloadInterface, WithVector};
use shard::files::{SEGMENTS_PATH, segment_manifest_path};
use shard::operations::CollectionUpdateOperations;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointOperations::DeletePoints;
use shard::operations::point_ops::PointStructPersisted;
use shard::segment_manifest::SegmentsManifest;
use tempfile::TempDir;
use uuid::Uuid;

use super::store::{recreate_payload_storages_append_only, store_batch};
use super::vacuumed_leader;
use crate::read_only::tests::{exact_count, open_follower, point};
use crate::read_only::{LocalSegmentEnumerator, ManifestSegmentEnumerator};
use crate::read_view::EdgeShardRead as _;
use crate::update_only::{PointApplyKind, UpdateOnlyEdgeShard};
use crate::{CountRequest, ReadOnlyEdgeShard, RetrieveRequestBuilder};

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

/// The writer records the move in both logs, with the same pair of slots, and a plain delete
/// records nothing.
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

    // A plain delete of the moved point records nothing
    let (_writer, outcome) = writer.apply_batch(delete_batch(2001, [MOVED])).unwrap();
    assert_eq!(outcome.deleted, 1);
    assert_eq!(logs(), after_move);
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

/// Points in the vacuumed shard: 301 to 1000.
const POINTS: usize = 700;

/// Snapshots of one shard around a move of [`MOVED`] from an immutable segment to the write target.
struct MoveFixture {
    /// Holds every snapshot and composed view.
    root: TempDir,
    /// Before the move.
    before: PathBuf,
    /// After the move.
    after: PathBuf,
    /// After a later plain delete of the moved point, in the write target.
    deleted: PathBuf,
    /// The immutable segment the point moved out of.
    source: Uuid,
    /// The write target the point moved into.
    target: Uuid,
}

impl MoveFixture {
    fn new() -> Self {
        let leader = vacuumed_leader("edge-moves");
        recreate_payload_storages_append_only(leader.path());
        let root = tempfile::Builder::new()
            .prefix("edge-moves-views")
            .tempdir()
            .unwrap();

        let before = root.path().join("before");
        copy_dir(leader.path(), &before);

        let writer = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(leader.path()).unwrap();
        let target = writer.write_target().expect("a write target");
        let moved = PointStructPersisted {
            vector: point(NEW_VECTOR as u64).vector,
            ..point(MOVED)
        };
        let (writer, outcome) = writer.apply_batch(store_batch(2000, vec![moved])).unwrap();
        let record = &outcome.points[0];
        assert_eq!(record.kind, PointApplyKind::Stored);
        let [(source, _)] = record.tombstoned[..] else {
            panic!("expected one old copy, got {:?}", record.tombstoned);
        };
        assert_ne!(source, target, "the point must move between segments");
        let (stored_in, _) = record.stored_at.expect("a stored point has a slot");
        assert_eq!(stored_in, target);

        let after = root.path().join("after");
        copy_dir(leader.path(), &after);

        let (_writer, outcome) = writer.apply_batch(delete_batch(2001, [MOVED])).unwrap();
        assert_eq!(outcome.deleted, 1);
        let deleted = root.path().join("deleted");
        copy_dir(leader.path(), &deleted);

        Self {
            root,
            before,
            after,
            deleted,
            source,
            target,
        }
    }

    /// A shard directory named `name` whose source segment comes from the `source` snapshot and
    /// whose write target comes from the `target` snapshot; every other segment is the same in all
    /// snapshots. `tweak` then adjusts single files.
    fn compose(
        &self,
        name: &str,
        source: &Path,
        target: &Path,
        tweak: impl FnOnce(&Self, &Path),
    ) -> PathBuf {
        let view = self.root.path().join(name);
        copy_dir(&self.after, &view);
        for (uuid, from) in [(self.source, source), (self.target, target)] {
            let segment = segment_path(&view, uuid);
            fs_err::remove_dir_all(&segment).unwrap();
            copy_dir(&segment_path(from, uuid), &segment);
        }
        tweak(self, &view);
        view
    }
}

/// Replace `file` of segment `uuid` in `view` with its version from the `from` snapshot, or remove
/// it where that snapshot has none.
fn take_file(view: &Path, uuid: Uuid, file: &str, from: &Path) {
    let source = segment_path(from, uuid).join(file);
    let destination = segment_path(view, uuid).join(file);
    if source.exists() {
        fs_err::copy(&source, &destination).unwrap();
    } else if destination.exists() {
        fs_err::remove_file(&destination).unwrap();
    }
}

fn copy_dir(from: &Path, to: &Path) {
    for entry in walkdir::WalkDir::new(from) {
        let entry = entry.unwrap();
        let relative = entry.path().strip_prefix(from).unwrap();
        let destination = to.join(relative);
        if entry.file_type().is_dir() {
            fs_err::create_dir_all(&destination).unwrap();
        } else {
            fs_err::copy(entry.path(), &destination).unwrap();
        }
    }
}

/// The first component of the only named dense vector of `vectors`.
fn first_component(vectors: segment::data_types::vectors::VectorStructInternal) -> f32 {
    use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
    let VectorStructInternal::Named(named) = vectors else {
        panic!("expected named vectors");
    };
    let Some(VectorInternal::Dense(dense)) = named.into_values().next() else {
        panic!("expected one named dense vector");
    };
    dense[0]
}

/// The first vector component the follower serves for `id`, `None` if it serves no such point.
fn served_vector(follower: &ReadOnlyEdgeShard<MmapFile>, id: u64) -> Option<f32> {
    let records = follower
        .retrieve(
            RetrieveRequestBuilder::new(vec![ExtendedPointId::NumId(id)])
                .with_payload(WithPayloadInterface::Bool(false))
                .with_vector(WithVector::Bool(true))
                .build(),
        )
        .unwrap();
    let record = records.into_iter().next()?;
    Some(first_component(
        record.vector.expect("vectors were requested"),
    ))
}

/// The approximate count sums each segment's live points without deduplicating ids, so a stale
/// second copy shows up in it.
fn approximate_count(follower: &ReadOnlyEdgeShard<MmapFile>) -> usize {
    follower
        .count(CountRequest {
            filter: None,
            exact: false,
        })
        .unwrap()
}

/// What a follower over `view` must serve: the point's vector (`None` when deleted), and the
/// number of points, exactly and approximately alike.
fn assert_view(view: &Path, vector: Option<f32>, points: usize, case: &str) {
    assert_serves(&open_follower(view), vector, points, case);
}

/// What `follower` must serve, see [`assert_view`].
fn assert_serves(
    follower: &ReadOnlyEdgeShard<MmapFile>,
    vector: Option<f32>,
    points: usize,
    case: &str,
) {
    assert_eq!(
        served_vector(follower, MOVED),
        vector,
        "{case}: served copy"
    );
    assert_eq!(exact_count(follower), points, "{case}: exact count");
    assert_eq!(
        approximate_count(follower),
        points,
        "{case}: approximate count, a stale copy counts twice",
    );
}

/// Every combination of the follower's views of the source and the target serves the point once,
/// at a version it had: the old one until the target shows the new copy, the new one from then on,
/// and none once it is deleted.
#[test]
fn every_torn_view_serves_the_point_once() {
    let fixture = MoveFixture::new();
    let old = Some(MOVED as f32);
    let new = Some(NEW_VECTOR);
    let none = |_: &MoveFixture, _: &Path| {};

    let before = fixture.before.clone();
    let after = fixture.after.clone();
    let deleted = fixture.deleted.clone();

    // Consistent views
    assert_view(
        &fixture.compose("both-before", &before, &before, none),
        old,
        POINTS,
        "both before",
    );
    assert_view(
        &fixture.compose("both-after", &after, &after, none),
        new,
        POINTS,
        "both after",
    );

    // The source is fresher: its tombstone is held until the target shows the new copy. Without
    // move logs the point is missing here.
    assert_view(
        &fixture.compose("source-fresher", &after, &before, none),
        old,
        POINTS,
        "source fresher",
    );

    // The target is fresher: its moved-in record masks the old copy, which S still serves.
    assert_view(
        &fixture.compose("target-fresher", &before, &after, none),
        new,
        POINTS,
        "target fresher",
    );

    // The source's record without its tombstone: masks once the target shows the new copy
    let record_only = |fixture: &MoveFixture, view: &Path| {
        take_file(view, fixture.source, "id_tracker.deleted", &fixture.before);
    };
    assert_view(
        &fixture.compose("record-only-target-before", &after, &before, record_only),
        old,
        POINTS,
        "source record only, target before",
    );
    assert_view(
        &fixture.compose("record-only-target-after", &after, &after, record_only),
        new,
        POINTS,
        "source record only, target after",
    );

    // The target's data and moved-in record without the published version: the slot has not
    // settled, so nothing is masked and the source keeps serving the old copy
    let unpublished = |fixture: &MoveFixture, view: &Path| {
        take_file(
            view,
            fixture.target,
            "mutable_id_tracker.versions",
            &fixture.before,
        );
    };
    assert_view(
        &fixture.compose(
            "target-unpublished-source-after",
            &after,
            &after,
            unpublished,
        ),
        old,
        POINTS,
        "target unpublished, source after",
    );
    assert_view(
        &fixture.compose(
            "target-unpublished-source-before",
            &before,
            &after,
            unpublished,
        ),
        old,
        POINTS,
        "target unpublished, source before",
    );

    // The moved point is deleted after the move. A source view from before the move must not bring
    // it back: the target's moved-in record masks it. Without move logs it comes back.
    assert_view(
        &fixture.compose("deleted-source-before", &before, &deleted, none),
        None,
        POINTS - 1,
        "deleted, source before",
    );
    assert_view(
        &fixture.compose("deleted-source-after", &after, &deleted, none),
        None,
        POINTS - 1,
        "deleted, source after",
    );
}

/// A move into a write target the manifest does not list yet: the target's directory exists, so the
/// follower holds the source's tombstone instead of taking the target for superseded, and releases
/// it once the manifest lists the target.
#[test]
fn move_into_an_unlisted_target_waits_for_its_listing() {
    let fixture = MoveFixture::new();
    let view = fixture.compose(
        "unlisted-target",
        &fixture.after.clone(),
        &fixture.after.clone(),
        |_, _| {},
    );

    let manifest_path = segment_manifest_path(&view);
    let listed: SegmentsManifest =
        serde_json::from_slice(&fs_err::read(&manifest_path).unwrap()).unwrap();
    let mut unlisted = listed.clone();
    assert!(unlisted.remove(&fixture.target).is_some());
    fs_err::write(&manifest_path, serde_json::to_vec(&unlisted).unwrap()).unwrap();

    let follower = ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        &view,
        ManifestSegmentEnumerator::new(MmapFs, &view),
        None,
        None,
        Default::default(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(
        served_vector(&follower, MOVED),
        Some(MOVED as f32),
        "the old copy serves while the target is not listed",
    );

    fs_err::write(&manifest_path, serde_json::to_vec(&listed).unwrap()).unwrap();
    follower.live_reload().unwrap();
    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    assert_eq!(approximate_count(&follower), POINTS);
}

/// A follower whose source view was fresher serves the old copy, and the pass that brings the
/// target's new copy into view also removes the old one.
#[test]
fn held_delete_is_released_once_the_target_catches_up() {
    let fixture = MoveFixture::new();
    let view = fixture.compose(
        "catching-up",
        &fixture.after.clone(),
        &fixture.before.clone(),
        |_, _| {},
    );

    let follower = open_follower(&view);
    assert_eq!(served_vector(&follower, MOVED), Some(MOVED as f32));
    assert_eq!(approximate_count(&follower), POINTS);

    // The target catches up: its files grow in place, as the writer's appends do
    let target = segment_path(&fixture.after, fixture.target);
    for entry in walkdir::WalkDir::new(&target) {
        let entry = entry.unwrap();
        let relative = entry.path().strip_prefix(&target).unwrap();
        let destination = segment_path(&view, fixture.target).join(relative);
        if entry.file_type().is_dir() {
            fs_err::create_dir_all(&destination).unwrap();
        } else {
            fs_err::copy(entry.path(), &destination).unwrap();
        }
    }
    follower.live_reload().unwrap();

    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    assert_eq!(exact_count(&follower), POINTS);
    assert_eq!(
        approximate_count(&follower),
        POINTS,
        "the old copy must be gone in the same pass",
    );
}

/// A read that started before a pass installed the target's new copy may have missed it, so the
/// pass masks the old copy only once that read is done. Until then both copies serve, which only
/// the approximate count notices.
#[test]
fn masks_wait_for_reads_of_the_previous_epoch() {
    let fixture = MoveFixture::new();
    let view = fixture.compose(
        "epoch",
        &fixture.after.clone(),
        &fixture.before.clone(),
        |_, _| {},
    );
    let follower = std::sync::Arc::new(open_follower(&view));
    assert_eq!(served_vector(&follower, MOVED), Some(MOVED as f32));

    let target = segment_path(&fixture.after, fixture.target);
    for entry in walkdir::WalkDir::new(&target) {
        let entry = entry.unwrap();
        let destination =
            segment_path(&view, fixture.target).join(entry.path().strip_prefix(&target).unwrap());
        if entry.file_type().is_dir() {
            fs_err::create_dir_all(&destination).unwrap();
        } else {
            fs_err::copy(entry.path(), &destination).unwrap();
        }
    }

    let read_in_flight = follower.hold_read_epoch();
    let reload = {
        let follower = follower.clone();
        std::thread::spawn(move || follower.live_reload().unwrap())
    };

    // The pass installs the new copy, then waits for the read in flight before masking the old one
    let started = std::time::Instant::now();
    while served_vector(&follower, MOVED) != Some(NEW_VECTOR) {
        assert!(started.elapsed() < std::time::Duration::from_secs(4));
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        !reload.is_finished(),
        "the pass waits for the read in flight"
    );
    assert_eq!(
        approximate_count(&follower),
        POINTS + 1,
        "both copies serve"
    );

    drop(read_in_flight);
    reload.join().unwrap();
    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    assert_eq!(approximate_count(&follower), POINTS);
}

/// Move segment `uuid` of `view` forward to its state in the `from` snapshot, the way the writer
/// changes it: append-only files grow in place, the deleted mask is replaced whole.
fn advance_segment(view: &Path, uuid: Uuid, from: &Path) {
    let from = segment_path(from, uuid);
    let to = segment_path(view, uuid);
    for entry in walkdir::WalkDir::new(&from) {
        let entry = entry.unwrap();
        let destination = to.join(entry.path().strip_prefix(&from).unwrap());
        if entry.file_type().is_dir() {
            fs_err::create_dir_all(&destination).unwrap();
        } else if entry.file_name() == "id_tracker.deleted" {
            let staged = destination.with_extension("staged");
            fs_err::copy(entry.path(), &staged).unwrap();
            fs_err::rename(&staged, &destination).unwrap();
        } else {
            fs_err::copy(entry.path(), &destination).unwrap();
        }
    }
}

/// A target keeps a settled moved-in record only until the source applied its mask: the pass after
/// the one that masked the old copy forgets the record. A record naming a source the follower does
/// not hold yet stays, and masks the old copy once the source loads.
#[test]
fn settled_moved_in_records_are_pruned_once_applied() {
    let fixture = MoveFixture::new();
    // The target shows the move and the source does not, so only the target's record masks the
    // old copy
    let view = fixture.compose(
        "pruned",
        &fixture.before.clone(),
        &fixture.after.clone(),
        |_, _| {},
    );

    let manifest_path = segment_manifest_path(&view);
    let listed: SegmentsManifest =
        serde_json::from_slice(&fs_err::read(&manifest_path).unwrap()).unwrap();
    let mut unlisted = listed.clone();
    assert!(unlisted.remove(&fixture.source).is_some());
    fs_err::write(&manifest_path, serde_json::to_vec(&unlisted).unwrap()).unwrap();

    let follower = ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        &view,
        ManifestSegmentEnumerator::new(MmapFs, &view),
        None,
        None,
        Default::default(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    for _ in 0..2 {
        assert_eq!(
            follower.settled_moved_in_count(),
            1,
            "the record waits while its source is not listed",
        );
        follower.live_reload().unwrap();
    }

    // The source is listed: it loads with the old copy masked, and the pass after forgets the
    // record
    fs_err::write(&manifest_path, serde_json::to_vec(&listed).unwrap()).unwrap();
    follower.live_reload().unwrap();
    assert_serves(&follower, Some(NEW_VECTOR), POINTS, "source loaded");
    follower.live_reload().unwrap();
    assert_eq!(follower.settled_moved_in_count(), 0, "record forgotten");
    assert_serves(&follower, Some(NEW_VECTOR), POINTS, "record forgotten");

    // The source's own record and tombstone arrive later and change nothing
    advance_segment(&view, fixture.source, &fixture.after);
    follower.live_reload().unwrap();
    assert_eq!(follower.settled_moved_in_count(), 0);
    assert_serves(&follower, Some(NEW_VECTOR), POINTS, "source caught up");
}
