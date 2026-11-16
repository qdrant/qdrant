//! Versioned deletes end to end: the writer stamps every delete with its batch version, and a
//! follower that gates deletes on a waterline never loses a moved point, applies deletes once the
//! waterline covers them, and holds them while a reload is incomplete.
//!
//! The torn views are real directories: the shard is snapshotted before and after a move, and a
//! follower opens a directory composed from those snapshots per segment.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::universal_io::{MmapFile, MmapFs};
use parking_lot::Mutex;
use segment::id_tracker::delete_versions::DeleteVersioning;
use segment::types::{ExtendedPointId, SeqNumberType};
use shard::files::SEGMENTS_PATH;
use shard::operations::CollectionUpdateOperations;
use shard::operations::CollectionUpdateOperations::PointOperation;
use uuid::Uuid;

use super::store::{recreate_payload_storages_append_only, store_batch};
use super::vacuumed_leader;
use crate::PointOperations::DeletePoints;
use crate::read_only::tests::{exact_count, open_follower, point};
use crate::{
    CountRequest, EdgeScope, EdgeShardRead as _, ListedSegment, LocalSegmentEnumerator,
    OperationResult, PointApplyKind, PointStructPersisted, ReadOnlyEdgeShard, ReadOnlyOpenOptions,
    RetrieveRequestBuilder, SegmentEnumerator, UpdateOnlyEdgeShard, WithPayloadInterface,
    WithVector,
};

/// The point the fixture moves; it lives in an immutable segment after the vacuum.
const MOVED: u64 = 500;
/// Another point of the immutable segment, deleted plainly.
const DELETED: u64 = 600;
/// The vector the move gives the point, to tell its new copy from its old one.
const NEW_VECTOR: f32 = 5000.0;
/// The cursor before the move.
const BEFORE: SeqNumberType = 100;
/// The version of the move.
const MOVE: SeqNumberType = 200;
/// The version of the plain deletes after it.
const DELETE: SeqNumberType = 300;
/// Points in the vacuumed shard: 301 to 1000.
const POINTS: usize = 700;

fn segment_path(shard: &Path, uuid: Uuid) -> PathBuf {
    shard.join(SEGMENTS_PATH).join(uuid.to_string())
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

fn delete_batch(
    version: SeqNumberType,
    ids: impl IntoIterator<Item = u64>,
) -> [(SeqNumberType, CollectionUpdateOperations); 1] {
    let ids = ids.into_iter().map(ExtendedPointId::NumId).collect();
    [(version, PointOperation(DeletePoints { ids }))]
}

fn writer(shard: &Path) -> UpdateOnlyEdgeShard<MmapFs> {
    UpdateOnlyEdgeShard::<MmapFs>::open_mmap(shard)
        .unwrap()
        .with_delete_versioning(DeleteVersioning::Versioned)
}

/// Move [`MOVED`] out of the immutable segment, returning that segment and the write target.
fn apply_move(shard: &Path) -> (Uuid, Uuid) {
    let writer = writer(shard);
    let target = writer.write_target().expect("a write target");
    let moved = PointStructPersisted {
        vector: point(NEW_VECTOR as u64).vector,
        ..point(MOVED)
    };
    let (_writer, outcome) = writer.apply_batch(store_batch(MOVE, vec![moved])).unwrap();
    let record = &outcome.points[0];
    assert_eq!(record.kind, PointApplyKind::Stored);
    let [(source, _)] = record.tombstoned[..] else {
        panic!("expected one old copy, got {:?}", record.tombstoned);
    };
    assert_ne!(source, target, "the point must move between segments");
    (source, target)
}

/// A follower over `shard` that gates deletes on `waterline`, discovering segments through
/// `enumerator`.
fn open_gated_with(
    shard: &Path,
    enumerator: impl SegmentEnumerator + 'static,
    waterline: SeqNumberType,
) -> ReadOnlyEdgeShard<MmapFile> {
    ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        shard,
        enumerator,
        ReadOnlyOpenOptions {
            config: None,
            load_profile: None,
            pools: Default::default(),
            waterline: Some(waterline),
        },
        &EdgeScope::new(),
    )
    .unwrap()
}

fn open_gated(shard: &Path, waterline: SeqNumberType) -> ReadOnlyEdgeShard<MmapFile> {
    open_gated_with(shard, LocalSegmentEnumerator::new(shard), waterline)
}

/// The first vector component the follower serves for `id`, `None` if it serves no such point.
fn served_vector(follower: &ReadOnlyEdgeShard<MmapFile>, id: u64) -> Option<f32> {
    use segment::data_types::vectors::{VectorInternal, VectorStructInternal};

    let records = follower
        .retrieve(
            RetrieveRequestBuilder::new(vec![ExtendedPointId::NumId(id)])
                .with_payload(WithPayloadInterface::Bool(false))
                .with_vector(WithVector::Bool(true))
                .build(),
        )
        .unwrap();
    let record = records.into_iter().next()?;
    let VectorStructInternal::Named(named) = record.vector.expect("vectors were requested") else {
        panic!("expected named vectors");
    };
    let Some(VectorInternal::Dense(dense)) = named.into_values().next() else {
        panic!("expected one named dense vector");
    };
    Some(dense[0])
}

/// The approximate count sums each segment's live points without deduplicating ids, so a held
/// old copy shows up in it.
fn approximate_count(follower: &ReadOnlyEdgeShard<MmapFile>) -> usize {
    follower
        .count(CountRequest {
            filter: None,
            exact: false,
        })
        .unwrap()
}

/// Every combination of a follower's views of the source and the target keeps serving the moved
/// point while the waterline is from before the move; the new copy wins once it is in view. The
/// view where the source's delete arrived first loses the point when deletes apply at once.
#[test]
fn torn_views_never_lose_the_moved_point() {
    let leader = vacuumed_leader("edge-deletes-torn");
    recreate_payload_storages_append_only(leader.path());
    let root = tempfile::Builder::new()
        .prefix("edge-deletes-views")
        .tempdir()
        .unwrap();
    let before = root.path().join("before");
    copy_dir(leader.path(), &before);
    let (source, target) = apply_move(leader.path());
    let after = root.path().join("after");
    copy_dir(leader.path(), &after);
    let old_vector = served_vector(&open_follower(&before), MOVED).unwrap();

    let compose = |name: &str, source_from: &Path, target_from: &Path| {
        let view = root.path().join(name);
        copy_dir(&after, &view);
        for (uuid, from) in [(source, source_from), (target, target_from)] {
            fs_err::remove_dir_all(segment_path(&view, uuid)).unwrap();
            copy_dir(&segment_path(from, uuid), &segment_path(&view, uuid));
        }
        view
    };

    for (name, source_from, target_from, vector) in [
        ("neither", &before, &before, old_vector),
        ("source-first", &after, &before, old_vector),
        ("target-first", &before, &after, NEW_VECTOR),
        ("both", &after, &after, NEW_VECTOR),
    ] {
        let view = compose(name, source_from, target_from);
        let follower = open_gated(&view, BEFORE);
        assert_eq!(served_vector(&follower, MOVED), Some(vector), "{name}");
        assert_eq!(exact_count(&follower), POINTS, "{name}");
    }

    // Applying deletes at once, the source's delete takes the only copy in view
    let view = compose("source-first-immediate", &after, &before);
    let follower = open_follower(&view);
    assert_eq!(served_vector(&follower, MOVED), None);

    // A waterline covering the move: the old copy is gone, the new one served once
    let follower = open_gated(&after, MOVE);
    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    assert_eq!(approximate_count(&follower), POINTS);
}

/// A live follower keeps a moved point's old copy next to its new one until a reload's waterline
/// covers the move, then deletes it as the reload's last step. Plain deletes wait for the
/// waterline the same way, and a late, lower waterline changes nothing.
#[test]
fn deletes_wait_for_the_waterline() {
    let leader = vacuumed_leader("edge-deletes-waterline");
    recreate_payload_storages_append_only(leader.path());
    let follower = open_gated(leader.path(), BEFORE);

    apply_move(leader.path());
    follower.live_reload_up_to(BEFORE).unwrap();
    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    assert_eq!(exact_count(&follower), POINTS);
    assert_eq!(approximate_count(&follower), POINTS + 1, "both copies");

    follower.live_reload_up_to(MOVE).unwrap();
    assert_eq!(approximate_count(&follower), POINTS);

    let (_writer, outcome) = writer(leader.path())
        .apply_batch(delete_batch(DELETE, [MOVED, DELETED]))
        .unwrap();
    assert_eq!(outcome.deleted, 2);
    follower.live_reload_up_to(DELETE - 1).unwrap();
    assert_eq!(served_vector(&follower, MOVED), Some(NEW_VECTOR));
    assert!(served_vector(&follower, DELETED).is_some());

    follower.live_reload_up_to(DELETE).unwrap();
    assert_eq!(served_vector(&follower, MOVED), None);
    assert_eq!(served_vector(&follower, DELETED), None);
    assert_eq!(exact_count(&follower), POINTS - 2);

    // A late waterline, and a reload without one, keep what was applied
    follower.live_reload_up_to(BEFORE).unwrap();
    follower.live_reload().unwrap();
    assert_eq!(exact_count(&follower), POINTS - 2);
    assert_eq!(approximate_count(&follower), POINTS - 2);
}

/// Lists the segments of a shard directory, hiding some and adding some that do not exist.
#[derive(Clone, Default)]
struct Listing {
    hidden: Arc<Mutex<Vec<Uuid>>>,
    missing: Arc<Mutex<Vec<Uuid>>>,
}

struct ListingEnumerator {
    inner: LocalSegmentEnumerator,
    shard: PathBuf,
    listing: Listing,
}

impl SegmentEnumerator for ListingEnumerator {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, ListedSegment>> {
        let mut listed = self.inner.list_segments()?;
        let hidden = self.listing.hidden.lock().clone();
        listed.retain(|uuid, _| !hidden.contains(uuid));
        for &uuid in self.listing.missing.lock().iter() {
            let path = segment_path(&self.shard, uuid);
            listed.insert(
                uuid,
                ListedSegment {
                    path,
                    writable: false,
                },
            );
        }
        Ok(listed)
    }
}

fn open_listed(
    shard: &Path,
    listing: &Listing,
    waterline: SeqNumberType,
) -> ReadOnlyEdgeShard<MmapFile> {
    let enumerator = ListingEnumerator {
        inner: LocalSegmentEnumerator::new(shard),
        shard: shard.to_path_buf(),
        listing: listing.clone(),
    };
    open_gated_with(shard, enumerator, waterline)
}

/// While a listed segment fails to load, the gate stays where the last complete reload left it, so
/// newer deletes wait; once every listed segment loads, they apply.
#[test]
fn incomplete_reloads_hold_newer_deletes() {
    let leader = vacuumed_leader("edge-deletes-incomplete");
    recreate_payload_storages_append_only(leader.path());
    let listing = Listing::default();
    let follower = open_listed(leader.path(), &listing, BEFORE);

    let (_writer, outcome) = writer(leader.path())
        .apply_batch(delete_batch(DELETE, [DELETED]))
        .unwrap();
    assert_eq!(outcome.deleted, 1);

    listing.missing.lock().push(Uuid::new_v4());
    follower.live_reload_up_to(DELETE).unwrap();
    assert!(served_vector(&follower, DELETED).is_some(), "incomplete");

    listing.missing.lock().clear();
    follower.live_reload_up_to(DELETE).unwrap();
    assert_eq!(served_vector(&follower, DELETED), None);
}

/// A segment the manifest stops listing keeps serving, and keeps reloading so deletes written to it
/// just before still apply, until a reload loads every listed segment. Then it is dropped.
#[test]
fn unlisted_segments_stay_until_a_complete_reload() {
    let leader = vacuumed_leader("edge-deletes-unlisted");
    recreate_payload_storages_append_only(leader.path());
    let listing = Listing::default();
    let follower = open_listed(leader.path(), &listing, BEFORE);
    let writer = writer(leader.path());
    let preview = writer
        .preview_batch(delete_batch(DELETE, [DELETED]))
        .unwrap();
    let immutable = preview.points[0].current.as_ref().unwrap().segment;
    let segments = follower.segments_count();

    // A complete reload raises the gate past the coming delete
    follower.live_reload_up_to(DELETE + 100).unwrap();

    // Unlisted while a listed segment fails to load: kept, reloaded, its delete applied
    listing.hidden.lock().push(immutable);
    listing.missing.lock().push(Uuid::new_v4());
    let (_writer, outcome) = writer.apply_batch(delete_batch(DELETE, [DELETED])).unwrap();
    assert_eq!(outcome.deleted, 1);
    follower.live_reload_up_to(DELETE + 100).unwrap();
    assert_eq!(follower.segments_count(), segments, "kept");
    assert_eq!(served_vector(&follower, DELETED), None);
    assert_eq!(exact_count(&follower), POINTS - 1);

    // A complete reload drops it
    listing.missing.lock().clear();
    follower.live_reload_up_to(DELETE + 100).unwrap();
    assert_eq!(follower.segments_count(), segments - 1);
    assert!(exact_count(&follower) < POINTS - 1);
}
