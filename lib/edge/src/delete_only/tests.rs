//! Fixture: points 1-300 in an immutable segment, plus newer copies of 1-3 in
//! the appendable only, as a writer leaves deferred points.

use std::collections::HashMap;

use common::ambient;
use common::universal_io::MmapFs;
use segment::segment::update_only::TrackerLookup;
use segment::types::{ExtendedPointId, SeqNumberType};
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use tempfile::TempDir;
use uuid::Uuid;

use crate::PointOperations::{DeletePoints, UpsertPoints};
use crate::read_only::tests::{exact_count, open_follower, point, test_config, upsert};
use crate::{
    DeleteOnlyEdgeShard, EdgeConfig, EdgeOptimizersConfig, EdgeShard, ListedSegment,
    LocalSegmentEnumerator, OperationResult, SegmentEnumerator, UpdateOnlyEdgeShard,
};

/// Version of the newer copies of points 1-3.
const NEWER: SeqNumberType = 100;

/// Segment enumerator that lists only the `keep` segments of `inner`.
struct Only {
    inner: LocalSegmentEnumerator,
    keep: Vec<Uuid>,
}

impl SegmentEnumerator for Only {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, ListedSegment>> {
        let mut listed = self.inner.list_segments()?;
        listed.retain(|uuid, _| self.keep.contains(uuid));
        Ok(listed)
    }
}

fn only(dir: &TempDir, keep: &[Uuid]) -> Only {
    Only {
        inner: LocalSegmentEnumerator::new(dir.path()),
        keep: keep.to_vec(),
    }
}

/// `(immutable, appendable)` segment uuids of the shard at `dir`.
fn segments(dir: &TempDir) -> (Uuid, Uuid) {
    let _scope = ambient::test_guard();
    let (mut immutable, mut appendable) = (None, None);
    for (uuid, listed) in LocalSegmentEnumerator::new(dir.path())
        .list_segments()
        .unwrap()
    {
        let lookup = TrackerLookup::open(MmapFs, &listed.path).unwrap();
        *if lookup.appendable {
            &mut appendable
        } else {
            &mut immutable
        } = Some(uuid);
    }
    (immutable.unwrap(), appendable.unwrap())
}

fn shadowed_leader(prefix: &str) -> (TempDir, Uuid) {
    let dir = tempfile::Builder::new().prefix(prefix).tempdir().unwrap();
    let config = EdgeConfig {
        optimizers: Some(EdgeOptimizersConfig {
            indexing_threshold: Some(1),
            ..EdgeOptimizersConfig::default()
        }),
        ..test_config()
    };
    let leader = EdgeShard::new(dir.path(), config).unwrap();
    // 300 one-dimensional points clear the 1 KB indexing threshold.
    upsert(&leader, 1..=300);
    leader.flush().unwrap();
    assert!(
        leader.optimize().unwrap(),
        "expected the points to be indexed"
    );
    leader.flush().unwrap();
    drop(leader);

    let (immutable, appendable) = segments(&dir);
    let writer =
        UpdateOnlyEdgeShard::open(MmapFs, dir.path(), only(&dir, &[appendable]), None).unwrap();
    let points = (1..=3).map(point).collect();
    let (_writer, outcome) = writer
        .apply_batch([(NEWER, PointOperation(UpsertPoints(PointsList(points))))])
        .unwrap();
    assert_eq!(outcome.stored, 3);
    (dir, immutable)
}

/// `find_outdated` for points 1-3 at `version`, sorted.
fn outdated(shard: &DeleteOnlyEdgeShard<MmapFs>, version: SeqNumberType) -> Vec<ExtendedPointId> {
    let mut ids = shard.find_outdated(&published(version)).unwrap();
    ids.sort();
    ids
}

fn ids(ids: impl IntoIterator<Item = u64>) -> Vec<ExtendedPointId> {
    ids.into_iter().map(ExtendedPointId::NumId).collect()
}

fn published(version: SeqNumberType) -> HashMap<ExtendedPointId, SeqNumberType> {
    (1..=3)
        .map(|id| (ExtendedPointId::NumId(id), version))
        .collect()
}

#[cfg_attr(
    windows,
    ignore = "the tombstone rewrite replaces id_tracker.deleted while the lookup holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn deletes_the_copies_older_than_the_published_ones() {
    let (dir, immutable) = shadowed_leader("edge-delete-only-delete");

    let mut shard = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();
    let stale = outdated(&shard, NEWER);
    assert_eq!(stale, ids(1..=3));
    assert_eq!(shard.delete_points(&stale, 0).unwrap(), 3);

    let reopened = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();
    assert!(outdated(&reopened, NEWER).is_empty());
    assert_eq!(exact_count(&open_follower(dir.path())), 300);
}

#[test]
fn a_newer_copy_makes_a_point_not_outdated() {
    let (dir, _) = shadowed_leader("edge-delete-only-head");

    // The appendable's copies at NEWER are past the published version.
    let shard = DeleteOnlyEdgeShard::open(MmapFs, LocalSegmentEnumerator::new(dir.path())).unwrap();
    assert!(outdated(&shard, NEWER - 1).is_empty());
}

/// A delete made after `open` survives `delete_points` once the shard is
/// reloaded.
#[cfg_attr(
    windows,
    ignore = "the tombstone rewrite replaces id_tracker.deleted while the lookup holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn a_reload_keeps_deletes_made_after_open() {
    let (dir, immutable) = shadowed_leader("edge-delete-only-refresh");
    let mut shard = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();

    let writer =
        UpdateOnlyEdgeShard::open(MmapFs, dir.path(), only(&dir, &[immutable]), None).unwrap();
    let (_writer, outcome) = writer
        .apply_batch([(NEWER + 1, PointOperation(DeletePoints { ids: ids([2, 5]) }))])
        .unwrap();
    assert_eq!(outcome.deleted, 2);

    shard.live_reload().unwrap();
    let stale = outdated(&shard, NEWER);
    assert_eq!(stale, ids([1, 3]));
    assert_eq!(shard.delete_points(&stale, 0).unwrap(), 2);

    let reopened = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();
    assert!(outdated(&reopened, NEWER).is_empty());
    // Points 1 to 3 from the appendable, the rest but 5 from the immutable.
    assert_eq!(exact_count(&open_follower(dir.path())), 299);
}

/// The deletes record the version they are given.
#[cfg_attr(
    windows,
    ignore = "the tombstone rewrite replaces id_tracker.deleted while the lookup holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn deletes_record_the_given_version() {
    use common::universal_io::MmapFile;
    use segment::id_tracker::delete_versions::{DeleteLogReader, DeleteVersioning};

    let (dir, immutable) = shadowed_leader("edge-delete-only-version");

    let mut shard = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable]))
        .unwrap()
        .with_delete_versioning(DeleteVersioning::Versioned);
    let stale = outdated(&shard, NEWER);
    assert_eq!(shard.delete_points(&stale, NEWER + 7).unwrap(), 3);

    let segment_path = LocalSegmentEnumerator::new(dir.path())
        .list_segments()
        .unwrap()
        .remove(&immutable)
        .unwrap()
        .path;
    let mut log = DeleteLogReader::<MmapFile>::new(&segment_path);
    let versions: Vec<SeqNumberType> = log
        .read_new(&MmapFs)
        .unwrap()
        .into_iter()
        .map(|entry| entry.version)
        .collect();
    assert_eq!(versions, [NEWER + 7; 3]);
}

/// A follower whose waterline is below the dedup's version keeps serving the outdated copies next
/// to the newer ones, and drops them once a waterline covers the dedup. The points stay served
/// throughout.
#[cfg_attr(
    windows,
    ignore = "the tombstone rewrite replaces id_tracker.deleted while the lookup holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn followers_drop_outdated_copies_once_the_waterline_covers_the_dedup() {
    use common::universal_io::MmapFile;
    use segment::id_tracker::delete_versions::DeleteVersioning;

    use crate::{
        CountRequest, EdgeScope, EdgeShardRead as _, ReadOnlyEdgeShard, ReadOnlyOpenOptions,
    };

    let approximate_count = |follower: &ReadOnlyEdgeShard<MmapFile>| {
        follower
            .count(CountRequest {
                filter: None,
                exact: false,
            })
            .unwrap()
    };

    let (dir, immutable) = shadowed_leader("edge-delete-only-gated");
    let follower = ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        dir.path(),
        LocalSegmentEnumerator::new(dir.path()),
        ReadOnlyOpenOptions {
            config: None,
            load_profile: None,
            pools: Default::default(),
            waterline: Some(NEWER),
        },
        &EdgeScope::new(),
    )
    .unwrap();
    assert_eq!(approximate_count(&follower), 303, "both copies of 1-3");

    let mut shard = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable]))
        .unwrap()
        .with_delete_versioning(DeleteVersioning::Versioned);
    let stale = outdated(&shard, NEWER);
    assert_eq!(shard.delete_points(&stale, NEWER + 1).unwrap(), 3);

    follower.live_reload_up_to(NEWER).unwrap();
    assert_eq!(exact_count(&follower), 300);
    assert_eq!(approximate_count(&follower), 303, "held");

    follower.live_reload_up_to(NEWER + 1).unwrap();
    assert_eq!(exact_count(&follower), 300);
    assert_eq!(approximate_count(&follower), 300);
}
