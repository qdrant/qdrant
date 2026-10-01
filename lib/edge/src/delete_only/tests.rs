//! A shard whose points 1 to 300 sit in an immutable segment, with newer copies
//! of points 1 to 3 written to the appendable alone — the state a writer
//! leaves for deferred points — and the delete-only shard retiring the older
//! copies once those newer versions are published.

use std::collections::HashMap;

use common::universal_io::MmapFs;
use segment::common::operation_error::OperationResult;
use segment::segment::update_only::TrackerLookup;
use segment::types::{ExtendedPointId, SeqNumberType};
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::{DeletePoints, UpsertPoints};
use tempfile::TempDir;
use uuid::Uuid;

use crate::delete_only::DeleteOnlyEdgeShard;
use crate::read_only::tests::{exact_count, open_follower, point, test_config, upsert};
use crate::read_only::{ListedSegment, LocalSegmentEnumerator, SegmentEnumerator};
use crate::update_only::UpdateOnlyEdgeShard;
use crate::{EdgeConfig, EdgeOptimizersConfig, EdgeShard};

/// The version the newer copies of points 1 to 3 carry.
const NEWER: SeqNumberType = 100;

/// The segments `inner` lists, narrowed to `keep`.
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
    let writer = UpdateOnlyEdgeShard::open(MmapFs, dir.path(), only(&dir, &[appendable])).unwrap();
    let points = (1..=3).map(point).collect();
    let (_writer, outcome) = writer
        .apply_batch([(NEWER, PointOperation(UpsertPoints(PointsList(points))))])
        .unwrap();
    assert_eq!(outcome.stored, 3);
    (dir, immutable)
}

fn newer(version: SeqNumberType) -> HashMap<ExtendedPointId, SeqNumberType> {
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
fn retires_the_copies_older_than_the_published_ones() {
    let (dir, immutable) = shadowed_leader("edge-delete-only-retire");

    let mut shard = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();
    assert_eq!(shard.superseded_count(&newer(NEWER)).unwrap(), 3);
    assert_eq!(shard.retire_superseded(&newer(NEWER)).unwrap(), 3);

    let reopened = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();
    assert_eq!(reopened.superseded_count(&newer(NEWER)).unwrap(), 0);
    assert_eq!(exact_count(&open_follower(dir.path())), 300);
}

#[test]
fn a_newer_head_keeps_the_older_copies() {
    let (dir, _) = shadowed_leader("edge-delete-only-head");

    // The appendable's copies at NEWER are past the published version.
    let mut shard =
        DeleteOnlyEdgeShard::open(MmapFs, LocalSegmentEnumerator::new(dir.path())).unwrap();
    assert_eq!(shard.superseded_count(&newer(NEWER - 1)).unwrap(), 0);
    assert_eq!(shard.retire_superseded(&newer(NEWER - 1)).unwrap(), 0);
}

/// A delete landing between the open and the retire must survive it, not be
/// overwritten by the deleted mask the open read.
#[cfg_attr(
    windows,
    ignore = "the tombstone rewrite replaces id_tracker.deleted while the lookup holds it \
              memory-mapped, which Windows refuses"
)]
#[test]
fn refreshes_before_retiring() {
    let (dir, immutable) = shadowed_leader("edge-delete-only-refresh");
    let mut shard = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();

    let writer = UpdateOnlyEdgeShard::open(MmapFs, dir.path(), only(&dir, &[immutable])).unwrap();
    let ids = vec![ExtendedPointId::NumId(2), ExtendedPointId::NumId(5)];
    let (_writer, outcome) = writer
        .apply_batch([(NEWER + 1, PointOperation(DeletePoints { ids }))])
        .unwrap();
    assert_eq!(outcome.deleted, 2);

    assert_eq!(shard.retire_superseded(&newer(NEWER)).unwrap(), 2);

    let reopened = DeleteOnlyEdgeShard::open(MmapFs, only(&dir, &[immutable])).unwrap();
    assert_eq!(reopened.superseded_count(&newer(NEWER)).unwrap(), 0);
    // Points 1 to 3 from the appendable, the rest but 5 from the immutable.
    assert_eq!(exact_count(&open_follower(dir.path())), 299);
}
