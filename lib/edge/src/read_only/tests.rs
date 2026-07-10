//! Leader + follower tests: a read-write [`EdgeShard`] (leader) writes to a directory and a
//! [`ReadOnlyEdgeShard`] (follower) opened over the same directory serves reads, converging on the
//! leader's flushed state after a [`refresh`](ReadOnlyEdgeShard::refresh).
#![expect(clippy::wildcard_enum_match_arm, reason = "test code")]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use common::flags::{FeatureFlags, init_feature_flags};
use common::universal_io::{MmapFile, MmapFs};
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use segment::types::{Distance, ExtendedPointId, WithPayloadInterface, WithVector};
use shard::count::CountRequestInternal;
use shard::files::{SEGMENTS_PATH, segment_manifest_path};
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::{DeletePoints, UpsertPoints};
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};
use shard::scroll::ScrollRequestInternal;
use uuid::Uuid;

use crate::config::vectors::EdgeVectorParams;
use crate::read_only::{
    LocalSegmentEnumerator, ManifestSegmentEnumerator, ReadOnlyEdgeShard, SegmentEnumerator,
};
use crate::read_view::EdgeShardRead;
use crate::{EdgeConfig, EdgeShard, scan_segment_dirs};

const VECTOR_NAME: &str = "edge-ro-test-vector";

fn test_config() -> EdgeConfig {
    EdgeConfig {
        on_disk_payload: Some(false),
        vectors: HashMap::from([(
            VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: 1,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )]),
        sparse_vectors: HashMap::new(),
        hnsw_config: None,
        quantization_config: None,
        optimizers: None,
        wal_options: None,
        max_search_threads: None,
    }
}

fn point(id: u64) -> PointStructPersisted {
    PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vec![id as f32]),
        )]))),
        payload: None,
    }
}

fn upsert(shard: &EdgeShard, ids: impl IntoIterator<Item = u64>) {
    let points = ids.into_iter().map(point).collect::<Vec<_>>();
    shard
        .update(PointOperation(UpsertPoints(PointsList(points))))
        .unwrap();
}

fn delete(shard: &EdgeShard, ids: impl IntoIterator<Item = u64>) {
    let ids = ids.into_iter().map(ExtendedPointId::NumId).collect();
    shard.update(PointOperation(DeletePoints { ids })).unwrap();
}

/// Open a follower that discovers segments by scanning the directory (no manifest required) — used
/// by the read/refresh tests, whose leaders don't write a manifest.
fn open_follower(path: &std::path::Path) -> ReadOnlyEdgeShard<MmapFile> {
    ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        path,
        LocalSegmentEnumerator::new(path),
        None,
    )
    .unwrap()
}

fn exact_count(follower: &ReadOnlyEdgeShard<MmapFile>) -> usize {
    follower
        .count(CountRequestInternal {
            filter: None,
            exact: true,
        })
        .unwrap()
}

fn leader_exact_count(leader: &EdgeShard) -> usize {
    leader
        .count(CountRequestInternal {
            filter: None,
            exact: true,
        })
        .unwrap()
}

/// Scrolled point ids (sorted) visible to the follower.
fn scrolled_ids(follower: &ReadOnlyEdgeShard<MmapFile>) -> Vec<ExtendedPointId> {
    let (records, _) = follower
        .scroll(ScrollRequestInternal {
            offset: None,
            limit: Some(10_000),
            filter: None,
            with_payload: Some(WithPayloadInterface::Bool(false)),
            with_vector: WithVector::Bool(false),
            order_by: None,
        })
        .unwrap();
    let mut ids = records.into_iter().map(|r| r.id).collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// Assert each id is retrievable from the follower with its expected vector `[id as f32]`.
fn assert_follower_vectors(follower: &ReadOnlyEdgeShard<MmapFile>, ids: &[u64]) {
    let point_ids = ids
        .iter()
        .map(|id| ExtendedPointId::NumId(*id))
        .collect::<Vec<_>>();
    let results = follower
        .retrieve(
            &point_ids,
            Some(WithPayloadInterface::Bool(false)),
            Some(WithVector::Bool(true)),
        )
        .unwrap();
    assert_eq!(results.len(), ids.len(), "expected all ids retrievable");
    for (result, &expected_id) in results.iter().zip(ids) {
        assert_eq!(result.id, ExtendedPointId::NumId(expected_id));
        let vectors = match result.vector.as_ref().expect("vector present") {
            VectorStructInternal::Named(named) => named,
            other => panic!("expected Named vectors, got {other:?}"),
        };
        let vec = match vectors.get(VECTOR_NAME).expect("vector name exists") {
            VectorInternal::Dense(v) => v,
            other => panic!("expected Dense vector, got {other:?}"),
        };
        assert_eq!(
            vec,
            &vec![expected_id as f32],
            "vector mismatch for {expected_id}"
        );
    }
}

#[test]
fn follower_sees_flushed_data() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-visibility")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=100);
    leader.flush();

    let follower = open_follower(dir.path());
    follower.refresh().unwrap();

    assert_eq!(exact_count(&follower), 100);
    assert_eq!(exact_count(&follower), leader_exact_count(&leader));
    assert_eq!(scrolled_ids(&follower).len(), 100);
    assert_follower_vectors(&follower, &[1, 50, 100]);
}

#[test]
fn refresh_picks_up_incremental_writes() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-incremental")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=50);
    leader.flush();

    let follower = open_follower(dir.path());
    follower.refresh().unwrap();
    assert_eq!(exact_count(&follower), 50);

    // Second batch: appended in place to the same (appendable) segment.
    upsert(&leader, 51..=100);
    leader.flush();
    follower.refresh().unwrap();

    assert_eq!(exact_count(&follower), 100);
    assert_eq!(exact_count(&follower), leader_exact_count(&leader));
    assert_follower_vectors(&follower, &[1, 50, 51, 100]);
}

#[test]
fn follower_reflects_deletes() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-deletes")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=100);
    leader.flush();

    let follower = open_follower(dir.path());
    follower.refresh().unwrap();
    assert_eq!(exact_count(&follower), 100);

    delete(&leader, 1..=40);
    leader.flush();
    follower.refresh().unwrap();

    assert_eq!(exact_count(&follower), 60);
    assert_eq!(exact_count(&follower), leader_exact_count(&leader));
    let ids = scrolled_ids(&follower);
    assert_eq!(ids.len(), 60);
    assert!(!ids.contains(&ExtendedPointId::NumId(1)));
    assert!(ids.contains(&ExtendedPointId::NumId(100)));
}

// On Windows a file/directory cannot be renamed or removed while another handle has it open, so the
// leader's optimization (which deletes the vacuumed segment dir) fails with "Access is denied" while
// the follower holds that segment mmap'd. On Linux/macOS unlinking open files is fine. This is an OS
// characteristic, not a logic difference, so the swap mechanics are exercised on the other platforms.
#[cfg_attr(
    target_os = "windows",
    ignore = "leader can't delete a segment dir held open (mmap) by the follower"
)]
#[test]
fn follower_tracks_optimization_swap() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-optimize")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=1000);
    // Delete enough to make the segment a vacuum candidate (>20%).
    delete(&leader, 1..=300);
    leader.flush();

    let follower = open_follower(dir.path());
    follower.refresh().unwrap();
    assert_eq!(exact_count(&follower), 700);

    // Vacuum rebuilds the segment under a new UUID and removes the old one.
    let optimized = leader.optimize().unwrap();
    assert!(optimized, "expected a vacuum optimization to run");
    leader.flush();
    follower.refresh().unwrap();

    // Follower drops the old UUID, opens the new one, and serves the same data.
    assert_eq!(exact_count(&follower), 700);
    assert_eq!(exact_count(&follower), leader_exact_count(&leader));
    assert_follower_vectors(&follower, &[301, 500, 1000]);
}

#[test]
fn refresh_on_unchanged_dir_is_noop() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-noop")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush();

    let follower = open_follower(dir.path());
    follower.refresh().unwrap();
    let before = exact_count(&follower);

    // Repeated refreshes without leader changes must be stable.
    follower.refresh().unwrap();
    follower.refresh().unwrap();
    assert_eq!(exact_count(&follower), before);
    assert_eq!(before, 10);
}

/// A read-only follower has no `edge_config.json`: it derives its config from the segments. Opening
/// after deleting the config the leader happened to write must still succeed and read the data.
#[test]
fn open_without_config_derives_from_segments() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-noconfig")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush();
    let expected = leader_exact_count(&leader);
    drop(leader);

    // Remove the config the leader wrote: a follower must not depend on it.
    fs_err::remove_file(dir.path().join("edge_config.json")).unwrap();

    let follower = open_follower(dir.path());
    assert_eq!(exact_count(&follower), expected);
    assert_eq!(expected, 10);
}

/// A caller-provided config overrides tunables at open only: `vectors` still derive from the
/// segments (so reads work with a vectors-less provided config), while [`refresh`] re-derives the
/// config from the segments alone.
///
/// [`refresh`]: ReadOnlyEdgeShard::refresh
#[test]
fn provided_config_overrides_tunables_at_open() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-provided-config")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=10);
    leader.flush();

    // Tunables only — no vector params. Those must come from the segments.
    let provided = EdgeConfig {
        on_disk_payload: None,
        vectors: HashMap::new(),
        sparse_vectors: HashMap::new(),
        hnsw_config: None,
        quantization_config: None,
        optimizers: None,
        wal_options: None,
        max_search_threads: Some(2),
    };

    let follower = ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        dir.path(),
        LocalSegmentEnumerator::new(dir.path()),
        Some(provided),
    )
    .unwrap();

    let config = follower.config_snapshot();
    assert!(config.vectors.contains_key(VECTOR_NAME));
    // Unspecified tunable: falls back to the segment-derived value.
    assert_eq!(config.on_disk_payload, Some(false));
    // Explicitly provided tunable: wins over the derived config.
    assert_eq!(config.max_search_threads, Some(2));
    assert_eq!(exact_count(&follower), 10);

    // Refresh re-derives the config from the segments alone: the provided tunables are dropped
    // (segments never carry `max_search_threads`), the segment-derived values remain.
    upsert(&leader, 11..=15);
    leader.flush();
    follower.refresh().unwrap();

    let config = follower.config_snapshot();
    assert!(config.vectors.contains_key(VECTOR_NAME));
    assert_eq!(config.on_disk_payload, Some(false));
    assert_eq!(config.max_search_threads, None);
    assert_eq!(exact_count(&follower), 15);
}

/// A [`SegmentEnumerator`] that scans the local `segments/` directory but hides a chosen UUID,
/// standing in for a non-local enumerator (e.g. S3 / a future manifest) to exercise the injection
/// seam: the follower must track exactly what the enumerator reports.
struct ExcludingEnumerator {
    segments_path: PathBuf,
    exclude: Uuid,
}

impl SegmentEnumerator for ExcludingEnumerator {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>> {
        let mut segments = scan_segment_dirs(&self.segments_path)?;
        segments.remove(&self.exclude);
        Ok(segments)
    }
}

#[test]
fn follower_uses_injected_enumerator() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-enumerator")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=100);
    leader.flush();

    let segments_path = dir.path().join(SEGMENTS_PATH);
    let all_segments = scan_segment_dirs(&segments_path).unwrap();
    assert!(!all_segments.is_empty());
    let hidden = *all_segments.keys().next().unwrap();

    // Baseline: the default (local) enumerator discovers every segment on disk.
    let baseline = open_follower(dir.path());
    assert_eq!(baseline.segments_count(), all_segments.len());

    // Injected enumerator hides one segment: the follower must track exactly what it reports, not
    // whatever happens to be on disk — proving discovery goes through the enumerator.
    let follower = ReadOnlyEdgeShard::<MmapFile>::open_with_enumerator(
        MmapFs,
        dir.path(),
        ExcludingEnumerator {
            segments_path,
            exclude: hidden,
        },
        None,
    )
    .unwrap();
    assert_eq!(follower.segments_count(), all_segments.len() - 1);

    // A refresh still goes through the same enumerator, so the hidden segment stays hidden.
    follower.refresh().unwrap();
    assert_eq!(follower.segments_count(), all_segments.len() - 1);
}

/// `ManifestSegmentEnumerator` requires a manifest (it errors without one, rather than scanning) and
/// returns only the manifest's `active` segments.
#[test]
fn manifest_enumerator_requires_manifest() {
    let dir = tempfile::Builder::new()
        .prefix("edge-ro-manifest")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=100);
    leader.flush();

    let segments_path = dir.path().join(SEGMENTS_PATH);
    let on_disk: HashSet<Uuid> = scan_segment_dirs(&segments_path)
        .unwrap()
        .into_keys()
        .collect();
    assert!(!on_disk.is_empty());

    let enumerator = ManifestSegmentEnumerator::new(MmapFs, dir.path());
    let manifest_path = segment_manifest_path(dir.path());

    // No manifest → error, not a directory scan.
    let _ = fs_err::remove_file(&manifest_path);
    assert!(enumerator.list_segments().is_err());

    // With a manifest, only its `active` segments are returned — here just one of the on-disk dirs.
    let active = *on_disk.iter().next().unwrap();
    fs_err::write(&manifest_path, format!(r#"{{"{active}":"active"}}"#)).unwrap();
    let from_manifest: HashSet<Uuid> = enumerator.list_segments().unwrap().into_keys().collect();
    assert_eq!(from_manifest, HashSet::from([active]));
}

/// With the `write_segment_manifest` flag enabled, the leader `EdgeShard` writes a manifest listing
/// its segments, and a follower opened over the same directory loads through it.
#[test]
fn leader_writes_manifest_and_follower_loads_it() {
    let mut flags = FeatureFlags::default();
    flags.write_segment_manifest = true;
    init_feature_flags(flags);

    // Another test in this process may have initialized flags first; the assertions below only hold
    // with the flag enabled.
    if !common::flags::feature_flags().write_segment_manifest {
        return;
    }

    let dir = tempfile::Builder::new()
        .prefix("edge-ro-manifest-write")
        .tempdir()
        .unwrap();

    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=100);
    leader.flush();

    // The leader wrote a manifest listing every live segment.
    let manifest_path = segment_manifest_path(dir.path());
    assert!(
        manifest_path.exists(),
        "leader should write the segment manifest"
    );
    let on_disk: HashSet<Uuid> = scan_segment_dirs(&dir.path().join(SEGMENTS_PATH))
        .unwrap()
        .into_keys()
        .collect();
    let from_manifest: HashSet<Uuid> = ManifestSegmentEnumerator::new(MmapFs, dir.path())
        .list_segments()
        .unwrap()
        .into_keys()
        .collect();
    assert_eq!(from_manifest, on_disk);

    // The follower discovers and serves the data through the manifest.
    let follower = ReadOnlyEdgeShard::<MmapFile>::open_mmap(dir.path()).unwrap();
    follower.refresh().unwrap();
    assert_eq!(exact_count(&follower), 100);
}
