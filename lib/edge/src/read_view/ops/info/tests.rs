mod fixtures;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::universal_io::{MmapFile, MmapFs};
use fixtures::{build_fixture, hnsw};
use parking_lot::{RwLock, RwLockReadGuard};
use segment::common::operation_error::OperationError;
use segment::data_types::load_profile::LoadProfile;
use segment::entry::{ReadSegmentEntry, VectorIndexInfo, VectorIndexInfoProvider};
use segment::index::{VectorIndexRead, VectorIndexType};
use segment::segment::read_only::ReadOnlySegment;
use segment::segment_constructor::get_vector_index_path;
use segment::types::Indexes;
use shard::locked_segment::LockedSegment;

use crate::read_only::tests::{VECTOR_NAME, open_follower, test_config, upsert};
use crate::read_view::{EdgeReadView, EdgeShardRead, ReadSegmentHandle, build_segment_pool};
use crate::{EdgeConfig, EdgeShard};

#[test]
fn plain_vectors_remain_plain_with_default_hnsw_config() {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), test_config()).unwrap();
    upsert(&leader, 1..=3);
    leader.flush().unwrap();
    let follower = open_follower(dir.path());

    assert!(follower.config_snapshot().hnsw_config().m > 0);
    assert!(
        follower.config_snapshot().vectors[VECTOR_NAME]
            .hnsw_config
            .is_none()
    );

    let expected = VectorIndexInfo {
        index_type: VectorIndexType::Plain,
        vectors_count: 3,
    };
    for info in [leader.info().unwrap(), follower.info().unwrap()] {
        let indexes = info
            .vector_indexes
            .get(VECTOR_NAME)
            .expect("vector index metadata");
        assert_eq!(
            indexes
                .iter()
                .map(|index| index.vectors_count)
                .sum::<usize>(),
            3
        );
        assert!(
            indexes
                .iter()
                .all(|index| index.index_type == VectorIndexType::Plain)
        );
        assert!(indexes.contains(&expected));
    }
}

#[test]
fn mixed_segments_keep_each_named_vectors_index() {
    let dir = tempfile::tempdir().unwrap();
    let indexed = build_fixture(
        dir.path(),
        &[("dense", hnsw(8, None)), ("other", Indexes::Plain {})],
        4,
    );
    let plain = build_fixture(
        dir.path(),
        &[("dense", Indexes::Plain {}), ("other", hnsw(12, Some(4)))],
        3,
    );
    let config = EdgeConfig::from_segment_config(indexed.config());
    let view = EdgeReadView::new(
        vec![LockedSegment::from(indexed), LockedSegment::from(plain)],
        Arc::new(config),
        build_segment_pool("index-info", 1, None).unwrap(),
    );
    let follower = open_follower(dir.path());
    let folded = follower.config_snapshot();
    assert!(
        folded
            .vectors
            .values()
            .any(|params| params.hnsw_config.is_some())
    );

    for info in [view.info().unwrap(), follower.info().unwrap()] {
        assert_eq!(info.segments_count, 2);
        assert_eq!(info.vector_indexes.len(), 3);
        for (name, expected) in [
            (
                "dense",
                VectorIndexInfo {
                    index_type: VectorIndexType::Hnsw {
                        m: 8,
                        payload_m: None,
                    },
                    vectors_count: 4,
                },
            ),
            (
                "dense",
                VectorIndexInfo {
                    index_type: VectorIndexType::Plain,
                    vectors_count: 3,
                },
            ),
            (
                "other",
                VectorIndexInfo {
                    index_type: VectorIndexType::Plain,
                    vectors_count: 4,
                },
            ),
            (
                "other",
                VectorIndexInfo {
                    index_type: VectorIndexType::Hnsw {
                        m: 12,
                        payload_m: Some(4),
                    },
                    vectors_count: 3,
                },
            ),
            (
                "sparse",
                VectorIndexInfo {
                    index_type: VectorIndexType::Sparse,
                    vectors_count: 4,
                },
            ),
            (
                "sparse",
                VectorIndexInfo {
                    index_type: VectorIndexType::Sparse,
                    vectors_count: 3,
                },
            ),
        ] {
            assert_eq!(info.vector_indexes[name].len(), 2);
            assert!(info.vector_indexes[name].contains(&expected));
        }
    }
}

#[test]
fn hnsw_reports_disabled_and_payload_graph_parameters() {
    for (m, payload_m) in [(0, None), (0, Some(0)), (0, Some(4)), (7, None)] {
        let dir = tempfile::tempdir().unwrap();
        let segment = build_fixture(dir.path(), &[("dense", hnsw(m, payload_m))], 4);
        let expected = VectorIndexInfo {
            index_type: VectorIndexType::Hnsw { m, payload_m },
            vectors_count: 4,
        };
        assert_eq!(segment.vector_index_info()["dense"], expected);
        let follower = open_follower(dir.path());
        assert_eq!(
            follower.info().unwrap().vector_indexes["dense"],
            vec![expected]
        );
    }
}

#[test]
fn runtime_hnsw_parameters_are_independent_of_segment_config() {
    let dir = tempfile::tempdir().unwrap();
    let mut segment = build_fixture(dir.path(), &[("dense", hnsw(8, Some(4)))], 4);
    let mut read_only =
        ReadOnlySegment::<MmapFile>::open(&MmapFs, &segment.segment_path, segment.uuid, None, None)
            .unwrap();
    segment
        .segment_config
        .vector_data
        .get_mut("dense")
        .unwrap()
        .index = Indexes::Plain {};
    read_only
        .segment_config
        .vector_data
        .get_mut("dense")
        .unwrap()
        .index = Indexes::Plain {};
    let expected = VectorIndexInfo {
        index_type: VectorIndexType::Hnsw {
            m: 8,
            payload_m: Some(4),
        },
        vectors_count: 4,
    };
    assert_eq!(segment.vector_index_info()["dense"], expected);
    assert_eq!(read_only.vector_index_info()["dense"], expected);
}

#[test]
fn metadata_does_not_load_a_deferred_hnsw_graph() {
    let dir = tempfile::tempdir().unwrap();
    let segment = build_fixture(dir.path(), &[("dense", hnsw(8, None))], 4);
    let index_path = get_vector_index_path(&segment.segment_path, "dense");
    let graph_config_path = index_path.join("hnsw_config.json");
    let mut graph_config = common::fs::read_json::<serde_json::Value>(&graph_config_path).unwrap();
    graph_config["indexed_vector_count"] = serde_json::Value::Null;
    common::fs::atomic_save_json(&graph_config_path, &graph_config).unwrap();
    let profile = LoadProfile::for_retrieve();
    let read_only = Arc::new(RwLock::new(
        ReadOnlySegment::<MmapFile>::open(
            &MmapFs,
            &segment.segment_path,
            segment.uuid,
            None,
            Some(&profile),
        )
        .unwrap(),
    ));
    assert_eq!(
        read_only.read().vector_data["dense"]
            .vector_index
            .borrow()
            .indexed_vector_count(),
        0
    );
    let view = EdgeReadView::new(
        vec![read_only.clone()],
        Arc::new(EdgeConfig::from_segment_config(segment.config())),
        build_segment_pool("cold-index-info", 1, None).unwrap(),
    );
    assert_eq!(
        view.info().unwrap().vector_indexes["dense"],
        vec![VectorIndexInfo {
            index_type: VectorIndexType::Hnsw {
                m: 8,
                payload_m: None
            },
            vectors_count: 4,
        }]
    );
    assert_eq!(
        read_only.read().vector_data["dense"]
            .vector_index
            .borrow()
            .indexed_vector_count(),
        0
    );
}

#[test]
fn empty_shard_and_empty_vector_storages_remain_distinct() {
    let dir = tempfile::tempdir().unwrap();
    fs_err::create_dir_all(dir.path().join(shard::files::SEGMENTS_PATH)).unwrap();
    let follower = open_follower(dir.path());
    assert!(follower.info().unwrap().vector_indexes.is_empty());
    let segment = build_fixture(dir.path(), &[("dense", Indexes::Plain {})], 0);
    follower.live_reload().unwrap();
    assert_eq!(
        follower.info().unwrap().vector_indexes["dense"],
        vec![VectorIndexInfo {
            index_type: VectorIndexType::Plain,
            vectors_count: 0,
        }]
    );
    drop(segment);
}

#[test]
fn index_metadata_tracks_segment_replacement_on_reload() {
    let dir = tempfile::tempdir().unwrap();
    let plain = build_fixture(dir.path(), &[("dense", Indexes::Plain {})], 4);
    let follower = open_follower(dir.path());
    assert_eq!(
        follower.info().unwrap().vector_indexes["dense"][0].index_type,
        VectorIndexType::Plain
    );
    let indexed = build_fixture(dir.path(), &[("dense", hnsw(8, None))], 4);
    let old_path = plain.segment_path.clone();
    drop(plain);
    fs_err::remove_dir_all(old_path).unwrap();
    follower.live_reload().unwrap();
    assert_eq!(
        follower.info().unwrap().vector_indexes["dense"],
        vec![VectorIndexInfo {
            index_type: VectorIndexType::Hnsw {
                m: 8,
                payload_m: None
            },
            vectors_count: 4,
        }]
    );
    drop(indexed);
}

#[test]
fn cancelled_info_returns_no_index_metadata() {
    use crate::read_view::EdgeShardReadWithCancellation;

    let dir = tempfile::tempdir().unwrap();
    let segment = build_fixture(dir.path(), &[("dense", hnsw(8, None))], 4);
    let follower = open_follower(dir.path());
    let api: &dyn EdgeShardReadWithCancellation = &follower;
    assert!(matches!(
        api.info(Arc::new(AtomicBool::new(true))),
        Err(OperationError::Cancelled { .. })
    ));
    drop(segment);
}

struct CancelOnRead {
    segment: Arc<RwLock<ReadOnlySegment<MmapFile>>>,
    stopped: Arc<AtomicBool>,
}

impl ReadSegmentHandle for CancelOnRead {
    type Segment = ReadOnlySegment<MmapFile>;

    fn read_segment(&self) -> RwLockReadGuard<'_, Self::Segment> {
        let guard = self.segment.read();
        self.stopped.store(true, Ordering::Relaxed);
        guard
    }

    fn segment_arc(&self) -> Arc<RwLock<Self::Segment>> {
        self.segment.clone()
    }
}

#[test]
fn info_cancels_while_reading_the_last_segment() {
    let dir = tempfile::tempdir().unwrap();
    let segment = build_fixture(dir.path(), &[("dense", Indexes::Plain {})], 4);
    let read_only =
        ReadOnlySegment::<MmapFile>::open(&MmapFs, &segment.segment_path, segment.uuid, None, None)
            .unwrap();
    let stopped = Arc::new(AtomicBool::new(false));
    let mut view = EdgeReadView::new(
        vec![CancelOnRead {
            segment: Arc::new(RwLock::new(read_only)),
            stopped: stopped.clone(),
        }],
        Arc::new(EdgeConfig::from_segment_config(segment.config())),
        build_segment_pool("cancel-index-info", 1, None).unwrap(),
    );
    view.is_stopped = stopped;
    assert!(matches!(view.info(), Err(OperationError::Cancelled { .. })));
}
