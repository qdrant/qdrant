use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::budget::ResourceBudget;
use common::counter::hardware_counter::HardwareCounterCell;
use common::flags::FeatureFlags;
use common::progress_tracker::new_progress_tracker;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorInternal;
use segment::entry::SegmentEntry;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::segment_constructor::segment_builder::SegmentBuilder;
use segment::types::{
    Distance, HnswConfig, HnswGlobalConfig, Indexes, PayloadStorageType, SegmentConfig,
    SparseVectorDataConfig, VectorDataConfig, VectorStorageType,
};
use shard::files::SEGMENTS_PATH;
use sparse::common::sparse_vector::SparseVector;
use uuid::Uuid;

use crate::read_only::tests::init_serverless_feature_flags;

pub(super) fn hnsw(m: usize, payload_m: Option<usize>) -> Indexes {
    Indexes::Hnsw(HnswConfig {
        m,
        payload_m,
        full_scan_threshold: 0,
        max_indexing_threads: 1,
        ..HnswConfig::default()
    })
}

pub(super) fn build_fixture(
    shard_path: &Path,
    indexes: &[(&str, Indexes)],
    points: usize,
) -> Segment {
    init_serverless_feature_flags();
    let segments_path = shard_path.join(SEGMENTS_PATH);
    fs_err::create_dir_all(&segments_path).unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let hw = HardwareCounterCell::disposable();
    let stopped = AtomicBool::new(false);
    let mut config = SegmentConfig {
        vector_data: indexes
            .iter()
            .map(|(name, _)| {
                (
                    name.to_string(),
                    VectorDataConfig {
                        size: 1,
                        distance: Distance::Dot,
                        storage_type: VectorStorageType::InRamChunkedMmap,
                        index: Indexes::Plain {},
                        quantization_config: None,
                        multivector_config: None,
                        datatype: None,
                    },
                )
            })
            .collect::<HashMap<_, _>>(),
        sparse_vector_data: HashMap::from([(
            "sparse".to_string(),
            SparseVectorDataConfig {
                index: SparseIndexConfig::default(),
                storage_type: Default::default(),
                modifier: None,
            },
        )]),
        payload_storage_type: PayloadStorageType::InRamMmap,
        id_tracker_memory: None,
    };
    let (mut source, _) = build_segment(source_dir.path(), &config, None, true).unwrap();
    for id in 1..=points {
        let mut vectors = NamedVectors::default();
        for (name, _) in indexes {
            vectors.insert(name.to_string(), VectorInternal::from(vec![id as f32]));
        }
        vectors.insert(
            "sparse".to_string(),
            VectorInternal::Sparse(SparseVector::new(vec![1], vec![id as f32]).unwrap()),
        );
        source
            .upsert_point(id as u64, (id as u64).into(), vectors, &hw)
            .unwrap();
    }
    for (name, index) in indexes {
        let params = config.vector_data.get_mut(*name).unwrap();
        params.index = index.clone();
        params.storage_type = VectorStorageType::Mmap;
    }
    config
        .sparse_vector_data
        .get_mut("sparse")
        .unwrap()
        .index
        .index_type = SparseIndexType::ImmutableRam;
    let mut builder = SegmentBuilder::new(
        temp_dir.path(),
        &config,
        &HnswGlobalConfig::default(),
        FeatureFlags::default(),
    )
    .unwrap();
    assert!(builder.update(&[&source], &stopped, &hw).unwrap());
    builder
        .build(
            &segments_path,
            Uuid::new_v4(),
            None,
            true,
            ResourceBudget::new(1, 1).try_acquire(1, 1).unwrap(),
            &stopped,
            &mut rand::rng(),
            &hw,
            new_progress_tracker().1,
        )
        .unwrap()
}
