use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::cpu::CpuPermit;
use common::types::PointOffsetType;
use rand::thread_rng;
use tempfile::Builder;

use crate::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
use crate::entry::entry_point::SegmentEntry;
use crate::fixtures::index_fixtures::random_vector;
use crate::index::hnsw_index::graph_links::{GraphLinks, GraphLinksRam};
use crate::index::hnsw_index::hnsw::HNSWIndex;
use crate::index::hnsw_index::num_rayon_threads;
use crate::index::VectorIndex;
use crate::segment_constructor::build_segment;
use crate::types::{
    Distance, HnswConfig, Indexes, SegmentConfig, SeqNumberType, VectorDataConfig,
    VectorStorageType,
};

#[test]
fn test_graph_connectivity() {
    let stopped = AtomicBool::new(false);

    let dim = 32;
    let m = 16;
    let num_vectors: u64 = 1_000;
    let ef_construct = 100;
    let distance = Distance::Cosine;
    let full_scan_threshold = 10_000;

    let mut rnd = thread_rng();

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: dim,
                distance,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
                multivec: None,
            },
        )]),
        payload_storage_type: Default::default(),
        sparse_vector_data: Default::default(),
    };

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for n in 0..num_vectors {
        let idx = n.into();
        let vector = random_vector(&mut rnd, dim);

        segment
            .upsert_point(n as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
    }

    let payload_index_ptr = segment.payload_index.clone();

    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold,
        max_indexing_threads: 4,
        on_disk: Some(false),
        payload_m: None,
    };

    let permit_cpu_count = num_rayon_threads(hnsw_config.max_indexing_threads);
    let permit = Arc::new(CpuPermit::dummy(permit_cpu_count as u32));

    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .clone(),
        Default::default(),
        payload_index_ptr.clone(),
        hnsw_config,
    )
    .unwrap();

    hnsw_index.build_index(permit, &stopped).unwrap();

    let graph = hnsw_index.graph().unwrap();

    let mut reverse_links = vec![vec![]; num_vectors as usize];

    for point_id in 0..num_vectors {
        let links = graph.links.links(point_id as PointOffsetType, 0);
        for link in links {
            reverse_links[*link as usize].push(point_id);
        }
    }

    for point_id in 0..num_vectors {
        assert!(
            !reverse_links[point_id as usize].is_empty(),
            "Point {} has no inbound links",
            point_id
        );
    }
}
