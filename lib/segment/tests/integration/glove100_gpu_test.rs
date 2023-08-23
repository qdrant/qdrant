use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::atomic::AtomicBool;

use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::data_types::vectors::only_default_vector;
use segment::entry::entry_point::SegmentEntry;
use segment::index::VectorIndex;
use segment::index::hnsw_index::gpu::set_gpu_indexing;
use segment::index::hnsw_index::graph_links::GraphLinksRam;
use segment::index::hnsw_index::hnsw::HNSWIndex;
use segment::segment_constructor::build_segment;
use segment::types::HnswConfig;
use segment::types::Indexes;
use segment::types::SegmentConfig;
use segment::types::SeqNumberType;
use segment::types::VectorDataConfig;
use segment::types::VectorStorageType;
use segment::vector_storage::ScoredPointOffset;
use serde::Serialize;
use serde::Deserialize;

#[derive(Serialize, Deserialize)]
struct Data {
    vectors: Vec<Vec<f32>>,
    queries: Vec<Vec<f32>>,
    nearests: Vec<Vec<u32>>,
}

//fn sames_count(a: &[Vec<ScoredPointOffset>], b: &[Vec<ScoredPointOffset>]) -> usize {
//    a[0].iter()
//        .map(|x| x.idx)
//        .collect::<BTreeSet<_>>()
//        .intersection(&b[0].iter().map(|x| x.idx).collect())
//        .count()
//}

#[test]
#[ignore]
fn test_filterable_hnsw() {
    set_gpu_indexing(true);

    let timer = std::time::Instant::now();
    let mut bytes = vec![];
    let mut f = File::open("C://snapshots//data-glove-50.bin").unwrap();
    f.read_to_end(&mut bytes).unwrap();
    let data = bincode::deserialize::<Data>(&bytes).unwrap();
    println!("load data: {:?}", timer.elapsed());

    let stopped = AtomicBool::new(false);
    let dir = tempfile::Builder::new().prefix("segment_dir").tempdir().unwrap();
    let hnsw_dir = tempfile::Builder::new().prefix("hnsw_dir").tempdir().unwrap();

    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: data.vectors[0].len(),
                distance: segment::types::Distance::Cosine,
                storage_type: VectorStorageType::Memory,
                index: Indexes::Plain {},
                quantization_config: None,
            },
        )]),
        payload_storage_type: Default::default(),
    };

    let mut segment = build_segment(dir.path(), &config, true).unwrap();
    for (i, vector) in data.vectors.iter().enumerate() {
        let idx = (i as u64).into();
        segment
            .upsert_point(i as SeqNumberType, idx, only_default_vector(&vector))
            .unwrap();
    }

    let m = 16;
    let ef_construct = 100;
    let hnsw_config = HnswConfig {
        m,
        ef_construct,
        full_scan_threshold: usize::MAX,
        max_indexing_threads: 0,
        on_disk: Some(false),
        payload_m: None,
    };

    let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
        hnsw_dir.path(),
        segment.id_tracker.clone(),
        segment.vector_data[DEFAULT_VECTOR_NAME]
            .vector_storage
            .clone(),
        segment.payload_index.clone(),
        hnsw_config,
    )
    .unwrap();

    println!("Start indexing");
    let timer = std::time::Instant::now();
    hnsw_index.build_index(&stopped).unwrap();
    println!("Index finished in : {:?}", timer.elapsed());
}
