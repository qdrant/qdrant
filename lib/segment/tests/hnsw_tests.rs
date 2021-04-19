mod fixtures;


#[cfg(test)]
mod tests {
    use crate::fixtures::segment::{build_segment_1, build_segment_2};
    use tempdir::TempDir;
    use segment::index::hnsw_index::hnsw::HNSWIndex;

    use std::sync::Arc;
    use atomic_refcell::AtomicRefCell;
    use segment::payload_storage::query_checker::SimpleConditionChecker;
    use segment::types::Indexes;
    use std::cmp::max;


    #[test]
    fn test_hnsw() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment1 = build_segment_1(dir.path());

        let hnsw_path = dir.path().join("hnsw_index");


        let mut hnsw = HNSWIndex::open(
            hnsw_path.as_path(),
            segment1.segment_config.distance,
            segment1.condition_checker,
            segment1.vector_storage.clone(),
            segment1.payload_index.clone(),
            Some(Indexes::Hnsw {
                m: 24,
                ef_construct: 128,
            }),
        ).expect("hnsw index created");

        let mut max_level = 0;
        for i in 0..100 {
            max_level = max(max_level, hnsw.get_random_layer());
        }

        assert!(max_level > 0);


    }
}