#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::atomic::AtomicBool;

    use rand::thread_rng;
    use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
    use segment::entry::entry_point::SegmentEntry;
    use segment::fixtures::payload_fixtures::random_vector;
    use segment::index::hnsw_index::graph_links::GraphLinksRam;
    use segment::index::hnsw_index::hnsw::HNSWIndex;
    use segment::index::VectorIndex;
    use segment::segment_constructor::build_segment;
    use segment::types::{
        Distance, HnswConfig, Indexes, SearchParams, SegmentConfig, SeqNumberType, StorageType,
        VectorDataConfig,
    };
    use segment::vector_storage::ScoredPointOffset;
    use tempfile::Builder;

    fn sames_count(a: &[Vec<ScoredPointOffset>], b: &[Vec<ScoredPointOffset>]) -> usize {
        a[0].iter()
            .map(|x| x.idx)
            .collect::<BTreeSet<_>>()
            .intersection(&b[0].iter().map(|x| x.idx).collect())
            .count()
    }

    #[test]
    fn hnsw_quantized_search_test() {
        let stopped = AtomicBool::new(false);

        let dim = 128;
        let m = 16;
        let num_vectors: u64 = 5_000;
        let ef = 64;
        let ef_construct = 64;
        let distance = Distance::Cosine;

        let mut rnd = thread_rng();

        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();

        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance,
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            payload_storage_type: Default::default(),
        };

        let mut segment = build_segment(dir.path(), &config).unwrap();
        for n in 0..num_vectors {
            let idx = n.into();
            let vector = random_vector(&mut rnd, dim);
            segment
                .upsert_vector(n as SeqNumberType, idx, &only_default_vector(&vector))
                .unwrap();
        }
        segment.vector_data.values_mut().for_each(|vector_storage| {
            vector_storage
                .vector_storage
                .borrow_mut()
                .quantize()
                .unwrap()
        });

        let hnsw_config = HnswConfig {
            m,
            ef_construct,
            full_scan_threshold: usize::MAX,
            max_indexing_threads: 2,
            on_disk: Some(false),
            payload_m: None,
        };

        let mut hnsw_index = HNSWIndex::<GraphLinksRam>::open(
            hnsw_dir.path(),
            segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_storage
                .clone(),
            segment.payload_index.clone(),
            hnsw_config,
        )
        .unwrap();

        hnsw_index.build_index(&stopped).unwrap();

        let top = 10;
        let attempts = 10;
        let mut sames: usize = 0;
        for _i in 0..attempts {
            let query = random_vector(&mut rnd, dim);

            let index_result = hnsw_index.search(
                &[&query],
                None,
                top,
                Some(&SearchParams {
                    hnsw_ef: Some(ef),
                    ..Default::default()
                }),
            );
            let plain_result = segment.vector_data[DEFAULT_VECTOR_NAME]
                .vector_index
                .borrow()
                .search(&[&query], None, top, None);
            sames += sames_count(&index_result, &plain_result);
        }
        let acc = 100.0 * sames as f64 / (attempts * top) as f64;
        println!(
            "sames = {:}, attempts = {:}, top = {:}, acc = {:}",
            sames, attempts, top, acc
        );
        assert!(acc > 40.0);
    }
}
