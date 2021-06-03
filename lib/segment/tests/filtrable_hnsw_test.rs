#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use segment::types::{StorageType, Distance, PayloadIndexType, Indexes, SegmentConfig, TheMap, PayloadKeyType, PayloadType, SeqNumberType, PointIdType, Condition, FieldCondition, Filter, Range, SearchParams, HnswConfig};
    use segment::segment_constructor::segment_constructor::build_segment;
    use segment::fixtures::payload_fixtures::{random_vector, random_int_payload};
    use segment::entry::entry_point::SegmentEntry;
    use segment::index::struct_payload_index::StructPayloadIndex;
    use segment::index::index::{PayloadIndex, VectorIndex};
    use segment::index::hnsw_index::hnsw::HNSWIndex;
    use std::sync::Arc;
    use atomic_refcell::AtomicRefCell;
    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    #[test]
    fn test_filterable_hnsw() {
        let dim = 8;
        let m = 8;
        let num_vectors: PointIdType = 5_000;
        let ef = 32;
        let ef_construct = 16;
        let distance = Distance::Cosine;
        let indexing_threshold = 500;
        let num_payload_values = 2;

        let mut rnd = thread_rng();

        let dir = TempDir::new("segment_dir").unwrap();
        let payload_index_dir = TempDir::new("payload_index_dir").unwrap();
        let hnsw_dir = TempDir::new("hnsw_dir").unwrap();

        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance,
        };

        let int_key = "int".to_string();

        let mut segment = build_segment(dir.path(), &config).unwrap();
        for idx in 0..num_vectors {
            let vector = random_vector(&mut rnd, dim);
            let mut payload: TheMap<PayloadKeyType, PayloadType> = Default::default();
            payload.insert(int_key.clone(), random_int_payload(&mut rnd, num_payload_values));

            segment.upsert_point(idx as SeqNumberType, idx, &vector).unwrap();
            segment.set_full_payload(idx as SeqNumberType, idx, payload.clone()).unwrap();
        }
        // let opnum = num_vectors + 1;

        let payload_index = StructPayloadIndex::open(
            segment.condition_checker.clone(),
            segment.vector_storage.clone(),
            segment.payload_storage.clone(),
            segment.id_mapper.clone(),
            payload_index_dir.path(),
        ).unwrap();

        let payload_index_ptr = Arc::new(AtomicRefCell::new(payload_index));

        let hnsw_config = HnswConfig {
            m,
            ef_construct,
            full_scan_threshold: indexing_threshold
        };

        let mut hnsw_index = HNSWIndex::open(
            hnsw_dir.path(),
            segment.condition_checker.clone(),
            segment.vector_storage.clone(),
            payload_index_ptr.clone(),
            hnsw_config
        ).unwrap();

        hnsw_index.build_index().unwrap();

        payload_index_ptr.borrow_mut().set_indexed(&int_key).unwrap();
        let borrowed_payload_index = payload_index_ptr.borrow();
        let blocks = borrowed_payload_index.payload_blocks(indexing_threshold).collect_vec();
        assert_eq!(blocks.len(), num_vectors as usize / indexing_threshold * 2);

        hnsw_index.build_index().unwrap();

        let top = 3;
        let mut hits = 0;
        let attempts = 100;
        for _i in 0..attempts {
            let query = random_vector(&mut rnd, dim);


            let range_size = 40;
            let left_range = rnd.gen_range(0..400);
            let right_range = left_range + range_size;

            let filter = Filter::new_must(Condition::Field(FieldCondition {
                key: int_key.clone(),
                r#match: None,
                range: Some(Range {
                    lt: None,
                    gt: None,
                    gte: Some(left_range as f64),
                    lte: Some(right_range as f64),
                }),
                geo_bounding_box: None,
                geo_radius: None,
            }));

            let filter_query = Some(&filter);
            // let filter_query = None;

            let index_result = hnsw_index.search_with_graph(
                &query,
                filter_query,
                top,
                Some(&SearchParams { hnsw_ef: Some(ef) })
            );

            let plain_result = segment.vector_index.borrow().search(&query, filter_query, top, None);

            if plain_result == index_result {
                hits += 1;
            }
        }
        assert!(attempts - hits < 5);  // Not more than 5% failures
        eprintln!("hits = {:#?} out of {}", hits, attempts);
    }
}