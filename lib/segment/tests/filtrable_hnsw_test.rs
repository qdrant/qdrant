#[cfg(test)]
mod tests {
    use atomic_refcell::AtomicRefCell;
    use itertools::Itertools;
    use rand::{thread_rng, Rng};
    use segment::entry::entry_point::SegmentEntry;
    use segment::fixtures::payload_fixtures::{random_int_payload, random_vector};
    use segment::index::hnsw_index::hnsw::HNSWIndex;
    use segment::index::struct_payload_index::StructPayloadIndex;
    use segment::index::{PayloadIndex, VectorIndex};
    use segment::segment_constructor::build_segment;
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, HnswConfig, Indexes, Payload,
        PayloadIndexType, PayloadSchemaType, Range, SearchParams, SegmentConfig, SeqNumberType,
        StorageType,
    };
    use segment::vector_storage::storage_points_iterator::StoragePointsIterator;
    use serde_json::json;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn test_filterable_hnsw() {
        let stopped = AtomicBool::new(false);

        let dim = 8;
        let m = 8;
        let num_vectors: u64 = 5_000;
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

        let int_key = "int";

        let mut segment = build_segment(dir.path(), &config).unwrap();
        for n in 0..num_vectors {
            let idx = n.into();
            let vector = random_vector(&mut rnd, dim);

            let payload: Payload =
                json!({int_key:random_int_payload(&mut rnd, num_payload_values..=num_payload_values),}).into();

            segment
                .upsert_point(n as SeqNumberType, idx, &vector)
                .unwrap();
            segment
                .set_full_payload(n as SeqNumberType, idx, &payload)
                .unwrap();
        }
        // let opnum = num_vectors + 1;

        let payload_index = StructPayloadIndex::open(
            Arc::new(AtomicRefCell::new(StoragePointsIterator(
                segment.vector_storage.clone(),
            ))),
            segment.payload_storage.clone(),
            segment.id_tracker.clone(),
            payload_index_dir.path(),
        )
        .unwrap();

        let payload_index_ptr = Arc::new(AtomicRefCell::new(payload_index));

        let hnsw_config = HnswConfig {
            m,
            ef_construct,
            full_scan_threshold: indexing_threshold,
        };

        let mut hnsw_index = HNSWIndex::open(
            hnsw_dir.path(),
            segment.vector_storage.clone(),
            payload_index_ptr.clone(),
            hnsw_config,
        )
        .unwrap();

        hnsw_index.build_index(&stopped).unwrap();

        payload_index_ptr
            .borrow_mut()
            .set_indexed(int_key, PayloadSchemaType::Integer)
            .unwrap();
        let borrowed_payload_index = payload_index_ptr.borrow();
        let blocks = borrowed_payload_index
            .payload_blocks(int_key, indexing_threshold)
            .collect_vec();
        for block in blocks.iter() {
            assert!(
                block.condition.range.is_some(),
                "only range conditions should be generated for this type of payload"
            );
        }

        assert_eq!(blocks.len(), num_vectors as usize / indexing_threshold * 2);

        hnsw_index.build_index(&stopped).unwrap();

        let top = 3;
        let mut hits = 0;
        let attempts = 100;
        for _i in 0..attempts {
            let query = random_vector(&mut rnd, dim);

            let range_size = 40;
            let left_range = rnd.gen_range(0..400);
            let right_range = left_range + range_size;

            let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
                int_key.to_owned(),
                Range {
                    lt: None,
                    gt: None,
                    gte: Some(left_range as f64),
                    lte: Some(right_range as f64),
                },
            )));

            let filter_query = Some(&filter);
            // let filter_query = None;

            let index_result = hnsw_index.search_with_graph(
                &query,
                filter_query,
                top,
                Some(&SearchParams { hnsw_ef: Some(ef) }),
            );

            let plain_result =
                segment
                    .vector_index
                    .borrow()
                    .search(&query, filter_query, top, None);

            if plain_result == index_result {
                hits += 1;
            }
        }
        assert!(attempts - hits < 5, "hits: {} of {}", hits, attempts); // Not more than 5% failures
        eprintln!("hits = {:#?} out of {}", hits, attempts);
    }
}
