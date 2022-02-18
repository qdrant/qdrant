#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use segment::entry::entry_point::SegmentEntry;
    use segment::fixtures::payload_fixtures::{
        random_filter, random_int_payload, random_keyword_payload, random_vector,
    };
    use segment::payload_storage::schema_storage::SchemaStorage;
    use segment::segment_constructor::build_segment;
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, Indexes, Payload, PayloadIndexType,
        PayloadKeyType, PayloadType, Range, SegmentConfig, StorageType, TheMap, WithPayload,
    };
    use serde_json::json;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn test_cardinality_estimation() {
        let mut rnd = rand::thread_rng();

        let dir1 = TempDir::new("segment1_dir").unwrap();
        let dim = 5;

        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Struct),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let str_key = "kvd";
        let int_key = "int";

        let num_points = 10000;
        let mut struct_segment =
            build_segment(dir1.path(), &config, Arc::new(SchemaStorage::new())).unwrap();

        let mut opnum = 0;
        for n in 0..num_points {
            let idx = n.into();
            let vector = random_vector(&mut rnd, dim);

            let payload: Payload = json!({
                str_key:random_keyword_payload(&mut rnd),
                int_key:random_int_payload(&mut rnd, 2),
            })
            .into();

            struct_segment.upsert_point(opnum, idx, &vector).unwrap();
            struct_segment
                .set_full_payload(opnum, idx, &payload)
                .unwrap();

            opnum += 1;
        }

        struct_segment.create_field_index(opnum, &str_key).unwrap();
        struct_segment.create_field_index(opnum, &int_key).unwrap();

        let filter = Filter::new_must(Condition::Field(FieldCondition {
            key: int_key.to_owned(),
            r#match: None,
            range: Some(Range {
                lt: None,
                gt: None,
                gte: Some(50.),
                lte: Some(100.),
            }),
            geo_bounding_box: None,
            geo_radius: None,
        }));

        let estimation = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&filter);

        let exact = struct_segment
            .vector_storage
            .borrow()
            .iter_ids()
            .filter(|x| struct_segment.condition_checker.check(*x, &filter))
            .collect_vec()
            .len();

        eprintln!("exact = {:#?}", exact);
        eprintln!("estimation = {:#?}", estimation);

        assert!(exact <= estimation.max);
        assert!(exact >= estimation.min);
    }

    #[test]
    fn test_struct_payload_index() {
        // Compare search with plain and struct indexes
        let mut rnd = rand::thread_rng();

        let dir1 = TempDir::new("segment1_dir").unwrap();
        let dir2 = TempDir::new("segment2_dir").unwrap();

        let dim = 5;

        let mut config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut plain_segment =
            build_segment(dir1.path(), &config, Arc::new(SchemaStorage::new())).unwrap();
        config.payload_index = Some(PayloadIndexType::Struct);
        let mut struct_segment =
            build_segment(dir2.path(), &config, Arc::new(SchemaStorage::new())).unwrap();

        let str_key = "kvd";
        let int_key = "int";

        let num_points = 1000;
        let num_int_values = 2;

        let mut opnum = 0;
        for idx in 0..num_points {
            let point_id = idx.into();
            let vector = random_vector(&mut rnd, dim);

            let payload: Payload = json!({
                str_key:random_keyword_payload(&mut rnd),
                int_key:random_int_payload(&mut rnd, num_int_values),
            })
            .into();

            plain_segment.upsert_point(idx, point_id, &vector).unwrap();
            struct_segment.upsert_point(idx, point_id, &vector).unwrap();

            plain_segment
                .set_full_payload(idx, point_id, &payload)
                .unwrap();
            struct_segment
                .set_full_payload(idx, point_id, &payload)
                .unwrap();

            opnum += 1;
        }

        struct_segment.create_field_index(opnum, &str_key).unwrap();
        struct_segment.create_field_index(opnum, &int_key).unwrap();

        let attempts = 100;
        for _i in 0..attempts {
            let query_vector = random_vector(&mut rnd, dim);
            let query_filter = random_filter(&mut rnd);

            let plain_result = plain_segment
                .search(
                    &query_vector,
                    &WithPayload::default(),
                    false,
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();
            let struct_result = struct_segment
                .search(
                    &query_vector,
                    &WithPayload::default(),
                    false,
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();

            let estimation = struct_segment
                .payload_index
                .borrow()
                .estimate_cardinality(&query_filter);

            assert!(estimation.min <= estimation.exp, "{:#?}", estimation);
            assert!(estimation.exp <= estimation.max, "{:#?}", estimation);
            assert!(estimation.max <= num_points as usize, "{:#?}", estimation);

            plain_result
                .iter()
                .zip(struct_result.iter())
                .for_each(|(r1, r2)| {
                    assert_eq!(r1.id, r2.id);
                    assert!((r1.score - r2.score) < 0.0001)
                });
        }
    }
}
