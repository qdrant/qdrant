mod utils;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use segment::data_types::vectors::{only_default_vector, DEFAULT_VECTOR_NAME};
    use segment::entry::entry_point::SegmentEntry;
    use segment::fixtures::payload_fixtures::{
        generate_diverse_nested_payload, generate_diverse_payload, random_filter,
        random_nested_filter, random_vector, FLICKING_KEY, GEO_KEY, INT_KEY, INT_KEY_2, LAT_RANGE,
        LON_RANGE, STR_KEY, STR_PROJ_KEY, TEXT_KEY,
    };
    use segment::index::PayloadIndex;
    use segment::segment::Segment;
    use segment::segment_constructor::build_segment;
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, GeoPoint, GeoRadius, Indexes,
        IsEmptyCondition, Payload, PayloadField, PayloadSchemaType, Range, SegmentConfig,
        StorageType, VectorDataConfig, WithPayload,
    };
    use tempfile::Builder;

    use crate::utils::scored_point_ties::ScoredPointTies;

    fn build_test_segments(path_struct: &Path, path_plain: &Path) -> (Segment, Segment) {
        let mut rnd = StdRng::seed_from_u64(42);
        let dim = 5;

        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    hnsw_config: None,
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            ..Default::default()
        };

        let mut plain_segment = build_segment(path_plain, &config).unwrap();
        let mut struct_segment = build_segment(path_struct, &config).unwrap();

        let num_points = 3000;
        let points_to_delete = 500;
        let points_to_clear = 500;

        let mut opnum = 0;
        struct_segment
            .create_field_index(opnum, INT_KEY_2, Some(&PayloadSchemaType::Integer.into()))
            .unwrap();

        opnum += 1;
        for n in 0..num_points {
            let idx = n.into();
            let vector = random_vector(&mut rnd, dim);
            let payload: Payload = generate_diverse_payload(&mut rnd);

            plain_segment
                .upsert_vector(opnum, idx, &only_default_vector(&vector))
                .unwrap();
            struct_segment
                .upsert_vector(opnum, idx, &only_default_vector(&vector))
                .unwrap();
            plain_segment
                .set_full_payload(opnum, idx, &payload)
                .unwrap();
            struct_segment
                .set_full_payload(opnum, idx, &payload)
                .unwrap();

            opnum += 1;
        }

        struct_segment
            .create_field_index(opnum, STR_KEY, Some(&PayloadSchemaType::Keyword.into()))
            .unwrap();
        struct_segment
            .create_field_index(opnum, INT_KEY, None)
            .unwrap();
        struct_segment
            .create_field_index(opnum, GEO_KEY, Some(&PayloadSchemaType::Geo.into()))
            .unwrap();
        struct_segment
            .create_field_index(opnum, TEXT_KEY, Some(&PayloadSchemaType::Text.into()))
            .unwrap();
        struct_segment
            .create_field_index(
                opnum,
                FLICKING_KEY,
                Some(&PayloadSchemaType::Integer.into()),
            )
            .unwrap();

        for _ in 0..points_to_clear {
            opnum += 1;
            let idx_to_remove = rnd.gen_range(0..num_points);
            plain_segment
                .clear_payload(opnum, idx_to_remove.into())
                .unwrap();
            struct_segment
                .clear_payload(opnum, idx_to_remove.into())
                .unwrap();
        }

        for _ in 0..points_to_delete {
            opnum += 1;
            let idx_to_remove = rnd.gen_range(0..num_points);
            plain_segment
                .delete_point(opnum, idx_to_remove.into())
                .unwrap();
            struct_segment
                .delete_point(opnum, idx_to_remove.into())
                .unwrap();
        }

        for (field, indexes) in struct_segment.payload_index.borrow().field_indexes.iter() {
            for index in indexes {
                assert!(index.indexed_points() < num_points as usize);
                if field != FLICKING_KEY {
                    assert!(
                        index.indexed_points()
                            > (num_points as usize - points_to_delete - points_to_clear)
                    );
                }
            }
        }

        (struct_segment, plain_segment)
    }

    fn build_test_segments_nested_payload(
        path_struct: &Path,
        path_plain: &Path,
    ) -> (Segment, Segment) {
        let mut rnd = StdRng::seed_from_u64(42);
        let dim = 5;

        let config = SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: dim,
                    distance: Distance::Dot,
                    hnsw_config: None,
                },
            )]),
            index: Indexes::Plain {},
            storage_type: StorageType::InMemory,
            ..Default::default()
        };

        let mut plain_segment = build_segment(path_plain, &config).unwrap();
        let mut struct_segment = build_segment(path_struct, &config).unwrap();

        let num_points = 3000;
        let points_to_delete = 500;
        let points_to_clear = 500;

        // Nested payload keys
        let nested_str_key = format!("{}.{}.{}", STR_KEY, "nested_1", "nested_2");
        let nested_str_proj_key = format!("{}.{}[].{}", STR_PROJ_KEY, "nested_1", "nested_2");

        let mut opnum = 0;
        struct_segment
            .create_field_index(
                opnum,
                &nested_str_key,
                Some(&PayloadSchemaType::Keyword.into()),
            )
            .unwrap();

        struct_segment
            .create_field_index(
                opnum,
                &nested_str_proj_key,
                Some(&PayloadSchemaType::Keyword.into()),
            )
            .unwrap();

        opnum += 1;
        for n in 0..num_points {
            let idx = n.into();
            let vector = random_vector(&mut rnd, dim);
            let payload: Payload = generate_diverse_nested_payload(&mut rnd);

            plain_segment
                .upsert_vector(opnum, idx, &only_default_vector(&vector))
                .unwrap();
            struct_segment
                .upsert_vector(opnum, idx, &only_default_vector(&vector))
                .unwrap();
            plain_segment
                .set_full_payload(opnum, idx, &payload)
                .unwrap();
            struct_segment
                .set_full_payload(opnum, idx, &payload)
                .unwrap();

            opnum += 1;
        }

        for _ in 0..points_to_clear {
            opnum += 1;
            let idx_to_remove = rnd.gen_range(0..num_points);
            plain_segment
                .clear_payload(opnum, idx_to_remove.into())
                .unwrap();
            struct_segment
                .clear_payload(opnum, idx_to_remove.into())
                .unwrap();
        }

        for _ in 0..points_to_delete {
            opnum += 1;
            let idx_to_remove = rnd.gen_range(0..num_points);
            plain_segment
                .delete_point(opnum, idx_to_remove.into())
                .unwrap();
            struct_segment
                .delete_point(opnum, idx_to_remove.into())
                .unwrap();
        }

        for (_field, indexes) in struct_segment.payload_index.borrow().field_indexes.iter() {
            for index in indexes {
                assert!(index.indexed_points() < num_points as usize);
                assert!(
                    index.indexed_points()
                        > (num_points as usize - points_to_delete - points_to_clear)
                );
            }
        }

        (struct_segment, plain_segment)
    }

    #[test]
    fn test_is_empty_conditions() {
        let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
        let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

        let (struct_segment, plain_segment) = build_test_segments(dir1.path(), dir2.path());

        let filter = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "flicking".to_string(),
            },
        }));

        let estimation_struct = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&filter);

        let estimation_plain = plain_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&filter);

        let real_number = plain_segment
            .payload_index
            .borrow()
            .query_points(&filter)
            .count();

        eprintln!("estimation_plain = {estimation_plain:#?}");
        eprintln!("estimation_struct = {estimation_struct:#?}");
        eprintln!("real_number = {real_number:#?}");

        assert!(estimation_plain.max >= real_number);
        assert!(estimation_plain.min <= real_number);

        assert!(estimation_struct.max >= real_number);
        assert!(estimation_struct.min <= real_number);

        assert!(
            (estimation_struct.exp as f64 - real_number as f64).abs()
                <= (estimation_plain.exp as f64 - real_number as f64).abs()
        );
    }

    #[test]
    fn test_cardinality_estimation() {
        let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
        let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

        let (struct_segment, _) = build_test_segments(dir1.path(), dir2.path());

        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            INT_KEY.to_owned(),
            Range {
                lt: None,
                gt: None,
                gte: Some(50.),
                lte: Some(100.),
            },
        )));

        let estimation = struct_segment
            .payload_index
            .borrow()
            .estimate_cardinality(&filter);

        let payload_index = struct_segment.payload_index.borrow();
        let filter_context = payload_index.filter_context(&filter);
        let exact = struct_segment
            .id_tracker
            .borrow()
            .iter_ids()
            .filter(|x| filter_context.check(*x))
            .collect_vec()
            .len();

        eprintln!("exact = {exact:#?}");
        eprintln!("estimation = {estimation:#?}");

        assert!(exact <= estimation.max);
        assert!(exact >= estimation.min);
    }

    #[test]
    fn test_struct_payload_index() {
        // Compare search with plain and struct indexes
        let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
        let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

        let dim = 5;

        let mut rnd = rand::thread_rng();

        let (struct_segment, plain_segment) = build_test_segments(dir1.path(), dir2.path());

        let attempts = 100;
        for _i in 0..attempts {
            let query_vector = random_vector(&mut rnd, dim);
            let query_filter = random_filter(&mut rnd, 3);

            let plain_result = plain_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query_vector,
                    &WithPayload::default(),
                    &false.into(),
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();
            let struct_result = struct_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query_vector,
                    &WithPayload::default(),
                    &false.into(),
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();

            let estimation = struct_segment
                .payload_index
                .borrow()
                .estimate_cardinality(&query_filter);

            assert!(estimation.min <= estimation.exp, "{estimation:#?}");
            assert!(estimation.exp <= estimation.max, "{estimation:#?}");
            assert!(
                estimation.max <= struct_segment.id_tracker.borrow().points_count(),
                "{estimation:#?}"
            );

            // Perform additional sort to break ties by score
            let mut plain_result_sorted_ties: Vec<ScoredPointTies> =
                plain_result.iter().map(|x| x.clone().into()).collect_vec();
            plain_result_sorted_ties.sort();

            let mut struct_result_sorted_ties: Vec<ScoredPointTies> =
                struct_result.iter().map(|x| x.clone().into()).collect_vec();
            struct_result_sorted_ties.sort();

            plain_result_sorted_ties
                .into_iter()
                .zip(struct_result_sorted_ties.into_iter())
                .map(|(r1, r2)| (r1.scored_point, r2.scored_point))
                .for_each(|(r1, r2)| {
                    assert_eq!(r1.id, r2.id, "got different ScoredPoint {r1:?} and {r2:?} for\nquery vector {query_vector:?}\nquery filter {query_filter:?}\nplain result {plain_result:?}\nstruct result{struct_result:?}");
                    assert!((r1.score - r2.score) < 0.0001)
                });
        }
    }

    #[test]
    fn test_struct_payload_geo_index() {
        // Compare search with plain and struct indexes
        let mut rnd = rand::thread_rng();

        let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
        let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

        let dim = 5;

        let (struct_segment, plain_segment) = build_test_segments(dir1.path(), dir2.path());

        let attempts = 100;
        for _i in 0..attempts {
            let query_vector = random_vector(&mut rnd, dim);
            let r_meters = rnd.gen_range(1.0..10000.0);
            let geo_radius = GeoRadius {
                center: GeoPoint {
                    lon: rnd.gen_range(LON_RANGE),
                    lat: rnd.gen_range(LAT_RANGE),
                },
                radius: r_meters,
            };

            let condition = Condition::Field(FieldCondition::new_geo_radius(
                "geo_key".to_string(),
                geo_radius,
            ));

            let query_filter = Filter {
                should: None,
                must: Some(vec![condition]),
                must_not: None,
            };

            let plain_result = plain_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query_vector,
                    &WithPayload::default(),
                    &false.into(),
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();

            let estimation = plain_segment
                .payload_index
                .borrow()
                .estimate_cardinality(&query_filter);

            assert!(estimation.min <= estimation.exp, "{estimation:#?}");
            assert!(estimation.exp <= estimation.max, "{estimation:#?}");
            assert!(
                estimation.max <= struct_segment.id_tracker.borrow().points_count(),
                "{estimation:#?}"
            );

            let struct_result = struct_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query_vector,
                    &WithPayload::default(),
                    &false.into(),
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();

            let estimation = struct_segment
                .payload_index
                .borrow()
                .estimate_cardinality(&query_filter);

            assert!(estimation.min <= estimation.exp, "{estimation:#?}");
            assert!(estimation.exp <= estimation.max, "{estimation:#?}");
            assert!(
                estimation.max <= struct_segment.id_tracker.borrow().points_count(),
                "{estimation:#?}"
            );

            plain_result
                .iter()
                .zip(struct_result.iter())
                .for_each(|(r1, r2)| {
                    assert_eq!(r1.id, r2.id);
                    assert!((r1.score - r2.score) < 0.0001)
                });
        }
    }

    #[test]
    fn test_struct_payload_index_nested_fields() {
        // Compare search with plain and struct indexes
        let dir1 = Builder::new().prefix("segment1_dir").tempdir().unwrap();
        let dir2 = Builder::new().prefix("segment2_dir").tempdir().unwrap();

        let dim = 5;

        let mut rnd = rand::thread_rng();

        let (struct_segment, plain_segment) =
            build_test_segments_nested_payload(dir1.path(), dir2.path());

        let attempts = 100;
        for _i in 0..attempts {
            let query_vector = random_vector(&mut rnd, dim);
            let query_filter = random_nested_filter(&mut rnd);
            let plain_result = plain_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query_vector,
                    &WithPayload {
                        enable: true,
                        payload_selector: None,
                    },
                    &false.into(),
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();
            let struct_result = struct_segment
                .search(
                    DEFAULT_VECTOR_NAME,
                    &query_vector,
                    &WithPayload {
                        enable: true,
                        payload_selector: None,
                    },
                    &false.into(),
                    Some(&query_filter),
                    5,
                    None,
                )
                .unwrap();

            let estimation = struct_segment
                .payload_index
                .borrow()
                .estimate_cardinality(&query_filter);

            assert!(estimation.min <= estimation.exp, "{estimation:#?}");
            assert!(estimation.exp <= estimation.max, "{estimation:#?}");
            assert!(
                estimation.max <= struct_segment.id_tracker.borrow().points_count(),
                "{estimation:#?}"
            );

            // warning: report flakiness at https://github.com/qdrant/qdrant/issues/534
            plain_result
                .iter()
                .zip(struct_result.iter())
                .for_each(|(r1, r2)| {
                    assert_eq!(r1.id, r2.id, "got different ScoredPoint {r1:?} and {r2:?} for\nquery vector {query_vector:?}\nquery filter {query_filter:?}\nplain result {plain_result:?}\nstruct result{struct_result:?}");
                    assert!((r1.score - r2.score) < 0.0001)
                });
        }
    }
}
