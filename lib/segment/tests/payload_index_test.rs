#[cfg(test)]
mod tests {
    use rand::prelude::ThreadRng;
    use rand::seq::SliceRandom;
    use segment::types::{PayloadType, VectorElementType, SegmentConfig, Indexes, PayloadIndexType, Distance, StorageType, TheMap, PayloadKeyType, Filter, Condition, FieldCondition, Match, Range as RangeConditionl};
    use rand::Rng;
    use tempdir::TempDir;
    use segment::segment_constructor::segment_constructor::build_segment;
    use segment::entry::entry_point::SegmentEntry;
    use itertools::Itertools;
    use std::ops::Range;

    const ADJECTIVE: &'static [&'static str] = &[
        "jobless",
        "rightful",
        "breakable",
        "impartial",
        "shocking",
        "faded",
        "phobic",
        "overt",
        "like",
        "wide-eyed",
        "broad",
    ];

    const NOUN: &'static [&'static str] = &[
        "territory",
        "jam",
        "neck",
        "chicken",
        "cap",
        "kiss",
        "veil",
        "trail",
        "size",
        "digestion",
        "rod",
        "seed",
    ];

    const INT_RANGE: Range<i64> = 0..500;

    fn random_keyword(rnd_gen: &mut ThreadRng) -> String {
        let random_adj = ADJECTIVE.choose(rnd_gen).unwrap();
        let random_noun = NOUN.choose(rnd_gen).unwrap();
        format!("{} {}", random_adj, random_noun)
    }

    fn random_keyword_payload(rnd_gen: &mut ThreadRng) -> PayloadType {
        PayloadType::Keyword(vec![random_keyword(rnd_gen)])
    }

    fn random_int_payload(rnd_gen: &mut ThreadRng) -> PayloadType {
        let val1: i64 = rnd_gen.gen_range(INT_RANGE);
        let val2: i64 = rnd_gen.gen_range(INT_RANGE);
        PayloadType::Integer(vec![val1, val2])
    }

    fn random_vector(rnd_gen: &mut ThreadRng, size: usize) -> Vec<VectorElementType> {
        (0..size).map(|_| rnd_gen.gen()).collect()
    }

    fn random_field_condition(rnd_gen: &mut ThreadRng) -> Condition {
        let kv_or_int: bool = rnd_gen.gen();
        match kv_or_int {
            true => Condition::Field(FieldCondition {
                key: "kvd".to_string(),
                r#match: Some(Match {
                    keyword: Some(random_keyword(rnd_gen)),
                    integer: None,
                }),
                range: None,
                geo_bounding_box: None,
                geo_radius: None,
            }),
            false => Condition::Field(FieldCondition {
                key: "int".to_string(),
                r#match: None,
                range: Some(RangeConditionl {
                    lt: None,
                    gt: None,
                    gte: Some(rnd_gen.gen_range(INT_RANGE) as f64),
                    lte: Some(rnd_gen.gen_range(INT_RANGE) as f64),
                }),
                geo_bounding_box: None,
                geo_radius: None,
            })
        }
    }

    fn random_filter(rnd_gen: &mut ThreadRng) -> Filter {
        let mut rnd1 = rand::thread_rng();

        let should_conditions = (0..=2)
            .take_while(|_| rnd1.gen::<f64>() > 0.6)
            .map(|_| random_field_condition(rnd_gen))
            .collect_vec();

        let should_conditions_opt = match should_conditions.is_empty() {
            false => Some(should_conditions),
            true => None,
        };

        let must_conditions = (0..=2)
            .take_while(|_| rnd1.gen::<f64>() > 0.6)
            .map(|_| random_field_condition(rnd_gen))
            .collect_vec();

        let must_conditions_opt = match must_conditions.is_empty() {
            false => Some(must_conditions),
            true => None,
        };

        Filter {
            should: should_conditions_opt,
            must: must_conditions_opt,
            must_not: None,
        }
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

        let mut plain_segment = build_segment(dir1.path(), &config).unwrap();
        config.payload_index = Some(PayloadIndexType::Struct);
        let mut struct_segment = build_segment(dir2.path(), &config).unwrap();

        let str_key = "kvd".to_string();
        let int_key = "int".to_string();

        let num_points = 1000;

        let mut opnum = 0;
        for idx in 0..num_points {
            let vector = random_vector(&mut rnd, dim);
            let mut payload: TheMap<PayloadKeyType, PayloadType> = Default::default();
            payload.insert(str_key.clone(), random_keyword_payload(&mut rnd));
            payload.insert(int_key.clone(), random_int_payload(&mut rnd));

            plain_segment.upsert_point(idx, idx, &vector).unwrap();
            struct_segment.upsert_point(idx, idx, &vector).unwrap();

            plain_segment.set_full_payload(idx, idx, payload.clone()).unwrap();
            struct_segment.set_full_payload(idx, idx, payload.clone()).unwrap();

            opnum += 1;
        }

        struct_segment.create_field_index(opnum, &str_key).unwrap();
        struct_segment.create_field_index(opnum, &int_key).unwrap();


        for _i in 0..100 {
            let query_vector = random_vector(&mut rnd, dim);
            let query_filter = random_filter(&mut rnd);

            let plain_result = plain_segment.search(&query_vector, Some(&query_filter), 5, None).unwrap();
            let struct_result = struct_segment.search(&query_vector, Some(&query_filter), 5, None).unwrap();

            let estimation = struct_segment.payload_index.borrow().estimate_cardinality(&query_filter);

            assert!(estimation.min <= estimation.exp);
            assert!(estimation.exp <= estimation.max);
            assert!(estimation.max <= num_points as usize);

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