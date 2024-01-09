use std::collections::HashMap;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;
use segment::fixtures::payload_context_fixture::FixtureIdTracker;
use segment::index::struct_payload_index::StructPayloadIndex;
use segment::index::PayloadIndex;
use segment::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use segment::payload_storage::PayloadStorage;
use segment::types::{Condition, FieldCondition, Filter, Match, Payload, PayloadSchemaType, Range};
use serde_json::json;
use tempfile::Builder;

const NUM_POINTS: usize = 200;

fn nested_payloads() -> Vec<Payload> {
    let mut res = Vec::new();
    for i in 0..NUM_POINTS {
        let payload: Payload = json!(
                {
                    "arr1": [
                        {"a": 1, "b": i % 10 + 1, "c": i % 2 + 1, "d": i % 3, "text": format!("a1 b{} c{} d{}", i, i % 10 + 1,  i % 3) },
                        {"a": 2, "b": i % 10 + 2, "c": i % 2 + 1, "d": i % 3, "text": format!("a2 b{} c{} d{}", i, i % 10 + 2,  i % 3) },
                        {"a": 3, "b": i % 10 + 3, "c": i % 2 + 2, "d": i % 3, "text": format!("a3 b{} c{} d{}", i, i % 10 + 3,  i % 3) },
                        {"a": 4, "b": i % 10 + 4, "c": i % 2 + 2, "d": i % 3, "text": format!("a4 b{} c{} d{}", i, i % 10 + 4,  i % 3) },
                        {"a": [5, 6], "b": i % 10 + 5, "c": i % 2 + 2, "d": i % 3, "text": format!("a5 b{} c{} d{}", i, i % 10 + 5,  i % 3) },
                    ],
                    "f": i % 10,
                    "arr2": [
                        {
                            "arr3": [
                                { "a": 1, "b": i % 7 + 1 },
                                { "a": 2, "b": i % 7 + 2 },
                            ]
                        },
                        {
                            "arr3": [
                                { "a": 3, "b": i % 7 + 3 },
                                { "a": 4, "b": i % 7 + 4 },
                            ]
                        }
                    ],
                }
            )
            .into();
        res.push(payload);
    }
    res
}

#[test]
fn test_filtering_context_consistency() {
    // let seed = 42;
    // let mut rng = StdRng::seed_from_u64(seed);

    let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

    let mut payload_storage = InMemoryPayloadStorage::default();

    let mut points = HashMap::new();

    for (idx, payload) in nested_payloads().into_iter().enumerate() {
        points.insert(idx, payload.clone());
        payload_storage
            .assign(idx as PointOffsetType, &payload)
            .unwrap();
    }

    let wrapped_payload_storage = Arc::new(AtomicRefCell::new(payload_storage.into()));
    let id_tracker = Arc::new(AtomicRefCell::new(FixtureIdTracker::new(NUM_POINTS)));

    let mut index =
        StructPayloadIndex::open(wrapped_payload_storage, id_tracker, dir.path(), true).unwrap();

    index
        .set_indexed("f", PayloadSchemaType::Integer.into())
        .unwrap();
    index
        .set_indexed("arr1[].a", PayloadSchemaType::Integer.into())
        .unwrap();
    index
        .set_indexed("arr1[].b", PayloadSchemaType::Integer.into())
        .unwrap();
    index
        .set_indexed("arr1[].c", PayloadSchemaType::Integer.into())
        .unwrap();
    index
        .set_indexed("arr1[].d", PayloadSchemaType::Integer.into())
        .unwrap();
    index
        .set_indexed("arr1[].text", PayloadSchemaType::Text.into())
        .unwrap();

    {
        let nested_condition_0 = Condition::new_nested(
            "arr1",
            Filter {
                must: Some(vec![
                    // E.g. idx = 6 => { "a" = 1, "b" = 7, "c" = 1, "d" = 0 }
                    Condition::Field(FieldCondition::new_match("a", 1.into())),
                    Condition::Field(FieldCondition::new_match("c", 1.into())),
                ]),
                should: None,
                min_should: None,
                must_not: Some(vec![Condition::Field(FieldCondition::new_range(
                    "d",
                    Range {
                        lte: Some(1.into()),
                        ..Default::default()
                    },
                ))]),
            },
        );

        let nested_filter_0 = Filter::new_must(nested_condition_0);
        let res0 = index.query_points(&nested_filter_0);

        let filter_context = index.filter_context(&nested_filter_0);

        let check_res0: Vec<_> = (0..NUM_POINTS as PointOffsetType)
            .filter(|point_id| filter_context.check(*point_id as PointOffsetType))
            .collect();

        assert_eq!(res0, check_res0);
        assert!(!res0.is_empty());

        // i % 2 + 1 == 1
        // i % 3 == 2

        // result = 2, 8, 14, ...
        assert!(res0.contains(&2));
        assert!(res0.contains(&8));
        assert!(res0.contains(&14));
    }

    {
        let nested_condition_1 = Condition::new_nested(
            "arr1",
            Filter {
                must: Some(vec![
                    // E.g. idx = 6 => { "a" = 1, "b" = 7, "c" = 1, "d" = 0 }
                    Condition::Field(FieldCondition::new_match("a", 1.into())),
                    Condition::Field(FieldCondition::new_match("c", 1.into())),
                    Condition::Field(FieldCondition::new_match("d", 0.into())),
                ]),
                should: None,
                min_should: None,
                must_not: None,
            },
        );

        let nested_filter_1 = Filter::new_must(nested_condition_1);

        let res1 = index.query_points(&nested_filter_1);

        let filter_context = index.filter_context(&nested_filter_1);

        let check_res1: Vec<_> = (0..NUM_POINTS as PointOffsetType)
            .filter(|point_id| filter_context.check(*point_id as PointOffsetType))
            .collect();

        assert_eq!(res1, check_res1);

        assert!(!res1.is_empty());
        assert!(res1.contains(&6));
    }

    {
        let nested_condition_2 = Condition::new_nested(
            "arr1",
            Filter {
                must: Some(vec![
                    // E.g. idx = 6 => { "a" = 1, "b" = 7, "c" = 1, "d" = 0 }
                    Condition::Field(FieldCondition::new_match("a", 1.into())),
                    Condition::Field(FieldCondition::new_match(
                        "text",
                        Match::Text("c1".to_string().into()),
                    )),
                    Condition::Field(FieldCondition::new_match("d", 0.into())),
                ]),
                should: None,
                min_should: None,
                must_not: None,
            },
        );

        let nested_filter_2 = Filter::new_must(nested_condition_2);

        let res2 = index.query_points(&nested_filter_2);

        let filter_context = index.filter_context(&nested_filter_2);

        let check_res2: Vec<_> = (0..NUM_POINTS as PointOffsetType)
            .filter(|point_id| filter_context.check(*point_id as PointOffsetType))
            .collect();

        assert_eq!(res2, check_res2);

        assert!(!res2.is_empty());
    }

    {
        let nested_condition_3 = Condition::new_nested(
            "arr1",
            Filter {
                must: Some(vec![Condition::Field(FieldCondition::new_match(
                    "b",
                    1.into(),
                ))]),
                should: None,
                min_should: None,
                must_not: None,
            },
        );

        let nester_condition_3_1 = Condition::new_nested(
            "arr2",
            Filter {
                must: Some(vec![Condition::new_nested(
                    "arr3",
                    Filter {
                        must: Some(vec![Condition::Field(FieldCondition::new_match(
                            "b",
                            10.into(),
                        ))]),
                        should: None,
                        min_should: None,
                        must_not: None,
                    },
                )]),
                should: None,
                min_should: None,
                must_not: None,
            },
        );

        let nested_filter_3 = Filter {
            must: Some(vec![nested_condition_3, nester_condition_3_1]),
            should: None,
            min_should: None,
            must_not: None,
        };

        let res3 = index.query_points(&nested_filter_3);

        let filter_context = index.filter_context(&nested_filter_3);

        let check_res3: Vec<_> = (0..NUM_POINTS as PointOffsetType)
            .filter(|point_id| filter_context.check(*point_id as PointOffsetType))
            .collect();

        assert_eq!(res3, check_res3);
        assert!(!res3.is_empty());
    }
}
