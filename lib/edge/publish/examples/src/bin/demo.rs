// See lib/edge/python/examples/demo.py for the equivalent Python example.

use std::error::Error;
use std::path::Path;

use examples::{TMP_DIR, load_new_shard};
use qdrant_edge::external::ordered_float::OrderedFloat;
use qdrant_edge::external::serde_json::json;
use qdrant_edge::external::uuid::Uuid;
use qdrant_edge::{
    Condition, CountRequest, CreateIndex, EdgeShard, FacetRequest, FieldCondition,
    FieldIndexOperations, Filter, Match, MatchTextAny, NamedQuery, PayloadFieldSchema,
    PayloadSchemaType, PointId, PointInsertOperations, PointOperations, PointStruct, QueryEnum,
    QueryRequest, Range, ScoringQuery, ScrollRequest, SearchRequest, UpdateOperation, Vector,
    Vectors, WithPayloadInterface, WithVector,
};

fn main() -> Result<(), Box<dyn Error>> {
    println!("---- Point conversions ----");

    let points = vec![
        // Multi-dense vector
        PointStruct::new(
            10u64,
            Vectors::try_from(vec![vec![1.0, 2.0, 3.0], vec![3.0, 4.0, 5.0]])?,
            json!({}),
        ),
        // Named sparse vector
        PointStruct::new(
            11,
            qdrant_edge::Vectors::new_named([(
                "sparse",
                Vector::new_sparse(vec![0, 2], vec![1.0, 3.0]).unwrap(),
            )]),
            json!({}),
        ),
    ];

    for point in &points {
        println!("{point:?}");
    }

    println!("---- Load shard ----");

    let shard = load_new_shard()?;

    println!("---- Upsert ----");

    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                1u64,
                vec![6.0, 9.0, 4.0, 2.0],
                json!({
                    "null": null,
                    "str": "string",
                    "uint": 42,
                    "int": -69,
                    "float": 4.20,
                    "bool": true,
                    "obj": {
                        "null": null,
                        "str": "string",
                        "uint": 42,
                        "int": -69,
                        "float": 4.20,
                        "bool": true,
                        "obj": {},
                        "arr": [],
                    },
                    "arr": [null, "string", 42, -69, 4.20, true, {}, []],
                }),
            )
            .into(),
            PointStruct::new(
                PointId::Uuid("e9408f2b-b917-4af1-ab75-d97ac6b2c047".parse().unwrap()),
                vec![6.0, 9.0, 3.0, -2.0],
                json!({
                    "hello": "world",
                    "price": 199.99,
                }),
            )
            .into(),
            PointStruct::new(
                PointId::Uuid(Uuid::new_v4()),
                vec![1.0, 6.0, 4.0, 2.0],
                json!({
                    "hello": "world",
                    "price": 999.99,
                }),
            )
            .into(),
        ])),
    ))?;

    println!("---- Query ----");

    let result = shard.query(QueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
            query: vec![6.0, 9.0, 4.0, 2.0].into(),
            using: None,
        }))),
        filter: None,
        score_threshold: None,
        limit: 10,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(true),
        with_payload: WithPayloadInterface::Bool(true),
    })?;

    for point in &result {
        println!("{point:?}");
    }

    println!("---- Search ----");

    let points = shard.search(SearchRequest {
        query: QueryEnum::Nearest(NamedQuery {
            query: vec![1.0, 1.0, 1.0, 1.0].into(),
            using: None,
        }),
        filter: None,
        params: None,
        limit: 10,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(WithVector::Bool(true)),
        score_threshold: None,
    })?;

    for point in &points {
        println!("{point:?}");
    }

    println!("---- Search + Filter ----");

    let search_filter = Filter {
        should: None,
        min_should: None,
        must: Some(vec![
            Condition::Field(FieldCondition::new_match(
                "hello".try_into().unwrap(),
                Match::TextAny(MatchTextAny {
                    text_any: "world".to_string(),
                }),
            )),
            Condition::Field(FieldCondition::new_range(
                "price".try_into().unwrap(),
                Range {
                    lt: None,
                    gt: None,
                    gte: Some(OrderedFloat(500.0)),
                    lte: None,
                },
            )),
        ]),
        must_not: None,
    };

    let points = shard.search(SearchRequest {
        query: QueryEnum::Nearest(NamedQuery {
            query: vec![1.0, 1.0, 1.0, 1.0].into(),
            using: None,
        }),
        filter: Some(search_filter),
        params: None,
        limit: 10,
        offset: 0,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(WithVector::Bool(true)),
        score_threshold: None,
    })?;

    for point in &points {
        println!("{point:?}");
    }

    println!("---- Retrieve ----");

    let points = shard.retrieve(
        &[PointId::NumId(1)],
        Some(WithPayloadInterface::Bool(true)),
        Some(WithVector::Bool(true)),
    )?;

    for point in &points {
        println!("{point:?}");
    }

    println!("---- Scroll ----");

    let (scroll_result, mut next_offset) = shard.scroll(ScrollRequest {
        offset: None,
        limit: Some(2),
        filter: None,
        with_payload: None,
        with_vector: WithVector::Bool(false),
        order_by: None,
    })?;
    for point in &scroll_result {
        println!("{point:?}");
    }

    while let Some(offset) = next_offset {
        println!("--- Next scroll (offset = {offset})---");
        let (scroll_result, next) = shard.scroll(ScrollRequest {
            offset: Some(offset),
            limit: Some(2),
            filter: None,
            with_payload: None,
            with_vector: WithVector::Bool(false),
            order_by: None,
        })?;
        next_offset = next;
        for point in &scroll_result {
            println!("{point:?}");
        }
    }

    println!("---- Count ----");

    let count = shard.count(CountRequest {
        filter: None,
        exact: true,
    })?;
    println!("Total points count: {count}");

    println!("---- Facet ----");

    shard.update(UpdateOperation::FieldIndexOperation(
        FieldIndexOperations::CreateIndex(CreateIndex {
            field_name: "hello".try_into().unwrap(),
            field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
        }),
    ))?;

    let response = shard.facet(FacetRequest {
        key: "hello".try_into().unwrap(),
        limit: 10,
        filter: None,
        exact: false,
    })?;

    println!("Facet results ({} hits):", response.hits.len());

    for hit in &response.hits {
        println!("  {:?}: {}", hit.value, hit.count);
    }

    println!("---- Info ----");

    let info = shard.info();
    println!("{info:?}");

    println!("---- Close and reopen shard ----");

    drop(shard);

    let reopened_shard = EdgeShard::load(Path::new(TMP_DIR), None)?;
    println!(
        "Edge shard reopened. Approx Points: {}",
        reopened_shard.info().points_count
    );

    Ok(())
}
