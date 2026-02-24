use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use qdrant_edge::EdgeShard;
use qdrant_edge::segment::data_types::vectors::{NamedQuery, VectorInternal, VectorStructInternal};
use qdrant_edge::segment::types::{
    Distance, ExtendedPointId, Payload, PayloadStorageType, SegmentConfig, VectorDataConfig,
    VectorStorageType, WithPayloadInterface, WithVector,
};
use qdrant_edge::shard::count::CountRequestInternal;
use qdrant_edge::shard::facet::FacetRequestInternal;
use qdrant_edge::shard::operations::CollectionUpdateOperations::PointOperation;
use qdrant_edge::shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use qdrant_edge::shard::operations::point_ops::PointOperations::UpsertPoints;
use qdrant_edge::shard::operations::point_ops::PointStructPersisted;
use qdrant_edge::shard::query::query_enum::QueryEnum;
use qdrant_edge::shard::query::{ScoringQuery, ShardQueryRequest};
use qdrant_edge::shard::scroll::ScrollRequestInternal;
use serde_json::{Value, json};

const DATA_DIR: &str = "./qdrant-edge-data";
const VECTOR_NAME: &str = "example-vector";

fn main() -> Result<(), Box<dyn Error>> {
    println!("---- Load shard ----");
    let shard = load_new_shard()?;

    println!("---- Upsert ----");
    let points = vec![
        point(
            1,
            vec![6.0, 9.0, 4.0, 2.0],
            json!({"color": "red", "price": 100.0}),
        ),
        point(
            2,
            vec![1.0, 2.0, 3.0, 4.0],
            json!({"color": "blue", "price": 200.0}),
        ),
        point(
            3,
            vec![5.0, 5.0, 5.0, 5.0],
            json!({"color": "green", "price": 500.0}),
        ),
    ];

    shard.update(PointOperation(UpsertPoints(PointsList(points))))?;
    println!("Upserted 3 points");

    println!("---- Query ----");
    let query_vec: VectorInternal = vec![6.0, 9.0, 4.0, 2.0].into();
    let results = shard.query(ShardQueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery {
            query: query_vec,
            using: Some(VECTOR_NAME.to_string()),
        }))),
        filter: None,
        score_threshold: None,
        limit: 10,
        offset: 0,
        params: None,
        with_vector: WithVector::Bool(false),
        with_payload: WithPayloadInterface::Bool(false),
    })?;

    for p in &results {
        println!("ID: {}, Score: {}", p.id, p.score);
    }

    println!("---- Retrieve ----");
    let retrieved = shard.retrieve(
        &[ExtendedPointId::NumId(1)],
        Some(WithPayloadInterface::Bool(false)),
        Some(WithVector::Bool(false)),
    )?;
    for p in &retrieved {
        println!("ID: {}", p.id);
    }

    println!("---- Scroll ----");
    let (records, _) = shard.scroll(ScrollRequestInternal {
        offset: None,
        limit: Some(3),
        filter: None,
        with_payload: Some(WithPayloadInterface::Bool(false)),
        with_vector: WithVector::Bool(false),
        order_by: None,
    })?;
    for r in &records {
        println!("ID: {}", r.id);
    }

    println!("---- Count ----");
    let count = shard.count(CountRequestInternal {
        filter: None,
        exact: true,
    })?;
    println!("Total points: {count}");

    println!("---- Facet (requires payload index) ----");
    // Note: Facet requires a payload index on the field being faceted.
    // In Edge, payload indexes cannot be created directly - you need to:
    // 1. Create the index in a full Qdrant instance
    // 2. Create a shard snapshot
    // 3. Load the snapshot into Edge
    let facet_result = shard.facet(FacetRequestInternal {
        key: "color".try_into().unwrap(),
        limit: 10,
        filter: None,
        exact: false,
    });
    match facet_result {
        Ok(response) => {
            println!("Facet results:");
            for hit in &response.hits {
                println!("  {:?}: {}", hit.value, hit.count);
            }
        }
        Err(e) => {
            // Expected error when no payload index exists
            println!("Facet error (expected without payload index): {e}");
        }
    }

    println!("---- Info ----");
    let info = shard.info();
    println!(
        "Segments: {}, Approx Points: {}",
        info.segments_count, info.points_count
    );

    println!("---- Close and reopen ----");
    drop(shard);
    let reopened = EdgeShard::load(Path::new(DATA_DIR), None)?;
    println!(
        "Edge shard reopened. Approx Points: {}",
        reopened.info().points_count
    );

    Ok(())
}

fn load_new_shard() -> Result<EdgeShard, Box<dyn Error>> {
    if Path::new(DATA_DIR).exists() {
        fs_err::remove_dir_all(DATA_DIR)?;
    }
    fs_err::create_dir_all(DATA_DIR)?;

    let config = SegmentConfig {
        vector_data: {
            let mut m = HashMap::new();
            m.insert(
                VECTOR_NAME.to_string(),
                VectorDataConfig {
                    size: 4,
                    distance: Distance::Dot,
                    storage_type: VectorStorageType::ChunkedMmap,
                    index: Default::default(),
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            );
            m
        },
        sparse_vector_data: HashMap::new(),
        payload_storage_type: PayloadStorageType::Mmap,
    };

    Ok(EdgeShard::load(Path::new(DATA_DIR), Some(config))?)
}

fn point(id: u64, vector: Vec<f32>, payload: Value) -> PointStructPersisted {
    let mut vectors = HashMap::new();
    vectors.insert(VECTOR_NAME.to_string(), VectorInternal::from(vector));
    PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: VectorStructInternal::Named(vectors).into(),
        payload: Some(json_to_payload(payload)),
    }
}

fn json_to_payload(value: Value) -> Payload {
    if let Value::Object(map) = value {
        let mut payload = Payload::default();
        for (k, v) in map {
            payload.0.insert(k, v);
        }
        payload
    } else {
        Payload::default()
    }
}
