// See lib/edge/python/examples/load-existing.py for the equivalent Python example.

use std::error::Error;
use std::path::Path;

use examples::{load_new_shard, point};
use qdrant_edge::EdgeShard;
use qdrant_edge::segment::data_types::vectors::VectorStructInternal;
use qdrant_edge::segment::types::{ExtendedPointId, WithPayloadInterface, WithVector};
use qdrant_edge::shard::operations::CollectionUpdateOperations::PointOperation;
use qdrant_edge::shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use qdrant_edge::shard::operations::point_ops::PointOperations::UpsertPoints;
use qdrant_edge::shard::scroll::ScrollRequestInternal;
use serde_json::json;
use uuid::Uuid;

const DATA_DIR: &str = "./data/load-existing";

fn main() -> Result<(), Box<dyn Error>> {
    // Create *new* edge shard and upsert some points
    let edge = load_new_shard(DATA_DIR)?;

    edge.update(PointOperation(UpsertPoints(PointsList(vec![
        point(
            1u64,
            VectorStructInternal::Single(vec![6.0, 9.0, 4.0, 2.0]),
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
        ),
        point(
            ExtendedPointId::Uuid("e9408f2b-b917-4af1-ab75-d97ac6b2c047".parse().unwrap()),
            VectorStructInternal::Single(vec![6.0, 9.0, 3.0, -2.0]),
            json!({
                "hello": "world",
                "price": 199.99,
            }),
        ),
        point(
            ExtendedPointId::Uuid(Uuid::new_v4()),
            VectorStructInternal::Single(vec![1.0, 6.0, 4.0, 2.0]),
            json!({
                "hello": "world",
                "price": 999.99,
            }),
        ),
    ]))))?;

    let (expected_points, _) = edge.scroll(ScrollRequestInternal {
        offset: None,
        limit: Some(5),
        filter: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: WithVector::Bool(true),
        order_by: None,
    })?;
    println!("{expected_points:?}");

    // Re-load edge shard from disk, assert points are available
    drop(edge);
    let edge = EdgeShard::load(Path::new(DATA_DIR), None)?;

    let (points, _) = edge.scroll(ScrollRequestInternal {
        offset: None,
        limit: Some(5),
        filter: None,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: WithVector::Bool(true),
        order_by: None,
    })?;
    println!("{points:?}");

    assert_eq!(points, expected_points);

    Ok(())
}
