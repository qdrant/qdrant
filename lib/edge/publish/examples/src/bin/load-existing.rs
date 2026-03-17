// See lib/edge/python/examples/load-existing.py for the equivalent Python example.

use std::error::Error;
use std::path::Path;

use examples::{TMP_DIR, load_new_shard};
use qdrant_edge::external::serde_json::json;
use qdrant_edge::external::uuid::Uuid;
use qdrant_edge::{
    EdgeShard, PointId, PointInsertOperations, PointOperations, PointStruct, ScrollRequest,
    UpdateOperation, WithPayloadInterface, WithVector,
};

fn main() -> Result<(), Box<dyn Error>> {
    // Create *new* edge shard and upsert some points
    let edge = load_new_shard()?;

    edge.update(UpdateOperation::PointOperation(
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

    let (expected_points, _) = edge.scroll(ScrollRequest {
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
    let edge = EdgeShard::load(Path::new(TMP_DIR), None)?;

    let (points, _) = edge.scroll(ScrollRequest {
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
