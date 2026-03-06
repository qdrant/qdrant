// Common helper items for the examples.
// See lib/edge/python/examples/common.py for the equivalent Python helpers.

use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use qdrant_edge::EdgeShard;
use qdrant_edge::config::shard::EdgeShardConfig;
use qdrant_edge::config::vectors::EdgeVectorParams;
use qdrant_edge::segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorStructInternal};
use qdrant_edge::segment::types::{Distance, ExtendedPointId, Payload};
use qdrant_edge::shard::operations::CollectionUpdateOperations::PointOperation;
use qdrant_edge::shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use qdrant_edge::shard::operations::point_ops::PointOperations::UpsertPoints;
use qdrant_edge::shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};
use serde_json::{Value, json};

pub fn load_new_shard(data_dir: &str) -> Result<EdgeShard, Box<dyn Error>> {
    println!("---- Load shard ----");

    // Clear and recreate data directory
    if Path::new(data_dir).exists() {
        fs_err::remove_dir_all(data_dir)?;
    }

    fs_err::create_dir_all(data_dir)?;

    // Load Qdrant Edge shard
    let config = EdgeShardConfig {
        on_disk_payload: false,
        vectors: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: 4,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )]),
        sparse_vectors: HashMap::new(),
        hnsw_config: Default::default(),
        quantization_config: None,
        optimizers: Default::default(),
    };

    Ok(EdgeShard::load(Path::new(data_dir), Some(config))?)
}

pub fn fill_dummy_data(shard: &EdgeShard) -> Result<(), Box<dyn Error>> {
    shard.update(PointOperation(UpsertPoints(PointsList(vec![
        point(
            1,
            VectorStructInternal::Single(vec![0.05, 0.61, 0.76, 0.74]),
            json!({"color": "red", "city": ["Moscow", "Berlin"]}),
        ),
        point(
            2,
            VectorStructInternal::Single(vec![0.19, 0.81, 0.75, 0.11]),
            json!({"color": "red", "city": "Mexico"}),
        ),
        point(
            3,
            VectorStructInternal::Single(vec![0.36, 0.55, 0.47, 0.94]),
            json!({"color": "blue", "city": ["Berlin", "Barcelona"]}),
        ),
        point(
            4,
            VectorStructInternal::Single(vec![0.12, 0.34, 0.56, 0.78]),
            json!({"color": "green", "city": "Lisbon", "rating": 4.5}),
        ),
        point(
            5,
            VectorStructInternal::Single(vec![0.88, 0.12, 0.33, 0.44]),
            json!({"color": "yellow", "city": ["Paris"], "active": true}),
        ),
        point(
            6,
            VectorStructInternal::Single(vec![0.21, 0.22, 0.23, 0.24]),
            json!({"color": "blue", "city": "Tokyo", "tags": ["night", "food"]}),
        ),
        point(
            7,
            VectorStructInternal::Single(vec![0.99, 0.01, 0.50, 0.50]),
            json!({"color": "red", "city": ["New York", "Boston"], "visits": 7}),
        ),
        point(
            8,
            VectorStructInternal::Single(vec![0.10, 0.20, 0.30, 0.40]),
            json!({"color": "blue", "city": "Seoul", "meta": {"source": "import"}}),
        ),
        point(
            9,
            VectorStructInternal::Single(vec![0.45, 0.55, 0.65, 0.75]),
            json!({"color": "green", "city": ["Berlin"], "score": 0.92}),
        ),
        point(
            10,
            VectorStructInternal::Single(vec![0.01, 0.02, 0.03, 0.04]),
            json!({"color": "yellow", "city": null, "featured": false}),
        ),
    ]))))?;
    Ok(())
}

pub fn point(
    id: impl Into<ExtendedPointId>,
    vector: impl Into<VectorStructInternal>,
    payload: Value,
) -> PointStructPersisted {
    PointStructPersisted {
        id: id.into(),
        vector: VectorStructPersisted::from(vector.into()),
        payload: match payload {
            Value::Object(map) => Some(Payload(map.into_iter().collect())),
            _ => panic!("Payload must be a JSON object"),
        },
    }
}
