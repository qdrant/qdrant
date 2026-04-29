// Common helper items for the examples.
// See lib/edge/python/examples/common.py for the equivalent Python helpers.

use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use qdrant_edge::external::serde_json::json;
use qdrant_edge::{
    DEFAULT_VECTOR_NAME, Distance, EdgeConfig, EdgeShard, EdgeVectorParams, PointInsertOperations,
    PointOperations, PointStruct, UpdateOperation,
};

pub const DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../data");
pub const TMP_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../data/tmp");

pub fn load_new_shard() -> Result<EdgeShard, Box<dyn Error>> {
    println!("---- Load shard ----");

    // Clear and recreate tmp directory
    if Path::new(TMP_DIR).exists() {
        fs_err::remove_dir_all(TMP_DIR)?;
    }

    fs_err::create_dir_all(TMP_DIR)?;

    // Load Qdrant Edge shard
    let config = EdgeConfig {
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

    Ok(EdgeShard::load(Path::new(TMP_DIR), Some(config))?)
}

pub fn fill_dummy_data(shard: &EdgeShard) -> Result<(), Box<dyn Error>> {
    shard.update(UpdateOperation::PointOperation(
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(vec![
            PointStruct::new(
                1,
                vec![0.05, 0.61, 0.76, 0.74],
                json!({"color": "red", "city": ["Moscow", "Berlin"]}),
            )
            .into(),
            PointStruct::new(
                2,
                vec![0.19, 0.81, 0.75, 0.11],
                json!({"color": "red", "city": "Mexico"}),
            )
            .into(),
            PointStruct::new(
                3,
                vec![0.36, 0.55, 0.47, 0.94],
                json!({"color": "blue", "city": ["Berlin", "Barcelona"]}),
            )
            .into(),
            PointStruct::new(
                4,
                vec![0.12, 0.34, 0.56, 0.78],
                json!({"color": "green", "city": "Lisbon", "rating": 4.5}),
            )
            .into(),
            PointStruct::new(
                5,
                vec![0.88, 0.12, 0.33, 0.44],
                json!({"color": "yellow", "city": ["Paris"], "active": true}),
            )
            .into(),
            PointStruct::new(
                6,
                vec![0.21, 0.22, 0.23, 0.24],
                json!({"color": "blue", "city": "Tokyo", "tags": ["night", "food"]}),
            )
            .into(),
            PointStruct::new(
                7,
                vec![0.99, 0.01, 0.50, 0.50],
                json!({"color": "red", "city": ["New York", "Boston"], "visits": 7}),
            )
            .into(),
            PointStruct::new(
                8,
                vec![0.10, 0.20, 0.30, 0.40],
                json!({"color": "blue", "city": "Seoul", "meta": {"source": "import"}}),
            )
            .into(),
            PointStruct::new(
                9,
                vec![0.45, 0.55, 0.65, 0.75],
                json!({"color": "green", "city": ["Berlin"], "score": 0.92}),
            )
            .into(),
            PointStruct::new(
                10,
                vec![0.01, 0.02, 0.03, 0.04],
                json!({"color": "yellow", "city": null, "featured": false}),
            )
            .into(),
        ])),
    ))?;
    Ok(())
}
