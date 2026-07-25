//! Write a small edge shard to a directory, for serving to the wasm build over HTTP.
//!
//! ```sh
//! cargo run -p edge-wasm --example make_fixture -- /tmp/edge-fixture
//! ```
//!
//! The layout it produces is exactly what a leader writes, so uploading the directory to a bucket
//! under some prefix gives the browser demo something real to open.

use std::collections::HashMap;
use std::path::PathBuf;

use common::flags::{FeatureFlags, init_feature_flags};
use edge::{
    Distance, EdgeConfig, EdgeShard, EdgeVectorParams, PointId, PointInsertOperations,
    PointOperations, UpdateOperation,
};
use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

const VECTOR_NAME: &str = "demo";
const DIM: usize = 4;
const POINTS: u64 = 256;

fn vector_for(id: u64) -> Vec<f32> {
    let base = id as f32;
    (0..DIM).map(|i| base + i as f32 * 0.01).collect()
}

fn main() {
    let target: PathBuf = std::env::args()
        .nth(1)
        .expect("usage: make_fixture <output-dir>")
        .into();

    let mut flags = FeatureFlags::default();
    flags.write_segment_manifest = true;
    init_feature_flags(flags);

    if target.exists() {
        std::fs::remove_dir_all(&target).unwrap();
    }
    std::fs::create_dir_all(&target).unwrap();

    let config = EdgeConfig {
        on_disk_payload: Some(false),
        vectors: HashMap::from([(
            VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: DIM,
                distance: Distance::Euclid,
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
        wal_options: None,
        max_search_threads: Some(1),
    };

    let points = (1..=POINTS)
        .map(|id| PointStructPersisted {
            id: PointId::NumId(id),
            vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
                VECTOR_NAME.to_string(),
                VectorInternal::from(vector_for(id)),
            )]))),
            payload: Some(
                serde_json::from_value(serde_json::json!({
                    "parity": if id % 2 == 0 { "even" } else { "odd" },
                }))
                .unwrap(),
            ),
        })
        .collect();

    let shard = EdgeShard::new(&target, config).unwrap();
    shard
        .update(UpdateOperation::PointOperation(
            PointOperations::UpsertPoints(PointInsertOperations::PointsList(points)),
        ))
        .unwrap();
    shard.flush().unwrap();
    drop(shard);

    println!(
        "wrote {POINTS} points ({DIM}-dim, vector name {VECTOR_NAME:?}) to {}",
        target.display()
    );
}
