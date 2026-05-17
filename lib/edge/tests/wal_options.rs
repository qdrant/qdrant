use std::collections::HashMap;
use std::num::NonZero;

use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams, WalOptions};

const VECTOR_NAME: &str = "edge-wal-options-test-vector";

fn test_config() -> EdgeConfig {
    EdgeConfig {
        on_disk_payload: false,
        vectors: HashMap::from([(
            VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: 1,
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
    }
}

fn small_wal_options() -> WalOptions {
    WalOptions {
        segment_capacity: 4 * 1024 * 1024,
        segment_queue_len: 0,
        retain_closed: NonZero::new(1).unwrap(),
    }
}

#[test]
fn load_with_wal_options_accepts_custom_capacity() {
    let dir = tempfile::Builder::new()
        .prefix("edge-wal-options-custom")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
    drop(shard);

    let shard = EdgeShard::load_with_wal_options(dir.path(), None, small_wal_options()).unwrap();
    drop(shard);
}

#[test]
fn load_still_works_with_default_wal_options() {
    let dir = tempfile::Builder::new()
        .prefix("edge-wal-options-default")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), test_config()).unwrap();
    drop(shard);

    let shard = EdgeShard::load(dir.path(), None).unwrap();
    drop(shard);
}
