//! Reloading an existing shard with a partially specified config: unspecified (`None`)
//! parameters keep their persisted values, explicitly provided ones overwrite them.

use std::num::NonZero;

use edge::{Distance, EdgeConfig, EdgeOptimizersConfig, EdgeShard, EdgeVectorParams, WalOptions};
use segment::types::HnswConfig;

const VECTOR_NAME: &str = "edge-config-merge-test-vector";

fn custom_hnsw_config() -> HnswConfig {
    HnswConfig {
        m: 24,
        ..HnswConfig::default()
    }
}

fn custom_optimizers_config() -> EdgeOptimizersConfig {
    EdgeOptimizersConfig {
        default_segment_number: Some(3),
        ..EdgeOptimizersConfig::default()
    }
}

fn vector_params() -> EdgeVectorParams {
    EdgeVectorParams {
        size: 1,
        distance: Distance::Dot,
        quantization_config: None,
        multivector_config: None,
        datatype: None,
        on_disk: None,
        hnsw_config: None,
    }
}

fn custom_wal_options() -> WalOptions {
    WalOptions {
        segment_capacity: 4 * 1024 * 1024,
        segment_queue_len: 0,
        retain_closed: NonZero::new(1).unwrap(),
    }
}

fn full_config() -> EdgeConfig {
    EdgeConfig::builder()
        .vector(VECTOR_NAME, vector_params())
        .on_disk_payload(false)
        .hnsw_config(custom_hnsw_config())
        .optimizers(custom_optimizers_config())
        .wal_options(custom_wal_options())
        .max_search_threads(3)
        .build()
}

/// Only vectors specified; everything else left unspecified.
fn vectors_only_config() -> EdgeConfig {
    EdgeConfig::builder()
        .vector(VECTOR_NAME, vector_params())
        .build()
}

#[test]
fn reload_with_unspecified_params_keeps_persisted_values() {
    let dir = tempfile::Builder::new()
        .prefix("edge-config-merge-keep")
        .tempdir()
        .unwrap();

    drop(EdgeShard::new(dir.path(), full_config()).unwrap());

    let shard = EdgeShard::load(dir.path(), Some(vectors_only_config())).unwrap();
    let config = shard.config().clone();
    assert_eq!(config.on_disk_payload, Some(false));
    assert_eq!(config.hnsw_config, Some(custom_hnsw_config()));
    assert_eq!(config.optimizers, Some(custom_optimizers_config()));
    assert_eq!(config.wal_options, Some(custom_wal_options()));
    assert_eq!(config.max_search_threads, Some(3));
    drop(shard);

    // The merged config is persisted: a plain reload sees the same values.
    let shard = EdgeShard::load(dir.path(), None).unwrap();
    let config = shard.config().clone();
    assert_eq!(config.on_disk_payload, Some(false));
    assert_eq!(config.hnsw_config, Some(custom_hnsw_config()));
    assert_eq!(config.optimizers, Some(custom_optimizers_config()));
    assert_eq!(config.wal_options, Some(custom_wal_options()));
    assert_eq!(config.max_search_threads, Some(3));
}

#[test]
fn reload_with_explicit_params_overwrites_persisted_values() {
    let dir = tempfile::Builder::new()
        .prefix("edge-config-merge-overwrite")
        .tempdir()
        .unwrap();

    drop(EdgeShard::new(dir.path(), full_config()).unwrap());

    let new_hnsw_config = HnswConfig {
        m: 48,
        ..HnswConfig::default()
    };
    let provided = EdgeConfig::builder()
        .vector(VECTOR_NAME, vector_params())
        .hnsw_config(new_hnsw_config)
        .build();

    let shard = EdgeShard::load(dir.path(), Some(provided)).unwrap();
    let config = shard.config().clone();
    // Explicitly provided: overwritten.
    assert_eq!(config.hnsw_config, Some(new_hnsw_config));
    // Unspecified: kept from the persisted config.
    assert_eq!(config.on_disk_payload, Some(false));
    assert_eq!(config.optimizers, Some(custom_optimizers_config()));
    assert_eq!(config.wal_options, Some(custom_wal_options()));
    assert_eq!(config.max_search_threads, Some(3));
}

/// Reloading an existing shard with a tunables-only config (no vector params at all): the vector
/// definitions are inherited from the persisted config instead of failing the compatibility
/// check, while the provided tunables still apply.
#[test]
fn reload_with_tunables_only_config_inherits_vectors() {
    let dir = tempfile::Builder::new()
        .prefix("edge-config-merge-no-vectors")
        .tempdir()
        .unwrap();

    drop(EdgeShard::new(dir.path(), full_config()).unwrap());

    let provided = EdgeConfig::builder().max_search_threads(2).build();
    let shard = EdgeShard::load(dir.path(), Some(provided)).unwrap();
    let config = shard.config().clone();
    assert!(config.vectors.contains_key(VECTOR_NAME));
    assert_eq!(config.max_search_threads, Some(2));
    assert_eq!(config.hnsw_config, Some(custom_hnsw_config()));
}

/// With no persisted config either, the vector definitions come from the segments themselves —
/// the last layer of the provided → persisted → derived-from-segments → default chain.
#[test]
fn reload_without_persisted_config_derives_vectors_from_segments() {
    let dir = tempfile::Builder::new()
        .prefix("edge-config-merge-derived")
        .tempdir()
        .unwrap();

    drop(EdgeShard::new(dir.path(), full_config()).unwrap());
    fs_err::remove_file(dir.path().join("edge_config.json")).unwrap();

    let provided = EdgeConfig::builder().max_search_threads(2).build();
    let shard = EdgeShard::load(dir.path(), Some(provided)).unwrap();
    let config = shard.config().clone();
    assert!(config.vectors.contains_key(VECTOR_NAME));
    assert_eq!(config.max_search_threads, Some(2));
    // Derived from the segments: the shard was created with in-RAM payload storage.
    assert_eq!(config.on_disk_payload, Some(false));
}

/// Provided vector params that don't match the stored data (here: a different vector size) must
/// fail the load instead of silently reconfiguring the shard.
#[test]
fn reload_with_incompatible_vectors_fails() {
    let dir = tempfile::Builder::new()
        .prefix("edge-config-merge-incompatible")
        .tempdir()
        .unwrap();

    drop(EdgeShard::new(dir.path(), full_config()).unwrap());

    let incompatible = EdgeConfig::builder()
        .vector(
            VECTOR_NAME,
            EdgeVectorParams {
                size: 2,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )
        .build();
    let err = EdgeShard::load(dir.path(), Some(incompatible)).unwrap_err();
    assert!(err.to_string().contains("incompatible"), "{err}");
}

#[test]
fn unspecified_params_resolve_to_defaults_on_new_shard() {
    let dir = tempfile::Builder::new()
        .prefix("edge-config-merge-defaults")
        .tempdir()
        .unwrap();

    let shard = EdgeShard::new(dir.path(), vectors_only_config()).unwrap();
    let config = shard.config().clone();
    assert_eq!(config.on_disk_payload, None);
    assert!(config.on_disk_payload());
    assert_eq!(config.hnsw_config(), HnswConfig::default());
    assert_eq!(config.optimizers(), EdgeOptimizersConfig::default());
}
