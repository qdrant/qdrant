use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;

use collection::collection::Collection;
use collection::config::{CollectionConfig, CollectionParams, WalConfig};
use collection::operations::types::VectorParams;
use segment::types::Distance;

#[path = "../../tests/integration/common/mod.rs"]
mod tests_common;

#[cfg(test)]
#[allow(dead_code)]
pub async fn custom_collection_fixture(storage_path: &Path, vec_dims: usize) -> Collection {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorParams {
            size: NonZeroU64::new(vec_dims as u64).unwrap(),
            distance: Distance::Dot,
            hnsw_config: None,
            quantization_config: None,
            on_disk: None,
        }
        .into(),
        shard_number: NonZeroU32::new(1).expect("Shard number can not be zero"),
        replication_factor: NonZeroU32::new(1).unwrap(),
        write_consistency_factor: NonZeroU32::new(1).unwrap(),
        on_disk_payload: false,
    };

    let collection_config = CollectionConfig {
        params: collection_params,
        optimizer_config: tests_common::TEST_OPTIMIZERS_CONFIG.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
    };

    let snapshot_path = storage_path.join("snapshots");

    tests_common::new_local_collection(
        "test".to_string(),
        storage_path,
        &snapshot_path,
        &collection_config,
    )
    .await
    .unwrap()
}
