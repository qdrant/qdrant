use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::save_on_disk::SaveOnDisk;
use common::types::{DetailsLevel, TelemetryDetail};
use strum::IntoEnumIterator;
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::operations::types::CollectionError;
use crate::shards::local_shard::LocalShard;
use crate::tests::fixtures::*;

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)] // required for creating the synthetic lock test situation
async fn test_shard_telemetry() {
    let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();

    let config = create_collection_config();

    let collection_name = "test".to_string();

    let current_runtime: Handle = Handle::current();

    let payload_index_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema_file = payload_index_schema_dir.path().join("payload-schema.json");
    let payload_index_schema =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let shard = LocalShard::build(
        0,
        collection_name.clone(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema.clone(),
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Validate for all details levels because the implementations are different
    for detail_level in DetailsLevel::iter() {
        let details = TelemetryDetail::new(detail_level, false);

        let telemetry = shard
            .get_telemetry_data(details, Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(telemetry.num_points, Some(0));

        // test that it timeouts if the segment_holder lock can't be acquired
        let write_segment_holder_guard = shard.segments().write();
        let telemetry = shard
            .get_telemetry_data(details, Duration::from_millis(10))
            .await;
        assert!(matches!(telemetry, Err(CollectionError::Timeout { .. })));

        drop(write_segment_holder_guard);
        let telemetry = shard
            .get_telemetry_data(details, Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(telemetry.num_points, Some(0));
    }
}
