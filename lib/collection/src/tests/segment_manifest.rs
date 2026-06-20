use std::sync::Arc;

use common::budget::ResourceBudget;
use common::flags::{FeatureFlags, init_feature_flags};
use common::fs::read_json;
use common::save_on_disk::SaveOnDisk;
use shard::files::segment_manifest_path;
use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::shards::local_shard::LocalShard;
use crate::tests::fixtures::create_collection_config;

/// With the `write_segment_manifest` flag enabled, building a shard writes `segments/manifest.json`
/// listing the shard's segments as `active`.
#[tokio::test]
#[allow(clippy::field_reassign_with_default)]
async fn writes_segment_manifest_when_flag_enabled() {
    let mut flags = FeatureFlags::default();
    flags.write_segment_manifest = true;
    init_feature_flags(flags);

    let collection_dir = Builder::new().prefix("segment-manifest").tempdir().unwrap();
    let payload_schema_dir = Builder::new().prefix("payload-schema").tempdir().unwrap();
    let config = create_collection_config();

    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(payload_schema_dir.path().join("payload-schema.json"))
            .unwrap(),
    );

    let _shard = LocalShard::build(
        0,
        "segment_manifest_test".to_string(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema,
        Handle::current(),
        AdaptiveSearchHandle::current_for_tests(),
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap();

    // Skip when the global flag wasn't actually applied (another test in this process initialized
    // the feature flags first); the assertions below only hold with the flag enabled.
    if !common::flags::feature_flags().write_segment_manifest {
        return;
    }

    let manifest_path = segment_manifest_path(collection_dir.path());
    assert!(
        manifest_path.exists(),
        "manifest.json should be written when the flag is enabled",
    );

    let manifest: SegmentsManifest = read_json(&manifest_path).unwrap();
    assert!(
        !manifest.is_empty(),
        "manifest should list at least the appendable segment",
    );
    for (_uuid, state) in manifest.iter() {
        assert_eq!(*state, SegmentManifestState::Active);
    }
}
