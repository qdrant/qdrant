//! Raft snapshots are exchanged as CBOR between peers that may run different
//! versions during a rolling upgrade, so `quota_config` must be optional in both
//! directions: absent in a snapshot from an older peer, and ignorable by one.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use storage::content_manager::consensus_manager::{CollectionsSnapshot, SnapshotData};
use storage::quota::QuotaConfig;
use storage::types::{PeerAddressById, PeerMetadataById};

/// `SnapshotData` as an older peer defines it — no `quota_config` field.
#[derive(Debug, Serialize, Deserialize)]
struct LegacySnapshotData {
    collections_data: CollectionsSnapshot,
    #[serde(with = "storage::serialize_peer_addresses")]
    address_by_id: PeerAddressById,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata_by_id: PeerMetadataById,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    cluster_metadata: HashMap<String, serde_json::Value>,
}

#[test]
fn old_snapshot_reads_as_no_quota() {
    let legacy = LegacySnapshotData {
        collections_data: CollectionsSnapshot::default(),
        address_by_id: HashMap::new(),
        metadata_by_id: HashMap::new(),
        cluster_metadata: HashMap::new(),
    };
    let bytes = serde_cbor::to_vec(&legacy).unwrap();
    let new: SnapshotData = bytes.as_slice().try_into().unwrap();
    assert_eq!(new.quota_config, None);
}

#[test]
fn new_snapshot_readable_by_old_peer() {
    let new = SnapshotData {
        collections_data: CollectionsSnapshot::default(),
        address_by_id: HashMap::new(),
        metadata_by_id: HashMap::new(),
        cluster_metadata: HashMap::new(),
        quota_config: Some(QuotaConfig {
            enabled: true,
            max_resident_memory_percent: Some(90),
            max_disk_usage_percent: Some(95),
        }),
    };
    let bytes = serde_cbor::to_vec(&new).unwrap();
    let legacy: LegacySnapshotData = serde_cbor::from_slice(&bytes).unwrap();
    assert!(legacy.cluster_metadata.is_empty());
}
