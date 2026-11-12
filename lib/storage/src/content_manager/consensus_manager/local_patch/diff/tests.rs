//! Apply `ConsensusStateDiff` to in-memory consensus state, without storage

use std::collections::{HashMap, HashSet};

use collection::collection_state::ShardInfo;
use collection::config::ShardingMethod;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::types::PeerMetadata;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardState;
use collection::shards::shard::PeerId;
use segment::types::ShardKey;
use serde_json::{Value, json};

use super::*;
use crate::content_manager::consensus_state_machine::tests::{PEER_ID, collection_state};

const COLLECTION: &str = "books";
const OTHER_COLLECTION: &str = "films";
const ALIAS: &str = "novels";
const OTHER_PEER_ID: PeerId = PEER_ID + 1;
const UNKNOWN_PEER_ID: PeerId = PEER_ID + 2;

#[test]
fn unknown_fields_are_rejected() {
    let top_level = json!({ "collection": {} });
    let nested = json!({
        "collections": { COLLECTION: { "update": { "shards": { "0": { "replica": {} } } } } },
    });

    for diff in [top_level, nested] {
        let result = serde_json::from_value::<ConsensusStateDiff>(diff.clone());
        assert!(result.is_err(), "diff {diff} must be rejected");
    }
}

#[test]
fn resharding_distinguishes_absent_and_null() {
    let absent: CollectionStateDiff = serde_json::from_value(json!({})).unwrap();
    let null: CollectionStateDiff = serde_json::from_value(json!({ "resharding": null })).unwrap();

    assert!(absent.resharding.is_none());
    assert!(matches!(null.resharding, Some(None)));
}

#[test]
fn replica_state_is_set_and_removed() {
    let mut state = state();

    let report = apply(
        &mut state,
        json!({
            "collections": {
                COLLECTION: {
                    "update": {
                        "shards": {
                            "0": { "replicas": { PEER_ID.to_string(): "Dead", OTHER_PEER_ID.to_string(): null } },
                        },
                    },
                },
            },
        }),
    )
    .unwrap();

    let replicas = &state.collections_data.collections[COLLECTION].shards[&0].replicas;
    assert_eq!(replicas, &HashMap::from([(PEER_ID, ReplicaState::Dead)]));

    assert_eq!(
        changes(&report),
        [
            (
                format!("collections.{COLLECTION}.shards.0.replicas.{PEER_ID}"),
                Some(json!("Active")),
                Some(json!("Dead")),
            ),
            (
                format!("collections.{COLLECTION}.shards.0.replicas.{OTHER_PEER_ID}"),
                Some(json!("Active")),
                None,
            ),
        ],
    );
}

#[test]
fn applying_same_diff_twice_reports_no_changes() {
    let mut state = state();

    let diff = json!({
        "collections": {
            COLLECTION: { "update": { "shards": { "0": { "replicas": { PEER_ID.to_string(): "Dead" } } } } },
        },
        "aliases": { ALIAS: null },
        "cluster_metadata": { "owner": "support" },
    });

    let first = apply(&mut state, diff.clone()).unwrap();
    let applied_state = serde_json::to_value(&state).unwrap();
    let second = apply(&mut state, diff).unwrap();

    assert_eq!(first.changes.len(), 3);
    assert!(second.changes.is_empty(), "{second:?}");
    assert_eq!(serde_json::to_value(&state).unwrap(), applied_state);
}

#[test]
fn null_removes_map_entry() {
    let mut state = state();

    let report = apply(&mut state, json!({ "cluster_metadata": { "owner": null } })).unwrap();

    assert!(state.cluster_metadata.is_empty());
    assert_eq!(
        changes(&report),
        [(
            "cluster_metadata.owner".to_string(),
            Some(json!("qdrant")),
            None
        )],
    );
}

#[test]
fn missing_collection_is_rejected() {
    let update = json!({ "collections": { OTHER_COLLECTION: { "update": {} } } });
    let delete = json!({ "collections": { OTHER_COLLECTION: "delete" } });

    for diff in [update, delete] {
        let error = rejected(&mut state(), diff);
        assert!(error.contains("doesn't exist"), "{error}");
    }
}

#[test]
fn collection_delete_removes_aliases() {
    let mut state = state();

    let report = apply(
        &mut state,
        json!({ "collections": { COLLECTION: "delete" } }),
    )
    .unwrap();

    assert!(state.collections_data.collections.is_empty());
    assert_eq!(state.collections_data.aliases.get(ALIAS), None);

    let paths: Vec<_> = report
        .changes
        .iter()
        .map(|change| change.path.as_str())
        .collect();
    assert_eq!(
        paths,
        [
            format!("collections.{COLLECTION}"),
            format!("aliases.{ALIAS}")
        ],
    );
}

#[test]
fn collection_uuid_change_is_rejected() {
    let mut config = state().collections_data.collections[COLLECTION]
        .config
        .clone();
    config.uuid = Some(uuid::Uuid::new_v4());

    let error = rejected(
        &mut state(),
        json!({ "collections": { COLLECTION: { "update": { "config": config } } } }),
    );

    assert!(error.contains("UUID"), "{error}");
}

#[test]
fn incompatible_collection_params_are_rejected() {
    let mut config = state().collections_data.collections[COLLECTION]
        .config
        .clone();
    config.params.sharding_method = Some(collection::config::ShardingMethod::Custom);

    let error = rejected(
        &mut state(),
        json!({ "collections": { COLLECTION: { "update": { "config": config } } } }),
    );

    assert!(error.contains("incompatible"), "{error}");
}

#[test]
fn compatible_collection_config_is_replaced() {
    let mut state = state();

    let mut config = state.collections_data.collections[COLLECTION]
        .config
        .clone();
    config.params.replication_factor = 3.try_into().unwrap();

    let report = apply(
        &mut state,
        json!({ "collections": { COLLECTION: { "update": { "config": config } } } }),
    )
    .unwrap();

    assert_eq!(
        state.collections_data.collections[COLLECTION].config,
        config
    );

    let paths: Vec<_> = report
        .changes
        .iter()
        .map(|change| change.path.as_str())
        .collect();
    assert_eq!(paths, [format!("collections.{COLLECTION}.config")]);
}

#[test]
fn collection_creation_time_is_ignored() {
    let mut state = state();
    let original_config = state.collections_data.collections[COLLECTION]
        .config
        .clone();

    let mut config = original_config.clone();
    config.created_at = Some(chrono::Utc::now());

    let report = apply(
        &mut state,
        json!({ "collections": { COLLECTION: { "update": { "config": config } } } }),
    )
    .unwrap();

    assert!(report.changes.is_empty(), "{report:?}");
    assert_eq!(
        state.collections_data.collections[COLLECTION].config,
        original_config,
    );

    // Creation time is kept, if other config changes are applied
    config.params.replication_factor = 3.try_into().unwrap();

    apply(
        &mut state,
        json!({ "collections": { COLLECTION: { "update": { "config": config } } } }),
    )
    .unwrap();

    let patched_config = &state.collections_data.collections[COLLECTION].config;
    assert_eq!(patched_config.params, config.params);
    assert_eq!(patched_config.created_at, original_config.created_at);
}

#[test]
fn alias_to_missing_collection_is_rejected() {
    let error = rejected(
        &mut state(),
        json!({ "aliases": { "films_alias": OTHER_COLLECTION } }),
    );

    assert!(error.contains("doesn't exist"), "{error}");
}

#[test]
fn alias_with_collection_name_is_rejected() {
    let mut state = state();
    state
        .collections_data
        .collections
        .insert(OTHER_COLLECTION.to_string(), collection_state(Vec::new()));

    let error = rejected(
        &mut state,
        json!({ "aliases": { OTHER_COLLECTION: COLLECTION } }),
    );

    assert!(error.contains("same name"), "{error}");
}

#[test]
fn peer_address_is_changed() {
    let mut state = state();

    let report = apply(
        &mut state,
        json!({ "peer_addresses": { PEER_ID.to_string(): "http://peer-1-new:6335" } }),
    )
    .unwrap();

    assert_eq!(
        state.address_by_id[&PEER_ID].to_string(),
        "http://peer-1-new:6335/"
    );
    assert_eq!(
        changes(&report),
        [(
            format!("peer_addresses.{PEER_ID}"),
            Some(json!("http://peer-1:6335/")),
            Some(json!("http://peer-1-new:6335/")),
        )],
    );
}

#[test]
fn invalid_peer_addresses_are_rejected() {
    let unknown_peer =
        json!({ "peer_addresses": { UNKNOWN_PEER_ID.to_string(): "http://peer-3:6335" } });
    let path_only = json!({ "peer_addresses": { PEER_ID.to_string(): "/peer" } });

    for (diff, expected) in [(unknown_peer, "doesn't exist"), (path_only, "scheme")] {
        let error = rejected(&mut state(), diff);
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn metadata_of_unknown_peer_is_rejected() {
    let metadata = serde_json::to_value(PeerMetadata::current()).unwrap();

    let error = rejected(
        &mut state(),
        json!({ "peer_metadata": { UNKNOWN_PEER_ID.to_string(): metadata } }),
    );

    assert!(error.contains("doesn't exist"), "{error}");
}

#[test]
fn orphan_peer_metadata_removal_is_reported() {
    let mut state = state();
    state
        .metadata_by_id
        .insert(UNKNOWN_PEER_ID, PeerMetadata::current());

    let report = apply(&mut state, json!({})).unwrap();

    assert!(!state.metadata_by_id.contains_key(&UNKNOWN_PEER_ID));

    let paths: Vec<_> = report
        .changes
        .iter()
        .map(|change| change.path.as_str())
        .collect();
    assert_eq!(paths, [format!("peer_metadata.{UNKNOWN_PEER_ID}")]);
}

#[test]
fn replica_on_unknown_peer() {
    // Adding a replica on an unknown peer is rejected
    let error = rejected(
        &mut state(),
        json!({
            "collections": {
                COLLECTION: { "update": { "shards": { "0": { "replicas": { UNKNOWN_PEER_ID.to_string(): "Active" } } } } },
            },
        }),
    );
    assert!(error.contains("doesn't exist"), "{error}");

    // Existing replica of an unknown peer can still be changed
    let mut state = state();
    state
        .collections_data
        .collections
        .get_mut(COLLECTION)
        .unwrap()
        .shards
        .get_mut(&0)
        .unwrap()
        .replicas
        .insert(UNKNOWN_PEER_ID, ReplicaState::Active);

    apply(
        &mut state,
        json!({
            "collections": {
                COLLECTION: { "update": { "shards": { "0": { "replicas": { UNKNOWN_PEER_ID.to_string(): "Dead" } } } } },
            },
        }),
    )
    .unwrap();
}

#[test]
fn new_shard_is_reported() {
    let mut state = state();

    let report = apply(
        &mut state,
        json!({
            "collections": {
                COLLECTION: { "update": { "shards": { "1": { "replicas": { PEER_ID.to_string(): "Active" } } } } },
            },
        }),
    )
    .unwrap();

    let paths: Vec<_> = report
        .changes
        .iter()
        .map(|change| change.path.as_str())
        .collect();
    assert_eq!(
        paths,
        [
            format!("collections.{COLLECTION}.shards.1"),
            format!("collections.{COLLECTION}.shards.1.replicas.{PEER_ID}"),
        ],
    );
}

#[test]
fn new_shard_without_replicas_is_rejected() {
    let error = rejected(
        &mut state(),
        json!({ "collections": { COLLECTION: { "update": { "shards": { "1": { "replicas": {} } } } } } }),
    );

    assert!(error.contains("must have replicas"), "{error}");
}

#[test]
fn shard_key_mapping_to_missing_shard_is_rejected() {
    let missing_shard = json!({
        "shards_key_mapping": [{ "key": "a", "shard_ids": [0] }, { "key": "b", "shard_ids": [1] }],
    });
    let removed_shard = json!({ "shards": { "0": null } });

    for diff in [missing_shard, removed_shard] {
        let error = rejected(
            &mut custom_sharding_state(),
            json!({ "collections": { COLLECTION: { "update": diff } } }),
        );
        assert!(error.contains("doesn't exist"), "{error}");
    }
}

#[test]
fn shard_with_many_shard_keys_is_rejected() {
    let error = rejected(
        &mut custom_sharding_state(),
        json!({
            "collections": {
                COLLECTION: {
                    "update": {
                        "shards_key_mapping": [{ "key": "a", "shard_ids": [0] }, { "key": "b", "shard_ids": [0] }],
                    },
                },
            },
        }),
    );

    assert!(error.contains("more than one shard key"), "{error}");
}

#[test]
fn shard_without_shard_key_is_rejected() {
    let error = rejected(
        &mut custom_sharding_state(),
        json!({
            "collections": {
                COLLECTION: { "update": { "shards": { "1": { "replicas": { PEER_ID.to_string(): "Active" } } } } },
            },
        }),
    );

    assert!(error.contains("has no shard key"), "{error}");
}

#[test]
fn shard_key_with_auto_sharding_is_rejected() {
    let error = rejected(
        &mut state(),
        json!({
            "collections": {
                COLLECTION: { "update": { "shards_key_mapping": [{ "key": "a", "shard_ids": [0] }] } },
            },
        }),
    );

    assert!(error.contains("auto sharding"), "{error}");
}

#[test]
fn resharding_without_hash_ring_is_rejected() {
    let resharding = |shard_key: Option<&str>| {
        ReshardState::new(
            uuid::Uuid::new_v4(),
            ReshardingDirection::Up,
            PEER_ID,
            1,
            shard_key.map(|key| ShardKey::from(key.to_string())),
        )
    };

    // Shard key with auto sharding, or unknown shard key with custom sharding
    let cases = [
        (state(), resharding(Some("a"))),
        (custom_sharding_state(), resharding(Some("b"))),
        (custom_sharding_state(), resharding(None)),
    ];

    for (mut state, resharding) in cases {
        let error = rejected(
            &mut state,
            json!({ "collections": { COLLECTION: { "update": { "resharding": resharding } } } }),
        );
        assert!(error.contains("resharding"), "{error}");
    }

    // Removing the last shard of a shard key, while resharding it
    let mut resharding_state = custom_sharding_state();
    resharding_state
        .collections_data
        .collections
        .get_mut(COLLECTION)
        .unwrap()
        .resharding = Some(resharding(Some("a")));

    let error = rejected(
        &mut resharding_state,
        json!({
            "collections": {
                COLLECTION: { "update": { "shards": { "0": null }, "shards_key_mapping": [] } },
            },
        }),
    );
    assert!(error.contains("resharding"), "{error}");

    // Resharding of an existing hash ring is accepted
    for (mut state, resharding) in [
        (state(), resharding(None)),
        (custom_sharding_state(), resharding(Some("a"))),
    ] {
        apply(
            &mut state,
            json!({ "collections": { COLLECTION: { "update": { "resharding": resharding } } } }),
        )
        .unwrap();
    }
}

#[test]
fn new_transfer_from_this_peer_is_rejected() {
    let transfer = |from: PeerId, to: PeerId| json!({ "shard_id": 0, "from": from, "to": to, "sync": true, "method": "stream_records" });
    let set_transfers = |transfers: Vec<Value>| json!({ "collections": { COLLECTION: { "update": { "transfers": transfers } } } });

    // New transfer from this peer is rejected
    let error = rejected(
        &mut state(),
        set_transfers(vec![transfer(PEER_ID, OTHER_PEER_ID)]),
    );
    assert!(error.contains("from this peer"), "{error}");

    // New transfer to this peer is accepted, existing transfer from this peer is kept
    let mut state = state();
    apply(
        &mut state,
        set_transfers(vec![transfer(OTHER_PEER_ID, PEER_ID)]),
    )
    .unwrap();

    state
        .collections_data
        .collections
        .get_mut(COLLECTION)
        .unwrap()
        .transfers
        .insert(serde_json::from_value(transfer(PEER_ID, OTHER_PEER_ID)).unwrap());

    let report = apply(
        &mut state,
        set_transfers(vec![transfer(PEER_ID, OTHER_PEER_ID)]),
    )
    .unwrap();

    let transfers = &state.collections_data.collections[COLLECTION].transfers;
    assert_eq!(transfers.len(), 1);
    assert!(transfers.iter().all(|transfer| transfer.from == PEER_ID));
    assert_eq!(report.changes.len(), 1);
}

#[test]
fn pre_existing_shard_layout_violations_dont_block_changes() {
    let mut state = custom_sharding_state();

    // Shard without shard key, and shard key mapping referencing a missing shard
    let collection = state
        .collections_data
        .collections
        .get_mut(COLLECTION)
        .unwrap();
    let replicas = HashMap::from([(PEER_ID, ReplicaState::Active)]);
    collection.shards.insert(1, ShardInfo { replicas });
    collection
        .shards_key_mapping
        .insert(ShardKey::from("b".to_string()), HashSet::from([2]));

    apply(
        &mut state,
        json!({
            "collections": {
                COLLECTION: {
                    "update": {
                        "shards": {
                            "0": { "replicas": { PEER_ID.to_string(): "Dead" } },
                            "1": { "replicas": { PEER_ID.to_string(): "Dead" } },
                        },
                    },
                },
            },
        }),
    )
    .unwrap();
}

#[test]
fn invalid_quota_config_is_rejected() {
    let error = rejected(
        &mut state(),
        json!({ "quota_config": { "enabled": true, "max_disk_usage_percent": 0 } }),
    );

    assert!(error.contains("invalid quota config"), "{error}");
}

#[test]
fn quota_config_change_is_reported() {
    let mut state = state();

    let mut quota_config = QuotaConfig::default();
    quota_config.enabled = !quota_config.enabled;

    let report = apply(&mut state, json!({ "quota_config": quota_config })).unwrap();

    assert_eq!(state.quota_config, Some(quota_config));

    let paths: Vec<_> = report
        .changes
        .iter()
        .map(|change| change.path.as_str())
        .collect();
    assert_eq!(paths, ["quota_config"]);
}

/// Consensus state with one collection with one shard on two peers, one alias,
/// two peers and one cluster metadata key
fn state() -> SnapshotData {
    let mut collection = collection_state(Vec::new());
    let replicas = HashMap::from([
        (PEER_ID, ReplicaState::Active),
        (OTHER_PEER_ID, ReplicaState::Active),
    ]);
    collection.shards.insert(0, ShardInfo { replicas });

    let mut aliases = AliasMapping::default();
    aliases.insert(ALIAS.to_string(), COLLECTION.to_string());

    let address_by_id = HashMap::from([
        (PEER_ID, "http://peer-1:6335".parse().unwrap()),
        (OTHER_PEER_ID, "http://peer-2:6335".parse().unwrap()),
    ]);

    SnapshotData {
        collections_data: CollectionsSnapshot {
            collections: HashMap::from([(COLLECTION.to_string(), collection)]),
            aliases,
        },
        address_by_id,
        metadata_by_id: HashMap::from([(PEER_ID, PeerMetadata::current())]),
        cluster_metadata: HashMap::from([("owner".to_string(), json!("qdrant"))]),
        quota_config: Some(QuotaConfig::default()),
    }
}

/// Same as [`state`], but the collection uses custom sharding, with shard 0 under shard key `a`
fn custom_sharding_state() -> SnapshotData {
    let mut state = state();

    let collection = state
        .collections_data
        .collections
        .get_mut(COLLECTION)
        .unwrap();
    collection.config.params.sharding_method = Some(ShardingMethod::Custom);
    collection
        .shards_key_mapping
        .insert(ShardKey::from("a".to_string()), HashSet::from([0]));

    state
}

fn apply(state: &mut SnapshotData, diff: Value) -> StorageResult<PatchReport> {
    let diff: ConsensusStateDiff = serde_json::from_value(diff).expect("valid diff");

    let mut report = PatchReport::default();
    diff.apply(state, PEER_ID, &mut report)?;

    Ok(report)
}

/// Apply diff and return the description of the expected bad request error
fn rejected(state: &mut SnapshotData, diff: Value) -> String {
    match apply(state, diff) {
        Err(StorageError::BadRequest { description }) => description,
        result => panic!("expected bad request error, got {result:?}"),
    }
}

/// Changes sorted by path
fn changes(report: &PatchReport) -> Vec<(String, Option<Value>, Option<Value>)> {
    let mut changes: Vec<_> = report
        .changes
        .iter()
        .map(|change| (change.path.clone(), change.old.clone(), change.new.clone()))
        .collect();

    changes.sort_by(|(left, _, _), (right, _, _)| left.cmp(right));
    changes
}
