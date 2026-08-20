//! Proptest generators for cluster state and consensus operations

use std::collections::HashMap;

use collection::collection_state;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use proptest::prelude::*;
use segment::data_types::modifier::Modifier;
use segment::data_types::vector_name_config::*;
use segment::json_path::JsonPath;
use segment::types::*;

use super::*;
use crate::content_manager::alias_mapping::AliasMapping;
use crate::content_manager::collection_meta_ops::*;
use crate::content_manager::consensus_ops::ConsensusOperations;
use crate::content_manager::consensus_state_machine::*;
use crate::quota::QuotaConfig;
use crate::types::PeerMetadataById;

const COLLECTION_NAMES: &[&str] = &["alpha", "beta", "gamma"];
const MISSING_COLLECTION_NAME: &str = "missing";

const ALIAS_NAMES: &[&str] = &["primary", "secondary"];
const DANGLING_ALIAS_NAME: &str = "dangling";

const VECTOR_NAMES: &[&str] = &["", "text", "image"];
const FIELD_NAMES: &[&str] = &["city", "count", "nested.key"];

/// This node, and one other peer
const PEER_IDS: &[PeerId] = &[PEER_ID, 43];
const PEER_VERSIONS: &[&str] = &["1.14.0", "1.15.0"];

const METADATA_KEYS: &[&str] = &["region", "tier"];

pub fn arb_state_and_operation() -> impl Strategy<Value = (ClusterState, ConsensusOperations)> {
    arb_cluster_state().prop_flat_map(|state| {
        let collections = state.collections.keys().cloned();
        let aliases = state.aliases.iter().map(|(alias, _)| alias.clone());

        let names = collections.chain(aliases).collect();
        let operations = arb_consensus_operation(names);

        (Just(state), operations)
    })
}

pub fn arb_cluster_state() -> impl Strategy<Value = ClusterState> {
    let collections = proptest::collection::hash_map(
        proptest::sample::select(COLLECTION_NAMES).prop_map(CollectionId::from),
        arb_collection_state(),
        0..3,
    );

    collections.prop_flat_map(|collections| {
        let names = collections.keys().cloned().collect();

        (
            Just(collections),
            arb_aliases(names),
            arb_peer_metadata_by_id(),
            arb_cluster_metadata(),
            proptest::option::of(arb_quota_config()),
        )
            .prop_map(
                |(collections, aliases, peer_metadata_by_id, cluster_metadata, quota_config)| {
                    ClusterState {
                        collections,
                        aliases,
                        peer_metadata_by_id,
                        cluster_metadata,
                        quota_config,
                        ..Default::default()
                    }
                },
            )
    })
}

fn arb_peer_metadata_by_id() -> impl Strategy<Value = PeerMetadataById> {
    proptest::collection::hash_map(arb_peer_id(), arb_peer_metadata(), 0..3)
}

fn arb_peer_id() -> impl Strategy<Value = PeerId> {
    proptest::sample::select(PEER_IDS)
}

fn arb_peer_metadata() -> impl Strategy<Value = PeerMetadata> {
    proptest::sample::select(PEER_VERSIONS)
        .prop_map(|version| PeerMetadata::new(version.parse().expect("valid version")))
}

/// Cluster metadata never holds a null value: that is how a key is removed
fn arb_cluster_metadata() -> impl Strategy<Value = HashMap<String, serde_json::Value>> {
    proptest::collection::hash_map(arb_metadata_key(), arb_metadata_value(), 0..2)
}

fn arb_metadata_key() -> impl Strategy<Value = String> {
    proptest::sample::select(METADATA_KEYS).prop_map(String::from)
}

fn arb_metadata_value() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(serde_json::json!("eu")),
        Just(serde_json::json!(2)),
        Just(serde_json::json!(true)),
    ]
}

/// Quota config varying two of its fields: no covered operation reads any of them
fn arb_quota_config() -> impl Strategy<Value = QuotaConfig> {
    let enabled = proptest::bool::ANY;
    let max_resident_memory_percent = proptest::option::of(Just(90));

    (enabled, max_resident_memory_percent).prop_map(|(enabled, max_resident_memory_percent)| {
        QuotaConfig {
            enabled,
            max_resident_memory_percent,
            max_disk_usage_percent: None,
            release_margin_percent: None,
        }
    })
}

fn arb_collection_state() -> impl Strategy<Value = collection_state::State> {
    let vectors =
        proptest::collection::btree_map(arb_vector_name(), arb_vector_name_config(), 0..3);
    let indexes = proptest::collection::hash_map(arb_field_name(), arb_field_schema(), 0..3);

    (vectors, indexes).prop_map(|(vectors, indexes)| {
        let mut state = collection_state(vectors.into_iter().collect());
        state.payload_index_schema.schema = indexes;
        state
    })
}

fn arb_aliases(collections: Vec<CollectionId>) -> impl Strategy<Value = AliasMapping> {
    // `select` samples from a non-empty list, and a state without collections has nothing to alias
    let aliases = if collections.is_empty() {
        Just(Vec::new()).boxed()
    } else {
        let alias = proptest::sample::select(ALIAS_NAMES);
        let collection = proptest::sample::select(collections);

        proptest::collection::vec((alias, collection), 0..2).boxed()
    };

    (aliases, proptest::bool::ANY).prop_map(|(aliases, dangling)| {
        let mut mapping = AliasMapping::default();

        for (alias, collection) in aliases {
            mapping.insert(alias.into(), collection);
        }

        if dangling {
            mapping.insert(DANGLING_ALIAS_NAME.into(), MISSING_COLLECTION_NAME.into());
        }

        mapping
    })
}

pub fn arb_consensus_operation(
    collection_names: Vec<String>,
) -> impl Strategy<Value = ConsensusOperations> {
    let collection_meta = arb_collection_meta_operation(collection_names)
        .prop_map(|operation| ConsensusOperations::CollectionMeta(Box::new(operation)));

    // Weighted by how many operations each arm covers, so one operation is as likely as another
    prop_oneof![
        6 => collection_meta,
        1 => arb_update_peer_metadata(),
        1 => arb_update_cluster_metadata(),
        1 => arb_quota_config().prop_map(ConsensusOperations::SetQuotaConfig),
    ]
}

fn arb_collection_meta_operation(
    mut collection_names: Vec<String>,
) -> impl Strategy<Value = CollectionMetaOperations> {
    collection_names.push(MISSING_COLLECTION_NAME.into());

    prop_oneof![
        Just(CollectionMetaOperations::Nop { token: 0 }),
        arb_change_aliases(collection_names.clone()),
        arb_create_named_vector(collection_names.clone()),
        arb_delete_named_vector(collection_names.clone()),
        arb_create_payload_index(collection_names.clone()),
        arb_drop_payload_index(collection_names.clone()),
    ]
}

fn arb_update_peer_metadata() -> impl Strategy<Value = ConsensusOperations> {
    (arb_peer_id(), arb_peer_metadata()).prop_map(|(peer_id, metadata)| {
        ConsensusOperations::UpdatePeerMetadata { peer_id, metadata }
    })
}

fn arb_update_cluster_metadata() -> impl Strategy<Value = ConsensusOperations> {
    let value = prop_oneof![arb_metadata_value(), Just(serde_json::Value::Null)];

    (arb_metadata_key(), value)
        .prop_map(|(key, value)| ConsensusOperations::UpdateClusterMetadata { key, value })
}

fn arb_collection_name(names: Vec<String>) -> impl Strategy<Value = String> {
    proptest::sample::select(names)
}

fn arb_change_aliases(collections: Vec<String>) -> impl Strategy<Value = CollectionMetaOperations> {
    let actions = proptest::collection::vec(arb_alias_operation(collections), 1..3);

    actions.prop_map(|actions| {
        CollectionMetaOperations::ChangeAliases(ChangeAliasesOperation { actions })
    })
}

fn arb_alias_operation(collections: Vec<String>) -> impl Strategy<Value = AliasOperations> {
    let create = (arb_alias_name(), arb_collection_name(collections)).prop_map(
        |(alias_name, collection_name)| {
            CreateAlias {
                collection_name,
                alias_name,
            }
            .into()
        },
    );

    let delete = arb_alias_name().prop_map(|alias_name| DeleteAlias { alias_name }.into());

    let rename =
        (arb_alias_name(), arb_alias_name()).prop_map(|(old_alias_name, new_alias_name)| {
            RenameAlias {
                old_alias_name,
                new_alias_name,
            }
            .into()
        });

    prop_oneof![create, delete, rename]
}

fn arb_alias_name() -> impl Strategy<Value = String> {
    let names: Vec<_> = ALIAS_NAMES
        .iter()
        .chain([&DANGLING_ALIAS_NAME])
        .copied()
        .collect();

    proptest::sample::select(names).prop_map(String::from)
}

fn arb_create_named_vector(
    collections: Vec<String>,
) -> impl Strategy<Value = CollectionMetaOperations> {
    let collection_name = arb_collection_name(collections);
    let vector_name = arb_vector_name();
    let config = arb_vector_name_config();

    (collection_name, vector_name, config).prop_map(|(collection_name, vector_name, config)| {
        CollectionMetaOperations::CreateNamedVector(CreateNamedVector {
            collection_name,
            vector_name,
            config,
        })
    })
}

fn arb_delete_named_vector(
    collections: Vec<String>,
) -> impl Strategy<Value = CollectionMetaOperations> {
    let collection_name = arb_collection_name(collections);
    let vector_name = arb_vector_name();

    (collection_name, vector_name).prop_map(|(collection_name, vector_name)| {
        CollectionMetaOperations::DeleteNamedVector(DeleteNamedVector {
            collection_name,
            vector_name,
        })
    })
}

fn arb_create_payload_index(
    collections: Vec<String>,
) -> impl Strategy<Value = CollectionMetaOperations> {
    let collection_name = arb_collection_name(collections);
    let field_name = arb_field_name();
    let field_schema = arb_field_schema();

    (collection_name, field_name, field_schema).prop_map(
        |(collection_name, field_name, field_schema)| {
            CollectionMetaOperations::CreatePayloadIndex(CreatePayloadIndex {
                collection_name,
                field_name,
                field_schema,
            })
        },
    )
}

fn arb_drop_payload_index(
    collections: Vec<String>,
) -> impl Strategy<Value = CollectionMetaOperations> {
    let collection_name = arb_collection_name(collections);
    let field_name = arb_field_name();

    (collection_name, field_name).prop_map(|(collection_name, field_name)| {
        CollectionMetaOperations::DropPayloadIndex(DropPayloadIndex {
            collection_name,
            field_name,
        })
    })
}

fn arb_vector_name() -> impl Strategy<Value = VectorNameBuf> {
    proptest::sample::select(VECTOR_NAMES).prop_map(VectorNameBuf::from)
}

fn arb_vector_name_config() -> impl Strategy<Value = VectorNameConfig> {
    prop_oneof![
        arb_dense_config().prop_map(VectorNameConfig::dense),
        arb_sparse_config().prop_map(VectorNameConfig::sparse),
    ]
}

fn arb_dense_config() -> impl Strategy<Value = DenseVectorConfig> {
    let size = proptest::sample::select(vec![4_usize, 8]);
    let distance = proptest::sample::select(vec![Distance::Cosine, Distance::Dot]);

    (size, distance).prop_map(|(size, distance)| DenseVectorConfig {
        size,
        distance,
        multivector_config: None,
        datatype: None,
    })
}

fn arb_sparse_config() -> impl Strategy<Value = SparseVectorConfig> {
    let modifier = proptest::option::of(Just(Modifier::Idf));

    modifier.prop_map(|modifier| SparseVectorConfig {
        modifier,
        datatype: None,
    })
}

fn arb_field_name() -> impl Strategy<Value = JsonPath> {
    proptest::sample::select(FIELD_NAMES).prop_map(|name| name.parse().expect("valid field name"))
}

fn arb_field_schema() -> impl Strategy<Value = PayloadFieldSchema> {
    const SCHEMA_TYPES: &[PayloadSchemaType] = &[
        PayloadSchemaType::Keyword,
        PayloadSchemaType::Integer,
        PayloadSchemaType::Float,
    ];

    proptest::sample::select(SCHEMA_TYPES).prop_map(PayloadFieldSchema::FieldType)
}
