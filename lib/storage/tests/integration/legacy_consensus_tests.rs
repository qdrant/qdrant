//! Compare CSM application with the retained consensus handlers

use collection::operations::types::PeerMetadata;
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use serde_json::json;
use storage::content_manager::collection_meta_ops::*;
use storage::content_manager::consensus_ops::ConsensusOperations;

use super::alias_tests::{FULL_ACCESS, new_dispatcher};
use super::consensus_action_applier_tests::consensus_manager;
use super::consensus_application_tests::{create_collection, entry, meta, state};

#[test]
fn collection_operations_match_legacy_handlers() {
    let (legacy_dir, _legacy_handle, legacy_dispatcher) = new_dispatcher();
    let (csm_dir, _csm_handle, csm_dispatcher) = new_dispatcher();
    let pass = new_unchecked_verification_pass();
    let legacy = consensus_manager(
        &legacy_dir,
        legacy_dispatcher.toc(&FULL_ACCESS, &pass).clone(),
    );
    let csm = consensus_manager(&csm_dir, csm_dispatcher.toc(&FULL_ACCESS, &pass).clone());
    let operations = vec![
        create_collection("books", false),
        meta(CollectionMetaOperations::ChangeAliases(
            ChangeAliasesOperation {
                actions: vec![
                    CreateAlias {
                        collection_name: "books".into(),
                        alias_name: "novels".into(),
                    }
                    .into(),
                ],
            },
        )),
        ConsensusOperations::initialize_replica("books".into(), 0, 0),
        meta(CollectionMetaOperations::UpdateCollection(
            UpdateCollectionOperation::new(
                "novels".into(),
                serde_json::from_value(
                    json!({"hnsw_config": {"m": 24}, "metadata": {"owner": "reader"}}),
                )
                .unwrap(),
            )
            .unwrap(),
        )),
        meta(CollectionMetaOperations::CreatePayloadIndex(
            CreatePayloadIndex {
                collection_name: "novels".into(),
                field_name: "city".parse().unwrap(),
                field_schema: segment::types::PayloadSchemaType::Keyword.into(),
            },
        )),
        meta(CollectionMetaOperations::DropPayloadIndex(
            DropPayloadIndex {
                collection_name: "books".into(),
                field_name: "city".parse().unwrap(),
            },
        )),
        // User errors must leave state unchanged and keep their error variants
        meta(CollectionMetaOperations::UpdateCollection(
            UpdateCollectionOperation::new(
                "books".into(),
                serde_json::from_value(
                    json!({"hnsw_config": {"m": 32}, "vectors": {"missing": {"on_disk": true}}}),
                )
                .unwrap(),
            )
            .unwrap(),
        )),
        meta(CollectionMetaOperations::CreateNamedVector(
            CreateNamedVector {
                collection_name: "books".into(),
                vector_name: "text".into(),
                config: serde_json::from_value(json!({"dense": {"size": 8, "distance": "Dot"}}))
                    .unwrap(),
            },
        )),
        meta(CollectionMetaOperations::DeleteNamedVector(
            DeleteNamedVector {
                collection_name: "books".into(),
                vector_name: "text".into(),
            },
        )),
        create_collection("custom", true),
        meta(CollectionMetaOperations::CreatePayloadIndex(
            CreatePayloadIndex {
                collection_name: "custom".into(),
                field_name: "city".parse().unwrap(),
                field_schema: segment::types::PayloadSchemaType::Keyword.into(),
            },
        )),
        meta(CollectionMetaOperations::CreateShardKey(CreateShardKey {
            collection_name: "custom".into(),
            shard_key: "tenant".to_string().into(),
            placement: vec![vec![0]],
            initial_state: Some(ReplicaState::Active),
        })),
        meta(CollectionMetaOperations::DropShardKey(DropShardKey {
            collection_name: "custom".into(),
            shard_key: "tenant".to_string().into(),
        })),
        ConsensusOperations::UpdatePeerMetadata {
            peer_id: 0,
            metadata: PeerMetadata::current(),
        },
        ConsensusOperations::UpdateClusterMetadata {
            key: "owner".into(),
            value: json!("reader"),
        },
        ConsensusOperations::UpdateClusterMetadata {
            key: "owner".into(),
            value: json!(null),
        },
        ConsensusOperations::SetQuotaConfig(Default::default()),
        meta(CollectionMetaOperations::DeleteCollection(
            DeleteCollectionOperation("books".into()),
        )),
        meta(CollectionMetaOperations::DeleteCollection(
            DeleteCollectionOperation("books".into()),
        )),
    ];

    for operation in operations {
        let expected = legacy._apply_normal_entry(&entry(&operation));
        let actual = csm.apply_normal_entry(&entry(&operation));

        match (expected, actual) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "{operation:?}"),
            (Err(expected), Err(actual)) => assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected),
                "{operation:?}: {actual} != {expected}"
            ),
            results => panic!("different results for {operation:?}: {results:?}"),
        }
        assert_eq!(
            state(&csm, csm_dispatcher.toc(&FULL_ACCESS, &pass)),
            state(&legacy, legacy_dispatcher.toc(&FULL_ACCESS, &pass)),
            "{operation:?}"
        );
    }
}
