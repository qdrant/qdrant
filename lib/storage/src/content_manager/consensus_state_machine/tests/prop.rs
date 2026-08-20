//! Proptest generators for cluster state and consensus operations

use collection::collection_state;
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

const COLLECTION_NAMES: &[&str] = &["alpha", "beta", "gamma"];
const MISSING_COLLECTION_NAME: &str = "missing";

const ALIAS_NAMES: &[&str] = &["primary", "secondary"];
const DANGLING_ALIAS_NAME: &str = "dangling";

const VECTOR_NAMES: &[&str] = &["", "text", "image"];
const FIELD_NAMES: &[&str] = &["city", "count", "nested.key"];

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

        (Just(collections), arb_aliases(names)).prop_map(|(collections, aliases)| ClusterState {
            collections,
            aliases,
            ..Default::default()
        })
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
    mut collection_names: Vec<String>,
) -> impl Strategy<Value = ConsensusOperations> {
    collection_names.push(MISSING_COLLECTION_NAME.into());

    prop_oneof![
        Just(CollectionMetaOperations::Nop { token: 0 }),
        arb_change_aliases(collection_names.clone()),
        arb_create_named_vector(collection_names.clone()),
        arb_delete_named_vector(collection_names.clone()),
        arb_create_payload_index(collection_names.clone()),
        arb_drop_payload_index(collection_names.clone()),
    ]
    .prop_map(|operation| ConsensusOperations::CollectionMeta(Box::new(operation)))
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
