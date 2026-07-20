// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::collections::BTreeMap;

use collection::config::{
    CollectionConfigInternal, CollectionParams, PayloadStorageParams, ShardingMethod,
};
use collection::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
    WalConfigDiff,
};
use collection::operations::types::{
    SparseVectorParams, SparseVectorsConfig, VectorsConfig, VectorsConfigDiff,
};
use collection::operations::validation;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId, ShardsPlacement};
use collection::shards::transfer::{ShardTransfer, ShardTransferKey, ShardTransferRestart};
use collection::shards::{CollectionId, replica_set};
use schemars::JsonSchema;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{
    Payload, PayloadFieldSchema, PayloadKeyType, QuantizationConfig, ShardKey, StrictModeConfig,
    VectorNameBuf,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

// Re-export staging types when the feature is enabled
#[cfg(feature = "staging")]
pub use super::staging::{TestSlowDown, TestTransientError};
use crate::content_manager::errors::{StorageError, StorageResult};
use crate::content_manager::shard_distribution::ShardDistributionProposal;

// *Operation wrapper structure is only required for better OpenAPI generation

/// Create alternative name for a collection.
/// Collection will be available under both names for search, retrieve,
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateAlias {
    pub collection_name: String,
    pub alias_name: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateAliasOperation {
    pub create_alias: CreateAlias,
}

/// Delete alias if exists
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DeleteAlias {
    pub alias_name: String,
}

/// Delete alias if exists
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DeleteAliasOperation {
    pub delete_alias: DeleteAlias,
}

/// Change alias to a new one
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RenameAlias {
    pub old_alias_name: String,
    pub new_alias_name: String,
}

/// Change alias to a new one
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RenameAliasOperation {
    pub rename_alias: RenameAlias,
}

/// Group of all the possible operations related to collection aliases
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum AliasOperations {
    CreateAlias(CreateAliasOperation),
    DeleteAlias(DeleteAliasOperation),
    RenameAlias(RenameAliasOperation),
}

impl From<CreateAlias> for AliasOperations {
    fn from(create_alias: CreateAlias) -> Self {
        AliasOperations::CreateAlias(CreateAliasOperation { create_alias })
    }
}

impl From<DeleteAlias> for AliasOperations {
    fn from(delete_alias: DeleteAlias) -> Self {
        AliasOperations::DeleteAlias(DeleteAliasOperation { delete_alias })
    }
}

impl From<RenameAlias> for AliasOperations {
    fn from(rename_alias: RenameAlias) -> Self {
        AliasOperations::RenameAlias(RenameAliasOperation { rename_alias })
    }
}

/// Operation for creating new collection and (optionally) specify index params
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateCollection {
    /// Vector data config.
    /// It is possible to provide one config for single vector mode and list of configs for multiple vectors mode.
    #[serde(default)]
    #[validate(nested)]
    pub vectors: VectorsConfig,
    /// For auto sharding:
    /// Number of shards in collection.
    ///  - Default is 1 for standalone, otherwise equal to the number of nodes
    ///  - Minimum is 1
    ///
    /// For custom sharding:
    /// Number of shards in collection per shard group.
    ///  - Default is 1, meaning that each shard key will be mapped to a single shard
    ///  - Minimum is 1
    #[serde(default)]
    #[validate(range(min = 1))]
    pub shard_number: Option<u32>,
    /// Sharding method
    /// Default is Auto - points are distributed across all available shards
    /// Custom - points are distributed across shards according to shard key
    #[serde(default)]
    pub sharding_method: Option<ShardingMethod>,
    /// Number of shards replicas.
    /// Default is 1
    /// Minimum is 1
    #[serde(default)]
    #[validate(range(min = 1))]
    pub replication_factor: Option<u32>,
    /// Defines how many replicas should apply the operation for us to consider it successful.
    /// Increasing this number will make the collection more resilient to inconsistencies, but will
    /// also make it fail if not enough replicas are available.
    /// Does not have any performance impact.
    #[serde(default)]
    #[validate(range(min = 1))]
    pub write_consistency_factor: Option<u32>,
    /// Deprecated: use `payload.memory` instead.
    /// If true - point's payload will not be stored in memory.
    /// It will be read from the disk every time it is requested.
    /// This setting saves RAM by (slightly) increasing the response time.
    /// Note: those payload values that are involved in filtering and are indexed - remain in RAM.
    ///
    /// Default: true
    #[serde(default)]
    #[deprecated(since = "1.19.0", note = "Use `payload.memory` instead")]
    pub on_disk_payload: Option<bool>,
    /// Configuration of the payload storage
    #[serde(default)]
    #[validate(nested)]
    pub payload: Option<PayloadStorageParams>,
    /// Custom params for HNSW index. If none - values from service configuration file are used.
    #[validate(nested)]
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Custom params for WAL. If none - values from service configuration file are used.
    #[validate(nested)]
    pub wal_config: Option<WalConfigDiff>,
    /// Custom params for Optimizers.  If none - values from service configuration file are used.
    #[serde(alias = "optimizer_config")]
    #[validate(nested)]
    pub optimizers_config: Option<OptimizersConfigDiff>,
    /// Quantization parameters. If none - quantization is disabled.
    #[serde(default, alias = "quantization")]
    #[validate(nested)]
    pub quantization_config: Option<QuantizationConfig>,
    /// Sparse vector data config.
    #[validate(nested)]
    pub sparse_vectors: Option<BTreeMap<VectorNameBuf, SparseVectorParams>>,
    /// Strict-mode config.
    #[validate(nested)]
    pub strict_mode_config: Option<StrictModeConfig>,
    #[serde(default)]
    #[schemars(skip)]
    pub uuid: Option<Uuid>,
    /// Arbitrary JSON metadata for the collection
    /// This can be used to store application-specific information
    /// such as creation time, migration data, inference model info, etc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Payload>,
}

/// Operation for creating new collection and (optionally) specify index params
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CreateCollectionOperation {
    pub collection_name: String,
    pub create_collection: CreateCollection,
    distribution: Option<ShardDistributionProposal>,
}

impl CreateCollectionOperation {
    pub fn new(
        collection_name: String,
        create_collection: CreateCollection,
    ) -> StorageResult<Self> {
        // Run the derived `Validate` checks here instead of relying on the API
        // layer: only the REST extractor validates the deserialized request,
        // while gRPC validates the proto message, whose constraints can lag
        // behind the internal ones (e.g. rejecting `memory: pinned` for dense
        // vectors and payload storage). Constructing the operation is the
        // common chokepoint for all API paths, before the operation is
        // proposed to consensus.
        create_collection.validate().map_err(|errs| {
            StorageError::bad_input(validation::label_errors("Validation error in body", &errs))
        })?;

        // Apply the same vector-name validation that the
        // `PUT /collections/{name}/vectors/{vector_name}` endpoint enforces
        // (length 0..=200, no filesystem-unsafe characters), so both creation
        // paths reject the same set of bad names. The `Validate` derive on
        // `CreateCollection` only walks `BTreeMap` *values*, never keys, so this
        // has to run imperatively here.
        //
        // The unnamed slot used by `VectorsConfig::Single` is exempt: its
        // implicit key is the empty `DEFAULT_VECTOR_NAME` constant and a
        // `Single` config has no user-supplied name to validate.
        if let collection::operations::types::VectorsConfig::Multi(multi) =
            &create_collection.vectors
        {
            for vector_name in multi.keys() {
                common::validation::validate_vector_name(vector_name).map_err(|err| {
                    StorageError::bad_input(format!(
                        "Invalid dense vector name `{vector_name}`: {err}",
                    ))
                })?;
            }
        }
        if let Some(sparse_config) = &create_collection.sparse_vectors {
            for vector_name in sparse_config.keys() {
                common::validation::validate_vector_name(vector_name).map_err(|err| {
                    StorageError::bad_input(format!(
                        "Invalid sparse vector name `{vector_name}`: {err}",
                    ))
                })?;
            }
        }

        // validate vector names are unique between dense and sparse vectors
        if let Some(sparse_config) = &create_collection.sparse_vectors {
            if sparse_config.contains_key(DEFAULT_VECTOR_NAME) {
                return Err(StorageError::bad_input(
                    "Sparse vector name cannot be empty",
                ));
            }

            let mut dense_names = create_collection.vectors.params_iter().map(|p| p.0);
            if let Some(duplicate_name) = dense_names.find(|name| sparse_config.contains_key(*name))
            {
                return Err(StorageError::bad_input(format!(
                    "Dense and sparse vector names must be unique - duplicate found with '{duplicate_name}'",
                )));
            }
        }

        Ok(Self {
            collection_name,
            create_collection,
            distribution: None,
        })
    }

    pub fn is_distribution_set(&self) -> bool {
        self.distribution.is_some()
    }

    pub fn take_distribution(&mut self) -> Option<ShardDistributionProposal> {
        self.distribution.take()
    }

    pub fn set_distribution(&mut self, distribution: ShardDistributionProposal) {
        self.distribution = Some(distribution);
    }
}

/// Operation for updating parameters of the existing collection
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct UpdateCollection {
    /// Map of vector data parameters to update for each named vector.
    /// To update parameters in a collection having a single unnamed vector, use an empty string as name.
    #[validate(nested)]
    pub vectors: Option<VectorsConfigDiff>,
    /// Custom params for Optimizers.  If none - it is left unchanged.
    /// This operation is blocking, it will only proceed once all current optimizations are complete
    #[serde(alias = "optimizer_config")]
    #[validate(nested)]
    pub optimizers_config: Option<OptimizersConfigDiff>, // TODO: Allow updates for other configuration params as well
    /// Collection base params. If none - it is left unchanged.
    #[validate(nested)]
    pub params: Option<CollectionParamsDiff>,
    /// HNSW parameters to update for the collection index. If none - it is left unchanged.
    #[validate(nested)]
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Quantization parameters to update. If none - it is left unchanged.
    #[serde(default, alias = "quantization")]
    #[validate(nested)]
    pub quantization_config: Option<QuantizationConfigDiff>,
    /// Map of sparse vector data parameters to update for each sparse vector.
    #[validate(nested)]
    pub sparse_vectors: Option<SparseVectorsConfig>,
    #[validate(nested)]
    pub strict_mode_config: Option<StrictModeConfig>,
    /// Metadata to update for the collection. If provided, this will merge with existing metadata.
    /// Individual keys can be removed by setting their value to `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Payload>,
}

/// Operation for updating parameters of the existing collection
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct UpdateCollectionOperation {
    pub collection_name: String,
    pub update_collection: UpdateCollection,
    shard_replica_changes: Option<Vec<replica_set::Change>>,
}

impl UpdateCollectionOperation {
    pub fn new_empty(collection_name: String) -> Self {
        Self {
            collection_name,
            update_collection: UpdateCollection {
                vectors: None,
                hnsw_config: None,
                params: None,
                optimizers_config: None,
                quantization_config: None,
                sparse_vectors: None,
                strict_mode_config: None,
                metadata: None,
            },
            shard_replica_changes: None,
        }
    }

    pub fn new(
        collection_name: String,
        update_collection: UpdateCollection,
    ) -> StorageResult<Self> {
        // API-layer-independent validation, see `CreateCollectionOperation::new`.
        update_collection.validate().map_err(|errs| {
            StorageError::bad_input(validation::label_errors("Validation error in body", &errs))
        })?;

        Ok(Self {
            collection_name,
            update_collection,
            shard_replica_changes: None,
        })
    }

    pub fn take_shard_replica_changes(&mut self) -> Option<Vec<replica_set::Change>> {
        self.shard_replica_changes.take()
    }

    pub fn set_shard_replica_changes(&mut self, changes: Vec<replica_set::Change>) {
        if changes.is_empty() {
            self.shard_replica_changes = None;
        } else {
            self.shard_replica_changes = Some(changes);
        }
    }
}

/// Operation for performing changes of collection aliases.
/// Alias changes are atomic, meaning that no collection modifications can happen between
/// alias operations.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct ChangeAliasesOperation {
    pub actions: Vec<AliasOperations>,
}

/// Operation for deleting collection with given name
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct DeleteCollectionOperation(pub String);

#[derive(Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub enum ReshardingOperation {
    Start(ReshardKey),
    CommitRead(ReshardKey),
    CommitWrite(ReshardKey),
    Finish(ReshardKey),
    Abort(ReshardKey),
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub enum ShardTransferOperations {
    Start(ShardTransfer),
    /// Restart an existing transfer with a new configuration
    ///
    /// If the given transfer is ongoing, it is aborted and restarted with the new configuration.
    Restart(ShardTransferRestart),
    Finish(ShardTransfer),
    /// Deprecated since Qdrant 1.9.0, used in Qdrant 1.7.0 and 1.8.0
    ///
    /// Used in `ShardTransferMethod::Snapshot`
    ///
    /// Called when the snapshot has successfully been recovered on the remote, brings the transfer
    /// to the next stage.
    SnapshotRecovered(ShardTransferKey),
    /// Used in `ShardTransferMethod::Snapshot` and `ShardTransferMethod::WalDelta`
    ///
    /// Called when the first stage of the transfer has been successfully finished, brings the
    /// transfer to the next stage.
    RecoveryToPartial(ShardTransferKey),
    Abort {
        transfer: ShardTransferKey,
        reason: String,
    },
}

/// Sets the state of shard replica
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct SetShardReplicaState {
    pub collection_name: String,
    pub shard_id: ShardId,
    pub peer_id: PeerId,
    /// If `Active` then the replica is up to date and can receive updates and answer requests
    pub state: ReplicaState,
    /// If `Some` then check that the replica is in this state before changing it
    /// If `None` then the replica can be in any state
    /// This is useful for example when we want to make sure
    /// we only make transition from `Initializing` to `Active`, and not from `Dead` to `Active`.
    /// If `from_state` does not match the current state of the replica, then the operation will be dismissed.
    #[serde(default)]
    pub from_state: Option<ReplicaState>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct CreateShardKey {
    pub collection_name: String,
    pub shard_key: ShardKey,
    pub placement: ShardsPlacement,
    pub initial_state: Option<ReplicaState>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct DropShardKey {
    pub collection_name: String,
    pub shard_key: ShardKey,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct CreatePayloadIndex {
    pub collection_name: String,
    pub field_name: PayloadKeyType,
    pub field_schema: PayloadFieldSchema,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct DropPayloadIndex {
    pub collection_name: String,
    pub field_name: PayloadKeyType,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct CreateNamedVector {
    pub collection_name: String,
    pub vector_name: segment::types::VectorNameBuf,
    pub config: shard::operations::vector_name_ops::VectorNameConfig,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub struct DeleteNamedVector {
    pub collection_name: String,
    pub vector_name: segment::types::VectorNameBuf,
}

/// Enumeration of all possible collection update operations
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub enum CollectionMetaOperations {
    CreateCollection(CreateCollectionOperation),
    UpdateCollection(UpdateCollectionOperation),
    DeleteCollection(DeleteCollectionOperation),
    ChangeAliases(ChangeAliasesOperation),
    Resharding(CollectionId, ReshardingOperation),
    TransferShard(CollectionId, ShardTransferOperations),
    SetShardReplicaState(SetShardReplicaState),
    CreateShardKey(CreateShardKey),
    DropShardKey(DropShardKey),
    CreatePayloadIndex(CreatePayloadIndex),
    DropPayloadIndex(DropPayloadIndex),
    CreateNamedVector(CreateNamedVector),
    DeleteNamedVector(DeleteNamedVector),
    Nop {
        token: usize,
    }, // Empty operation
    /// Introduce artificial delay to a specific peer node
    #[cfg(feature = "staging")]
    TestSlowDown(TestSlowDown),
    /// Simulate a transient consensus failure on a specific peer node
    #[cfg(feature = "staging")]
    TestTransientError(TestTransientError),
}

/// Use config of the existing collection to generate a create collection operation
/// for the new collection
impl From<CollectionConfigInternal> for CreateCollection {
    fn from(value: CollectionConfigInternal) -> Self {
        let CollectionConfigInternal {
            params,
            hnsw_config,
            optimizer_config,
            wal_config,
            quantization_config,
            strict_mode_config,
            uuid,
            metadata,
        } = value;

        let CollectionParams {
            vectors,
            shard_number,
            sharding_method,
            replication_factor,
            write_consistency_factor,
            read_fan_out_factor: _,
            read_fan_out_delay_ms: _,
            on_disk_payload,
            payload,
            sparse_vectors,
        } = params;

        Self {
            vectors,
            shard_number: Some(shard_number.get()),
            sharding_method,
            replication_factor: Some(replication_factor.get()),
            write_consistency_factor: Some(write_consistency_factor.get()),
            on_disk_payload: Some(on_disk_payload),
            payload,
            hnsw_config: Some(hnsw_config.into()),
            wal_config: Some(wal_config.into()),
            optimizers_config: Some(optimizer_config.into()),
            quantization_config,
            sparse_vectors,
            strict_mode_config,
            uuid,
            metadata,
        }
    }
}
