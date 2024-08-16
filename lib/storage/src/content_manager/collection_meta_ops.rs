use std::collections::BTreeMap;

use collection::config::{CollectionConfig, ShardingMethod};
use collection::operations::config_diff::{
    CollectionParamsDiff, HnswConfigDiff, OptimizersConfigDiff, QuantizationConfigDiff,
    StrictModeConfigDiff, WalConfigDiff,
};
use collection::operations::types::{
    SparseVectorParams, SparseVectorsConfig, VectorsConfig, VectorsConfigDiff,
};
use collection::shards::replica_set::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::{PeerId, ShardId, ShardsPlacement};
use collection::shards::transfer::{ShardTransfer, ShardTransferKey, ShardTransferRestart};
use collection::shards::{replica_set, CollectionId};
use schemars::JsonSchema;
use segment::types::{PayloadFieldSchema, PayloadKeyType, QuantizationConfig, ShardKey};
use serde::{Deserialize, Serialize};
use validator::Validate;

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
#[derive(Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "snake_case")]
pub struct InitFrom {
    pub collection: CollectionId,
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
    /// If true - point's payload will not be stored in memory.
    /// It will be read from the disk every time it is requested.
    /// This setting saves RAM by (slightly) increasing the response time.
    /// Note: those payload values that are involved in filtering and are indexed - remain in RAM.
    #[serde(default)]
    pub on_disk_payload: Option<bool>,
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
    /// Specify other collection to copy data from.
    #[serde(default)]
    pub init_from: Option<InitFrom>,
    /// Quantization parameters. If none - quantization is disabled.
    #[serde(default, alias = "quantization")]
    #[validate(nested)]
    pub quantization_config: Option<QuantizationConfig>,
    /// Sparse vector data config.
    #[validate(nested)]
    pub sparse_vectors: Option<BTreeMap<String, SparseVectorParams>>,
    /// Strict-mode config.
    #[validate]
    pub strict_mode_config: Option<StrictModeConfigDiff>,
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
    pub fn new(collection_name: String, create_collection: CreateCollection) -> Self {
        Self {
            collection_name,
            create_collection,
            distribution: None,
        }
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
    pub optimizers_config: Option<OptimizersConfigDiff>, // TODO: Allow updates for other configuration params as well
    /// Collection base params. If none - it is left unchanged.
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
            },
            shard_replica_changes: None,
        }
    }

    pub fn new(collection_name: String, update_collection: UpdateCollection) -> Self {
        Self {
            collection_name,
            update_collection,
            shard_replica_changes: None,
        }
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
    Nop { token: usize }, // Empty operation
}

/// Use config of the existing collection to generate a create collection operation
/// for the new collection
impl From<CollectionConfig> for CreateCollection {
    fn from(value: CollectionConfig) -> Self {
        Self {
            vectors: value.params.vectors,
            shard_number: Some(value.params.shard_number.get()),
            sharding_method: value.params.sharding_method,
            replication_factor: Some(value.params.replication_factor.get()),
            write_consistency_factor: Some(value.params.write_consistency_factor.get()),
            on_disk_payload: Some(value.params.on_disk_payload),
            hnsw_config: Some(value.hnsw_config.into()),
            wal_config: Some(value.wal_config.into()),
            optimizers_config: Some(value.optimizer_config.into()),
            init_from: None,
            quantization_config: value.quantization_config,
            sparse_vectors: value.params.sparse_vectors,
            strict_mode_config: value.strict_mode_config,
        }
    }
}
