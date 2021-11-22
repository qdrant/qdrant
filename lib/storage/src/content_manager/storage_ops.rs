use collection::operations::config_diff::{HnswConfigDiff, OptimizersConfigDiff, WalConfigDiff};
use schemars::JsonSchema;
use segment::types::Distance;
use serde::{Deserialize, Serialize};

/// Create alternative name for a collection.
/// Collection will be available under both names for search, retrieve,
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CreateAlias {
    pub collection_name: String,
    pub alias_name: String,
}

/// Delete alias if exists
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DeleteAlias {
    pub alias_name: String
}

/// Change alias to a new one
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct RenameAlias {
    pub old_alias_name: String,
    pub new_alias_name: String,
}

/// Group of all the possible operations related to collection aliases
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AliasOperations {
    CreateAlias(CreateAlias),
    DeleteAlias(DeleteAlias),
    RenameAlias(RenameAlias),
}

/// Operation for creating new collection and (optionally) specify index params
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CreateCollection {
    pub vector_size: usize,
    pub distance: Distance,
    /// Custom params for HNSW index. If none - values from service configuration file are used.
    pub hnsw_config: Option<HnswConfigDiff>,
    /// Custom params for WAL. If none - values from service configuration file are used.
    pub wal_config: Option<WalConfigDiff>,
    /// Custom params for Optimizers.  If none - values from service configuration file are used.
    pub optimizers_config: Option<OptimizersConfigDiff>,
}

/// Operation for creating new collection and (optionally) specify index params
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct CreateCollectionOperation {
    pub name: String,
    #[serde(flatten)]
    pub create_collection: CreateCollection,
}

/// Operation for updating parameters of the existing collection
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UpdateCollection {
    /// Custom params for Optimizers.  If none - values from service configuration file are used.
    /// This operation is blocking, it will only proceed ones all current optimizations are complete
    pub optimizers_config: Option<OptimizersConfigDiff>, // ToDo: Allow updates for other configuration params as well
}

/// Operation for updating parameters of the existing collection
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct UpdateCollectionOperation {
    pub name: String,
    #[serde(flatten)]
    pub update_collection: UpdateCollection,
}

/// Operation for performing changes of collection aliases.
/// Alias changes are atomic, meaning that no collection modifications can happen between
/// alias operations.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ChangeAliasesOperation {
    pub actions: Vec<AliasOperations>,
}

/// Operation for deleting collection with given name
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DeleteCollectionOperation(pub String);

/// Enumeration of all possible collection update operations
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageOperations {
    CreateCollection(CreateCollectionOperation),
    UpdateCollection(UpdateCollectionOperation),
    DeleteCollection(DeleteCollectionOperation),
    ChangeAliases(ChangeAliasesOperation),
}
