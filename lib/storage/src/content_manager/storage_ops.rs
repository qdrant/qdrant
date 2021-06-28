use collection::operations::config_diff::{HnswConfigDiff, OptimizersConfigDiff, WalConfigDiff};
use schemars::JsonSchema;
use segment::types::Distance;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AliasOperations {
    /// Create alternative name for a collection.
    /// Collection will be available under both names for search, retrieve,
    CreateAlias {
        collection_name: String,
        alias_name: String,
    },
    /// Delete alias if exists
    DeleteAlias { alias_name: String },
    /// Change alias to a new one
    RenameAlias {
        old_alias_name: String,
        new_alias_name: String,
    },
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageOperations {
    /// Create new collection and (optionally) specify index params
    CreateCollection {
        name: String,
        vector_size: usize,
        distance: Distance,
        /// Custom params for HNSW index. If none - values from service configuration file are used.
        hnsw_config: Option<HnswConfigDiff>,
        /// Custom params for WAL. If none - values from service configuration file are used.
        wal_config: Option<WalConfigDiff>,
        /// Custom params for Optimizers.  If none - values from service configuration file are used.
        optimizers_config: Option<OptimizersConfigDiff>,
    },
    /// Update parameters of the existing collection
    UpdateCollection {
        name: String,
        /// Custom params for Optimizers.  If none - values from service configuration file are used.
        /// This operation is blocking, it will only proceed ones all current optimizations are complete
        optimizers_config: Option<OptimizersConfigDiff>, // ToDo: Allow updates for other configuration params as well
    },
    /// Delete collection with given name
    DeleteCollection(String),
    /// Perform changes of collection aliases.
    /// Alias changes are atomic, meaning that no collection modifications can happen between
    /// alias operations.
    ChangeAliases { actions: Vec<AliasOperations> },
}
