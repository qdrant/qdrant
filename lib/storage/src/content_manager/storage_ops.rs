use serde::{Deserialize, Serialize};
use schemars::{JsonSchema};
use segment::types::{Distance, Indexes};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AliasOperations {
    CreateAlias {
        collection_name: String,
        alias_name: String,
    },
    DeleteAlias {
        alias_name: String,
    },
    RenameAlias {
        old_alias_name: String,
        new_alias_name: String,
    },
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StorageOps {
    /// Create new collection and (optionally) specify index params
    CreateCollection {
        name: String,
        vector_size: usize,
        distance: Distance,
        index: Option<Indexes>,
    },
    /// Delete collection with given name
    DeleteCollection(String),
    /// Perform changes of collection aliases
    ChangeAliases {
        actions: Vec<AliasOperations>,
    }
}
