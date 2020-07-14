use serde::{Deserialize, Serialize};
use segment::types::Distance;
use collection::operations::index_def::Indexes;

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionOps {
    /// Create new collection and (optionally) specify index params
    CreateCollection {
        collection_name: String,
        dim: usize,
        distance: Distance,
        index: Option<Indexes>,
    },
    /// Force construct specified index
    ConstructIndex {
        collection_name: String,
        index: Indexes,
    },
    /// Drop collection
    DeleteCollection {
        collection_name: String,
    },
    /// Perform changes of index aliases
    ChangeAliases {
        actions: Vec<AliasOperations>,
    }
}
