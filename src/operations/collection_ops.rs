use crate::common::index_def::IndexType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct CreateCollection {
    collection_name: String,
    dim: usize,
    index: Option<IndexType>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConstructIndex {
    collection_name: String,
    index: IndexType,
}

#[derive(Debug, Deserialize, Serialize)]
struct DeleteCollection {
    collection_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateAlias {
    collection_name: String,
    alias_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DeleteAlias {
    alias_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RenameAlias {
    old_alias_name: String,
    new_alias_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
enum AliasOperations {
    CreateAlias,
    DeleteAlias,
    RenameAlias,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChangeAliases {
    actions: Vec<AliasOperations>,
}
