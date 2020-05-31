use crate::common::index_def::IndexType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateCollection {
    pub collection_name: String,
    pub dim: usize,
    pub index: Option<IndexType>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ConstructIndex {
    pub collection_name: String,
    pub index: IndexType,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteCollection {
    pub collection_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateAlias {
    pub collection_name: String,
    pub alias_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteAlias {
    pub alias_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RenameAlias {
    pub old_alias_name: String,
    pub new_alias_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum AliasOperations {
    CreateAlias,
    DeleteAlias,
    RenameAlias,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ChangeAliases {
    actions: Vec<AliasOperations>,
}
