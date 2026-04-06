// Re-export VectorNameConfig types from segment crate
pub use segment::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use segment::types::VectorNameBuf;
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};

/// Operations for creating and deleting named vectors at the shard level.
/// Serialized into WAL.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants, Hash)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum VectorNameOperations {
    /// Create a new named vector in the shard
    CreateVectorName(CreateVectorName),
    /// Delete a named vector from the shard
    DeleteVectorName(DeleteVectorName),
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "snake_case")]
pub struct CreateVectorName {
    pub vector_name: VectorNameBuf,
    pub config: VectorNameConfig,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "snake_case")]
pub struct DeleteVectorName {
    pub vector_name: VectorNameBuf,
}
