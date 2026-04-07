use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::data_types::modifier::Modifier;
use crate::index::sparse_index::sparse_index_config::SparseIndexConfig;
use crate::types::{
    Distance, Indexes, MultiVectorConfig, SparseVectorDataConfig, VectorDataConfig,
    VectorStorageDatatype, VectorStorageType,
};

/// Configuration for creating a new named vector.
///
/// Contains only the immutable properties that define a vector space.
/// Storage type, index, and quantization are determined automatically
/// based on the segment type and can be configured separately later.
///
/// Example JSON for a dense vector:
/// ```json
/// { "dense": { "size": 768, "distance": "Cosine" } }
/// ```
///
/// Example JSON for a sparse vector:
/// ```json
/// { "sparse": { "modifier": "Idf" } }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum VectorNameConfig {
    Dense(DenseVectorNameConfig),
    Sparse(SparseVectorNameConfig),
}

/// Wrapper for dense vector creation config.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DenseVectorNameConfig {
    /// Dense vector parameters
    pub dense: DenseVectorConfig,
}

/// Wrapper for sparse vector creation config.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SparseVectorNameConfig {
    /// Sparse vector parameters
    pub sparse: SparseVectorConfig,
}

/// Configuration for creating a new dense named vector.
///
/// Only includes properties that define the vector space and cannot be changed
/// after creation. Storage type, index type, and quantization are inferred.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct DenseVectorConfig {
    /// Dimensionality of the vectors
    pub size: usize,
    /// Distance function used for measuring distance between vectors
    pub distance: Distance,
    /// Configuration for multi-vector points (e.g., ColBERT)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multivector_config: Option<MultiVectorConfig>,
    /// Element storage type (Float32, Float16, Uint8)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

/// Configuration for creating a new sparse named vector.
///
/// Only includes properties that define the vector space and cannot be changed
/// after creation.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SparseVectorConfig {
    /// Value modifier for sparse vectors (e.g., IDF)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<Modifier>,
    /// Datatype used to store weights in the index
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl Validate for VectorNameConfig {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        Ok(())
    }
}

// Convenience constructors for creating VectorNameConfig without wrappers

impl VectorNameConfig {
    pub fn dense(config: DenseVectorConfig) -> Self {
        VectorNameConfig::Dense(DenseVectorNameConfig { dense: config })
    }

    pub fn sparse(config: SparseVectorConfig) -> Self {
        VectorNameConfig::Sparse(SparseVectorNameConfig { sparse: config })
    }
}

impl DenseVectorConfig {
    /// Convert to internal VectorDataConfig with appropriate defaults for a new vector.
    ///
    /// - Storage type is inferred from `on_disk` preference
    /// - Index is always Plain (HNSW can be built later by optimizer)
    /// - Quantization is None (can be configured separately)
    pub fn to_internal(&self, on_disk: bool) -> VectorDataConfig {
        let Self {
            size,
            distance,
            multivector_config,
            datatype,
        } = self;

        VectorDataConfig {
            size: *size,
            distance: *distance,
            storage_type: VectorStorageType::from_on_disk(on_disk),
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config: *multivector_config,
            datatype: *datatype,
        }
    }
}

impl SparseVectorConfig {
    /// Convert to internal SparseVectorDataConfig with appropriate defaults.
    pub fn to_internal(&self) -> SparseVectorDataConfig {
        let Self { modifier, datatype } = self;

        SparseVectorDataConfig {
            index: SparseIndexConfig {
                datatype: *datatype,
                ..Default::default()
            },
            storage_type: Default::default(),
            modifier: *modifier,
        }
    }
}
