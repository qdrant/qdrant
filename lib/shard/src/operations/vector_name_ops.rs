use segment::data_types::modifier::Modifier;
use segment::index::sparse_index::sparse_index_config::SparseIndexConfig;
use segment::types::{
    Distance, Indexes, MultiVectorConfig, SparseVectorDataConfig, VectorDataConfig, VectorNameBuf,
    VectorStorageDatatype, VectorStorageType,
};
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

/// Configuration for creating a new named vector.
///
/// Contains only the immutable properties that define a vector space.
/// Storage type, index, and quantization are determined automatically
/// based on the segment type and can be configured separately later.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorNameConfig {
    Dense(DenseVectorNameConfig),
    Sparse(SparseVectorNameConfig),
}

/// Configuration for creating a new dense named vector.
///
/// Only includes properties that define the vector space and cannot be changed
/// after creation. Storage type, index type, and quantization are inferred.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DenseVectorNameConfig {
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
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SparseVectorNameConfig {
    /// Value modifier for sparse vectors (e.g., IDF)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<Modifier>,
    /// Datatype used to store weights in the index
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datatype: Option<VectorStorageDatatype>,
}

impl DenseVectorNameConfig {
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

impl SparseVectorNameConfig {
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

impl VectorNameConfig {
    /// Convert to the segment-internal VectorNameConfig type.
    pub fn to_internal(&self, on_disk: bool) -> segment::types::VectorNameConfigInternal {
        match self {
            VectorNameConfig::Dense(dense) => {
                segment::types::VectorNameConfigInternal::Dense(dense.to_internal(on_disk))
            }
            VectorNameConfig::Sparse(sparse) => {
                segment::types::VectorNameConfigInternal::Sparse(sparse.to_internal())
            }
        }
    }
}
