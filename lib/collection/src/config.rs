use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write as _};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::Path;

use atomicwrites::AtomicFile;
use atomicwrites::OverwriteBehavior::AllowOverwrite;
use fs_err::File;
use schemars::JsonSchema;
use segment::common::anonymize::Anonymize;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::types::{
    Distance, HnswConfig, Indexes, Payload, PayloadStorageType, QuantizationConfig, SegmentConfig,
    SparseVectorDataConfig, StrictModeConfig, VectorDataConfig, VectorName, VectorNameBuf,
    VectorStorageDatatype, VectorStorageType,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;
use wal::WalOptions;

use crate::operations::config_diff::{DiffConfig, QuantizationConfigDiff};
use crate::operations::types::{
    CollectionError, CollectionResult, CollectionWarning, SparseVectorParams, SparseVectorsConfig,
    VectorParams, VectorParamsDiff, VectorsConfig, VectorsConfigDiff,
};
use crate::operations::validation;
use crate::optimizers_builder::OptimizersConfig;

pub const COLLECTION_CONFIG_FILE: &str = "config.json";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Anonymize, Clone, PartialEq, Eq)]
#[anonymize(false)]
pub struct WalConfig {
    /// Size of a single WAL segment in MB
    #[validate(range(min = 1))]
    pub wal_capacity_mb: usize,
    /// Number of WAL segments to create ahead of actually used ones
    pub wal_segments_ahead: usize,
    /// Number of closed WAL segments to keep
    #[validate(range(min = 1))]
    #[serde(default = "default_wal_retain_closed")]
    pub wal_retain_closed: usize,
}

fn default_wal_retain_closed() -> usize {
    1
}

impl From<&WalConfig> for WalOptions {
    fn from(config: &WalConfig) -> Self {
        let WalConfig {
            wal_capacity_mb,
            wal_segments_ahead,
            wal_retain_closed,
        } = config;
        WalOptions {
            segment_capacity: wal_capacity_mb * 1024 * 1024,
            segment_queue_len: *wal_segments_ahead,
            retain_closed: NonZeroUsize::new(*wal_retain_closed).unwrap(),
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            wal_capacity_mb: 32,
            wal_segments_ahead: 0,
            wal_retain_closed: default_wal_retain_closed(),
        }
    }
}

#[derive(
    Debug, Deserialize, Serialize, JsonSchema, Anonymize, PartialEq, Eq, Hash, Clone, Copy, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ShardingMethod {
    #[default]
    Auto,
    Custom,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Anonymize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct CollectionParams {
    /// Configuration of the vector storage
    #[validate(nested)]
    #[serde(default)]
    pub vectors: VectorsConfig,
    /// Number of shards the collection has
    #[serde(default = "default_shard_number")]
    #[anonymize(false)]
    pub shard_number: NonZeroU32,
    /// Sharding method
    /// Default is Auto - points are distributed across all available shards
    /// Custom - points are distributed across shards according to shard key
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sharding_method: Option<ShardingMethod>,
    /// Number of replicas for each shard
    #[serde(default = "default_replication_factor")]
    #[anonymize(false)]
    pub replication_factor: NonZeroU32,
    /// Defines how many replicas should apply the operation for us to consider it successful.
    /// Increasing this number will make the collection more resilient to inconsistencies, but will
    /// also make it fail if not enough replicas are available.
    /// Does not have any performance impact.
    #[serde(default = "default_write_consistency_factor")]
    #[anonymize(false)]
    pub write_consistency_factor: NonZeroU32,
    /// Defines how many additional replicas should be processing read request at the same time.
    /// Default value is Auto, which means that fan-out will be determined automatically based on
    /// the busyness of the local replica.
    /// Having more than 0 might be useful to smooth latency spikes of individual nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[anonymize(false)]
    pub read_fan_out_factor: Option<u32>,
    /// If true - point's payload will not be stored in memory.
    /// It will be read from the disk every time it is requested.
    /// This setting saves RAM by (slightly) increasing the response time.
    /// Note: those payload values that are involved in filtering and are indexed - remain in RAM.
    ///
    /// Default: true
    #[serde(default = "default_on_disk_payload")]
    pub on_disk_payload: bool,
    /// Configuration of the sparse vector storage
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub sparse_vectors: Option<BTreeMap<VectorNameBuf, SparseVectorParams>>,
}

impl CollectionParams {
    pub fn payload_storage_type(&self) -> PayloadStorageType {
        #[cfg(feature = "rocksdb")]
        if self.on_disk_payload {
            PayloadStorageType::Mmap
        } else if common::flags::feature_flags().payload_storage_skip_rocksdb {
            PayloadStorageType::InRamMmap
        } else {
            PayloadStorageType::InMemory
        }

        #[cfg(not(feature = "rocksdb"))]
        if self.on_disk_payload {
            PayloadStorageType::Mmap
        } else {
            PayloadStorageType::InRamMmap
        }
    }

    pub fn check_compatible(&self, other: &CollectionParams) -> CollectionResult<()> {
        let CollectionParams {
            vectors,
            shard_number: _, // Maybe be updated by resharding, assume local shards needs to be dropped
            sharding_method, // Not changeable
            replication_factor: _, // May be changed
            write_consistency_factor: _, // May be changed
            read_fan_out_factor: _, // May be changed
            on_disk_payload: _, // May be changed
            sparse_vectors,  // Parameters may be changes, but not the structure
        } = other;

        self.vectors.check_compatible(vectors)?;

        let this_sparse_vectors: HashSet<_> = if let Some(sparse_vectors) = &self.sparse_vectors {
            sparse_vectors.keys().collect()
        } else {
            HashSet::new()
        };

        let other_sparse_vectors: HashSet<_> = if let Some(sparse_vectors) = sparse_vectors {
            sparse_vectors.keys().collect()
        } else {
            HashSet::new()
        };

        if this_sparse_vectors != other_sparse_vectors {
            return Err(CollectionError::bad_input(format!(
                "sparse vectors are incompatible: \
                 origin sparse vectors: {this_sparse_vectors:?}, \
                 while other sparse vectors: {other_sparse_vectors:?}",
            )));
        }

        let this_sharding_method = self.sharding_method.unwrap_or_default();
        let other_sharding_method = sharding_method.unwrap_or_default();

        if this_sharding_method != other_sharding_method {
            return Err(CollectionError::bad_input(format!(
                "sharding method is incompatible: \
                 origin sharding method: {this_sharding_method:?}, \
                 while other sharding method: {other_sharding_method:?}",
            )));
        }

        Ok(())
    }
}

pub fn default_shard_number() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}

pub fn default_replication_factor() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}

pub fn default_write_consistency_factor() -> NonZeroU32 {
    NonZeroU32::new(1).unwrap()
}

pub const fn default_on_disk_payload() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Validate, Clone, PartialEq)]
pub struct CollectionConfigInternal {
    #[validate(nested)]
    pub params: CollectionParams,
    #[validate(nested)]
    pub hnsw_config: HnswConfig,
    #[validate(nested)]
    pub optimizer_config: OptimizersConfig,
    #[validate(nested)]
    pub wal_config: WalConfig,
    #[serde(default)]
    #[validate(nested)]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(nested)]
    pub strict_mode_config: Option<StrictModeConfig>,
    #[serde(default)]
    pub uuid: Option<Uuid>,
    /// Arbitrary JSON metadata for the collection
    /// This can be used to store application-specific information
    /// such as creation time, migration data, inference model info, etc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Payload>,
}

impl CollectionConfigInternal {
    pub fn to_bytes(&self) -> CollectionResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|err| CollectionError::service_error(err.to_string()))
    }

    pub fn save(&self, path: &Path) -> CollectionResult<()> {
        let config_path = path.join(COLLECTION_CONFIG_FILE);
        let af = AtomicFile::new(&config_path, AllowOverwrite);
        let state_bytes = serde_json::to_vec(self).unwrap();
        af.write(|f| f.write_all(&state_bytes)).map_err(|err| {
            CollectionError::service_error(format!("Can't write {config_path:?}, error: {err}"))
        })?;
        Ok(())
    }

    pub fn load(path: &Path) -> CollectionResult<Self> {
        let config_path = path.join(COLLECTION_CONFIG_FILE);
        let mut contents = String::new();
        let mut file = File::open(config_path)?;
        file.read_to_string(&mut contents)?;
        Ok(serde_json::from_str(&contents)?)
    }

    /// Check if collection config exists
    pub fn check(path: &Path) -> bool {
        let config_path = path.join(COLLECTION_CONFIG_FILE);
        config_path.exists()
    }

    pub fn validate_and_warn(&self) {
        if let Err(ref errs) = self.validate() {
            validation::warn_validation_errors("Collection configuration file", errs);
        }
    }

    /// Get warnings related to this configuration
    pub fn get_warnings(&self) -> Vec<CollectionWarning> {
        let mut warnings = Vec::new();

        for (vector_name, vector_config) in self.params.vectors.params_iter() {
            let vector_hnsw = self
                .hnsw_config
                .update_opt(vector_config.hnsw_config.as_ref());

            let vector_quantization =
                vector_config.quantization_config.is_some() || self.quantization_config.is_some();

            if vector_hnsw.inline_storage.unwrap_or_default() {
                if !vector_quantization {
                    warnings.push(CollectionWarning {
                        message: format!(
                            "The `hnsw_config.inline_storage` option for vector '{vector_name}' \
                             requires quantization to be enabled. This option will be ignored."
                        ),
                    });
                }
                if vector_config.multivector_config.is_some() {
                    warnings.push(CollectionWarning {
                        message: format!(
                            "The `hnsw_config.inline_storage` option for vector '{vector_name}' \
                             is not compatible with multivectors. This option will be ignored."
                        ),
                    });
                }
            }
        }

        warnings
    }

    pub fn to_base_segment_config(&self) -> CollectionResult<SegmentConfig> {
        self.params
            .to_base_segment_config(self.quantization_config.as_ref())
    }
}

impl CollectionParams {
    pub fn empty() -> Self {
        CollectionParams {
            vectors: Default::default(),
            shard_number: default_shard_number(),
            sharding_method: None,
            replication_factor: default_replication_factor(),
            write_consistency_factor: default_write_consistency_factor(),
            read_fan_out_factor: None,
            on_disk_payload: default_on_disk_payload(),
            sparse_vectors: None,
        }
    }

    fn missing_vector_error(&self, vector_name: &VectorName) -> CollectionError {
        let mut available_names = vec![];

        match &self.vectors {
            VectorsConfig::Single(_) => {
                available_names.push(DEFAULT_VECTOR_NAME.to_owned());
            }
            VectorsConfig::Multi(vectors) => {
                for name in vectors.keys() {
                    available_names.push(name.clone());
                }
            }
        }

        if let Some(sparse_vectors) = &self.sparse_vectors {
            for name in sparse_vectors.keys() {
                available_names.push(name.clone());
            }
        }

        if available_names.is_empty() {
            CollectionError::BadInput {
                description: "Vectors are not configured in this collection".into(),
            }
        } else if available_names == vec![DEFAULT_VECTOR_NAME] {
            CollectionError::BadInput {
                description: format!(
                    "Vector with name {vector_name} is not configured in this collection"
                ),
            }
        } else {
            let available_names = available_names.join(", ");
            if vector_name == DEFAULT_VECTOR_NAME {
                return CollectionError::BadInput {
                    description: format!(
                        "Collection requires specified vector name in the request, available names: {available_names}"
                    ),
                };
            }

            CollectionError::BadInput {
                description: format!(
                    "Vector with name `{vector_name}` is not configured in this collection, available names: {available_names}"
                ),
            }
        }
    }

    pub fn get_distance(&self, vector_name: &VectorName) -> CollectionResult<Distance> {
        match self.vectors.get_params(vector_name) {
            Some(params) => Ok(params.distance),
            None => {
                if let Some(sparse_vectors) = &self.sparse_vectors
                    && let Some(_params) = sparse_vectors.get(vector_name)
                {
                    return Ok(Distance::Dot);
                }
                Err(self.missing_vector_error(vector_name))
            }
        }
    }

    fn get_vector_params_mut(
        &mut self,
        vector_name: &VectorName,
    ) -> CollectionResult<&mut VectorParams> {
        self.vectors
            .get_params_mut(vector_name)
            .ok_or_else(|| CollectionError::BadInput {
                description: if vector_name == DEFAULT_VECTOR_NAME {
                    "Default vector params are not specified in config".into()
                } else {
                    format!("Vector params for {vector_name} are not specified in config")
                },
            })
    }

    pub fn get_sparse_vector_params_opt(
        &self,
        vector_name: &VectorName,
    ) -> Option<&SparseVectorParams> {
        self.sparse_vectors
            .as_ref()
            .and_then(|sparse_vectors| sparse_vectors.get(vector_name))
    }

    pub fn get_sparse_vector_params_mut(
        &mut self,
        vector_name: &VectorName,
    ) -> CollectionResult<&mut SparseVectorParams> {
        self.sparse_vectors
            .as_mut()
            .ok_or_else(|| CollectionError::BadInput {
                description: format!(
                    "Sparse vector `{vector_name}` is not specified in collection config"
                ),
            })?
            .get_mut(vector_name)
            .ok_or_else(|| CollectionError::BadInput {
                description: format!(
                    "Sparse vector `{vector_name}` is not specified in collection config"
                ),
            })
    }

    /// Update collection vectors from the given update vectors config
    pub fn update_vectors_from_diff(
        &mut self,
        update_vectors_diff: &VectorsConfigDiff,
    ) -> CollectionResult<()> {
        for (vector_name, update_params) in update_vectors_diff.0.iter() {
            let vector_params = self.get_vector_params_mut(vector_name)?;
            let VectorParamsDiff {
                hnsw_config,
                quantization_config,
                on_disk,
            } = update_params.clone();

            if let Some(hnsw_diff) = hnsw_config {
                if let Some(existing_hnsw) = &vector_params.hnsw_config {
                    vector_params.hnsw_config = Some(existing_hnsw.update(&hnsw_diff));
                } else {
                    vector_params.hnsw_config = Some(hnsw_diff);
                }
            }

            if let Some(quantization_diff) = quantization_config {
                vector_params.quantization_config = match quantization_diff.clone() {
                    QuantizationConfigDiff::Scalar(scalar) => {
                        Some(QuantizationConfig::Scalar(scalar))
                    }
                    QuantizationConfigDiff::Product(product) => {
                        Some(QuantizationConfig::Product(product))
                    }
                    QuantizationConfigDiff::Binary(binary) => {
                        Some(QuantizationConfig::Binary(binary))
                    }
                    QuantizationConfigDiff::Disabled(_) => None,
                }
            }

            if let Some(on_disk) = on_disk {
                vector_params.on_disk = Some(on_disk);
            }
        }
        Ok(())
    }

    /// Update collection vectors from the given update vectors config
    pub fn update_sparse_vectors_from_other(
        &mut self,
        update_vectors: &SparseVectorsConfig,
    ) -> CollectionResult<()> {
        for (vector_name, update_params) in update_vectors.0.iter() {
            let sparse_vector_params = self.get_sparse_vector_params_mut(vector_name)?;
            let SparseVectorParams { index, modifier } = update_params.clone();

            if let Some(modifier) = modifier {
                sparse_vector_params.modifier = Some(modifier);
            }

            if let Some(index) = index {
                if let Some(existing_index) = &mut sparse_vector_params.index {
                    existing_index.update_from_other(index);
                } else {
                    sparse_vector_params.index.replace(index);
                }
            }
        }
        Ok(())
    }

    /// Convert into unoptimized named vector data configs
    ///
    /// It is the job of the segment optimizer to change this configuration with optimized settings
    /// based on threshold configurations.
    pub fn to_base_vector_data(
        &self,
        collection_quantization: Option<&QuantizationConfig>,
    ) -> CollectionResult<HashMap<VectorNameBuf, VectorDataConfig>> {
        let quantization_fn = |quantization_config: Option<&QuantizationConfig>| {
            quantization_config
                // Only if there is no `quantization_config` we may start using `collection_quantization` (to avoid mixing quantizations between segments)
                .or(collection_quantization)
                .filter(|c| c.supports_appendable())
                .cloned()
        };

        Ok(self
            .vectors
            .params_iter()
            .map(|(name, params)| {
                (
                    name.into(),
                    VectorDataConfig {
                        size: params.size.get() as usize,
                        distance: params.distance,
                        // Plain (disabled) index
                        index: Indexes::Plain {},
                        // Quantizaton config in appendable segment if runtime feature flag is set
                        quantization_config: common::flags::feature_flags()
                            .appendable_quantization
                            .then(|| quantization_fn(params.quantization_config.as_ref()))
                            .flatten(),
                        // Default to in memory storage
                        storage_type: if params.on_disk.unwrap_or_default() {
                            VectorStorageType::ChunkedMmap
                        } else {
                            VectorStorageType::InRamChunkedMmap
                        },
                        multivector_config: params.multivector_config,
                        datatype: params.datatype.map(VectorStorageDatatype::from),
                    },
                )
            })
            .collect())
    }

    /// Convert into unoptimized sparse vector data configs
    ///
    /// It is the job of the segment optimizer to change this configuration with optimized settings
    /// based on threshold configurations.
    pub fn to_sparse_vector_data(
        &self,
    ) -> CollectionResult<HashMap<VectorNameBuf, SparseVectorDataConfig>> {
        if let Some(sparse_vectors) = &self.sparse_vectors {
            sparse_vectors
                .iter()
                .map(|(name, params)| {
                    Ok((
                        name.clone(),
                        SparseVectorDataConfig {
                            index: SparseIndexConfig {
                                full_scan_threshold: params
                                    .index
                                    .and_then(|index| index.full_scan_threshold),
                                index_type: SparseIndexType::MutableRam,
                                datatype: params
                                    .index
                                    .and_then(|index| index.datatype)
                                    .map(VectorStorageDatatype::from),
                            },
                            storage_type: params.storage_type(),
                        },
                    ))
                })
                .collect()
        } else {
            Ok(Default::default())
        }
    }

    /// Convert into unoptimized segment config
    ///
    /// It is the job of the segment optimizer to change this configuration with optimized settings
    /// based on threshold configurations.
    pub fn to_base_segment_config(
        &self,
        collection_quantization: Option<&QuantizationConfig>,
    ) -> CollectionResult<SegmentConfig> {
        let vector_data = self
            .to_base_vector_data(collection_quantization)
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to source dense vector configuration from collection parameters: {err:?}"
                ))
            })?;

        let sparse_vector_data = self.to_sparse_vector_data().map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to source sparse vector configuration from collection parameters: {err:?}"
            ))
        })?;

        let payload_storage_type = self.payload_storage_type();

        let segment_config = SegmentConfig {
            vector_data,
            sparse_vector_data,
            payload_storage_type,
        };

        Ok(segment_config)
    }
}
