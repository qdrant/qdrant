use std::path::{Path, PathBuf};

use bitvec::prelude::BitSlice;
use serde::{Deserialize, Serialize};

use crate::common::file_operations::{atomic_save_json, read_json};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{Distance, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::scalar_quantized::ScalarQuantizedVectors;
use crate::vector_storage::quantized::scalar_quantized_mmap_storage::{
    create_scalar_quantized_vectors_mmap, load_scalar_quantized_vectors_mmap, QuantizedMmapStorage,
};
use crate::vector_storage::quantized::scalar_quantized_ram_storage::{
    create_scalar_quantized_vectors_ram, load_scalar_quantized_vectors_ram,
};
use crate::vector_storage::RawScorer;

pub const QUANTIZED_CONFIG_PATH: &str = "quantized.config.json";

#[derive(Deserialize, Serialize, Clone)]
pub struct QuantizedVectorsConfig {
    pub quantization_config: QuantizationConfig,
    pub vector_parameters: quantization::VectorParameters,
}

pub enum QuantizedVectorStorageImpl {
    ScalarRam(ScalarQuantizedVectors<ChunkedVectors<u8>>),
    ScalarMmap(ScalarQuantizedVectors<QuantizedMmapStorage>),
}

pub struct QuantizedVectorsStorage {
    storage_impl: QuantizedVectorStorageImpl,
    config: QuantizedVectorsConfig,
    path: PathBuf,
}

pub trait QuantizedVectors: Send + Sync {
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitSlice,
    ) -> Box<dyn RawScorer + 'a>;

    fn save_to(&self, path: &Path) -> OperationResult<()>;

    /// List all files used by the quantized vectors storage
    fn files(&self) -> Vec<PathBuf>;
}

impl QuantizedVectors for QuantizedVectorsStorage {
    fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitSlice,
    ) -> Box<dyn RawScorer + 'a> {
        match &self.storage_impl {
            QuantizedVectorStorageImpl::ScalarRam(storage) => storage.raw_scorer(query, deleted),
            QuantizedVectorStorageImpl::ScalarMmap(storage) => storage.raw_scorer(query, deleted),
        }
    }

    fn save_to(&self, path: &Path) -> OperationResult<()> {
        match &self.storage_impl {
            QuantizedVectorStorageImpl::ScalarRam(storage) => storage.save_to(path),
            QuantizedVectorStorageImpl::ScalarMmap(storage) => storage.save_to(path),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut result = vec![self.path.join(QUANTIZED_CONFIG_PATH)];
        let storage_files = match &self.storage_impl {
            QuantizedVectorStorageImpl::ScalarRam(storage) => storage.files(),
            QuantizedVectorStorageImpl::ScalarMmap(storage) => storage.files(),
        };

        result.extend(storage_files.into_iter().map(|file| self.path.join(file)));
        result
    }
}

impl QuantizedVectorsStorage {
    fn check_use_ram_quantization_storage(
        config: &ScalarQuantizationConfig,
        on_disk_vector_storage: bool,
    ) -> bool {
        !on_disk_vector_storage || config.always_ram == Some(true)
    }

    fn construct_vector_parameters(
        distance: Distance,
        dim: usize,
        count: usize,
    ) -> quantization::VectorParameters {
        quantization::VectorParameters {
            dim,
            count,
            distance_type: match distance {
                Distance::Cosine => quantization::DistanceType::Dot,
                Distance::Euclid => quantization::DistanceType::L2,
                Distance::Dot => quantization::DistanceType::Dot,
            },
            invert: distance == Distance::Euclid,
        }
    }

    pub fn create<'a>(
        vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
        quantization_config: &QuantizationConfig,
        distance: Distance,
        dim: usize,
        count: usize,
        path: &Path,
        on_disk_vector_storage: bool,
    ) -> OperationResult<Self> {
        let vector_parameters = Self::construct_vector_parameters(distance, dim, count);

        let quantized_storage = match quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_config,
            }) => {
                let in_ram =
                    Self::check_use_ram_quantization_storage(scalar_config, on_disk_vector_storage);
                if in_ram {
                    let storage = create_scalar_quantized_vectors_ram(
                        vectors,
                        scalar_config,
                        &vector_parameters,
                        distance,
                    )?;
                    QuantizedVectorStorageImpl::ScalarRam(storage)
                } else {
                    let storage = create_scalar_quantized_vectors_mmap(
                        vectors,
                        scalar_config,
                        &vector_parameters,
                        path,
                        distance,
                    )?;
                    QuantizedVectorStorageImpl::ScalarMmap(storage)
                }
            }
        };

        let quantized_vectors_config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
        };

        let quantized_vectors = QuantizedVectorsStorage {
            storage_impl: quantized_storage,
            config: quantized_vectors_config,
            path: path.to_path_buf(),
        };

        quantized_vectors.save_to(path)?;
        atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), &quantized_vectors.config)?;
        Ok(quantized_vectors)
    }

    pub fn check_exists(path: &Path) -> bool {
        path.join(QUANTIZED_CONFIG_PATH).exists()
    }

    pub fn load(
        data_path: &Path,
        on_disk_vector_storage: bool,
        distance: Distance,
    ) -> OperationResult<Self> {
        let config: QuantizedVectorsConfig = read_json(&data_path.join(QUANTIZED_CONFIG_PATH))?;
        let quantized_store = match &config.quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_u8_config,
            }) => {
                let is_ram = Self::check_use_ram_quantization_storage(
                    scalar_u8_config,
                    on_disk_vector_storage,
                );
                if is_ram {
                    let storage = load_scalar_quantized_vectors_ram(
                        data_path,
                        &config.vector_parameters,
                        distance,
                    )?;
                    QuantizedVectorStorageImpl::ScalarRam(storage)
                } else {
                    let storage = load_scalar_quantized_vectors_mmap(
                        data_path,
                        &config.vector_parameters,
                        distance,
                    )?;
                    QuantizedVectorStorageImpl::ScalarMmap(storage)
                }
            }
        };

        Ok(QuantizedVectorsStorage {
            storage_impl: quantized_store,
            config,
            path: data_path.to_path_buf(),
        })
    }
}
