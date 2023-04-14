use std::path::{Path, PathBuf};

use bitvec::prelude::BitVec;
use quantization::{EncodedVectors, EncodedVectorsPQ, EncodedVectorsU8};
use serde::{Deserialize, Serialize};

use super::quantized_raw_scorer::QuantizedRawScorer;
use crate::common::file_operations::{atomic_save_json, read_json};
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::{Distance, ProductQuantization, QuantizationConfig, ScalarQuantization};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::quantized_mmap_storage::{
    QuantizedMmapStorage, QuantizedMmapStorageBuilder,
};
use crate::vector_storage::RawScorer;

pub const QUANTIZED_CONFIG_PATH: &str = "quantized.config.json";
pub const QUANTIZED_DATA_PATH: &str = "quantized.data";
pub const QUANTIZED_META_PATH: &str = "quantized.meta.json";

#[derive(Deserialize, Serialize, Clone)]
pub struct QuantizedVectorsConfig {
    pub quantization_config: QuantizationConfig,
    pub vector_parameters: quantization::VectorParameters,
}

pub enum QuantizedVectorStorage {
    ScalarRam(EncodedVectorsU8<ChunkedVectors<u8>>),
    ScalarMmap(EncodedVectorsU8<QuantizedMmapStorage>),
    PQRam(EncodedVectorsPQ<ChunkedVectors<u8>>),
    PQMmap(EncodedVectorsPQ<QuantizedMmapStorage>),
}

pub struct QuantizedVectors {
    storage_impl: QuantizedVectorStorage,
    config: QuantizedVectorsConfig,
    path: PathBuf,
    distance: Distance,
}

impl QuantizedVectors {
    pub fn raw_scorer<'a>(
        &'a self,
        query: &[VectorElementType],
        deleted: &'a BitVec,
    ) -> Box<dyn RawScorer + 'a> {
        let query = self
            .distance
            .preprocess_vector(query)
            .unwrap_or_else(|| query.to_vec());
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => {
                let query = storage.encode_query(&query);
                Box::new(QuantizedRawScorer {
                    query,
                    deleted,
                    quantized_data: storage,
                })
            }
            QuantizedVectorStorage::ScalarMmap(storage) => {
                let query = storage.encode_query(&query);
                Box::new(QuantizedRawScorer {
                    query,
                    deleted,
                    quantized_data: storage,
                })
            }
            QuantizedVectorStorage::PQRam(storage) => {
                let query = storage.encode_query(&query);
                Box::new(QuantizedRawScorer {
                    query,
                    deleted,
                    quantized_data: storage,
                })
            }
            QuantizedVectorStorage::PQMmap(storage) => {
                let query = storage.encode_query(&query);
                Box::new(QuantizedRawScorer {
                    query,
                    deleted,
                    quantized_data: storage,
                })
            }
        }
    }

    pub fn save_to(&self, path: &Path) -> OperationResult<()> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::ScalarMmap(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::PQRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::PQMmap(storage) => storage.save(&data_path, &meta_path)?,
        };
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut result = vec![self.path.join(QUANTIZED_CONFIG_PATH)];
        let storage_files: Vec<PathBuf> =
            vec![QUANTIZED_DATA_PATH.into(), QUANTIZED_META_PATH.into()];
        result.extend(storage_files.into_iter().map(|file| self.path.join(file)));
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create<'a>(
        vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
        quantization_config: &QuantizationConfig,
        distance: Distance,
        dim: usize,
        count: usize,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
    ) -> OperationResult<Self> {
        let vector_parameters = Self::construct_vector_parameters(distance, dim, count);

        let quantized_storage = match quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_config,
            }) => Self::crate_scalar(
                vectors,
                &vector_parameters,
                scalar_config,
                path,
                on_disk_vector_storage,
            )?,
            QuantizationConfig::Product(ProductQuantization { product: pq_config }) => {
                Self::crate_pq(
                    vectors,
                    &vector_parameters,
                    pq_config,
                    path,
                    on_disk_vector_storage,
                    max_threads,
                )?
            }
        };

        let quantized_vectors_config = QuantizedVectorsConfig {
            quantization_config: quantization_config.clone(),
            vector_parameters,
        };

        let quantized_vectors = QuantizedVectors {
            storage_impl: quantized_storage,
            config: quantized_vectors_config,
            path: path.to_path_buf(),
            distance,
        };

        quantized_vectors.save_to(path)?;
        atomic_save_json(&path.join(QUANTIZED_CONFIG_PATH), &quantized_vectors.config)?;
        Ok(quantized_vectors)
    }

    pub fn check_exists(path: &Path) -> bool {
        path.join(QUANTIZED_CONFIG_PATH).exists()
    }

    pub fn load(
        path: &Path,
        on_disk_vector_storage: bool,
        distance: Distance,
    ) -> OperationResult<Self> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        let config_path = path.join(QUANTIZED_CONFIG_PATH);
        let config: QuantizedVectorsConfig = read_json(&config_path)?;
        let quantized_store = match &config.quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                if Self::is_ram(scalar.always_ram, on_disk_vector_storage) {
                    QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::<ChunkedVectors<u8>>::load(
                        &data_path,
                        &meta_path,
                        &config.vector_parameters,
                    )?)
                } else {
                    QuantizedVectorStorage::ScalarMmap(
                        EncodedVectorsU8::<QuantizedMmapStorage>::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?,
                    )
                }
            }
            QuantizationConfig::Product(ProductQuantization { product: pq }) => {
                if Self::is_ram(pq.always_ram, on_disk_vector_storage) {
                    QuantizedVectorStorage::PQRam(EncodedVectorsPQ::<ChunkedVectors<u8>>::load(
                        &data_path,
                        &meta_path,
                        &config.vector_parameters,
                    )?)
                } else {
                    QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::<QuantizedMmapStorage>::load(
                        &data_path,
                        &meta_path,
                        &config.vector_parameters,
                    )?)
                }
            }
        };

        Ok(QuantizedVectors {
            storage_impl: quantized_store,
            config,
            path: data_path,
            distance,
        })
    }

    fn crate_scalar<'a>(
        vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
        vector_parameters: &quantization::VectorParameters,
        scalar_config: &crate::types::ScalarQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let quantized_vector_size =
            EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(vector_parameters);
        let in_ram = Self::is_ram(scalar_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
            Ok(QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                scalar_config.quantile,
            )?))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                vector_parameters.count,
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::ScalarMmap(
                EncodedVectorsU8::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    scalar_config.quantile,
                )?,
            ))
        }
    }

    fn crate_pq<'a>(
        vectors: impl IntoIterator<Item = &'a [f32]> + Clone,
        vector_parameters: &quantization::VectorParameters,
        pq_config: &crate::types::ProductQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
    ) -> OperationResult<QuantizedVectorStorage> {
        let quantized_vector_size =
            EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
                pq_config.bucket_size,
            );
        let in_ram = Self::is_ram(pq_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
            Ok(QuantizedVectorStorage::PQRam(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                pq_config.bucket_size,
                max_threads,
            )?))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                vector_parameters.count,
                quantized_vector_size,
            )?;
            Ok(QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                pq_config.bucket_size,
                max_threads,
            )?))
        }
    }

    fn is_ram(always_ram: Option<bool>, on_disk_vector_storage: bool) -> bool {
        !on_disk_vector_storage || always_ram == Some(true)
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
}
