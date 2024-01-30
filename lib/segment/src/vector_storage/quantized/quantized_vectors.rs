use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::slice::BitSlice;
use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::{EncodedVectors, EncodedVectorsPQ, EncodedVectorsU8};
use serde::{Deserialize, Serialize};

use super::quantized_scorer_builder::QuantizedScorerBuilder;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::vector_utils::TrySetCapacityExact;
use crate::data_types::vectors::{QueryVector, VectorElementType};
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, CompressionRatio, Distance, ProductQuantization,
    ProductQuantizationConfig, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig,
};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::quantized::quantized_mmap_storage::{
    QuantizedMmapStorage, QuantizedMmapStorageBuilder,
};
use crate::vector_storage::{DenseVectorStorage, RawScorer, VectorStorage, VectorStorageEnum};

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
    BinaryRam(EncodedVectorsBin<ChunkedVectors<u8>>),
    BinaryMmap(EncodedVectorsBin<QuantizedMmapStorage>),
}

pub struct QuantizedVectors {
    storage_impl: QuantizedVectorStorage,
    config: QuantizedVectorsConfig,
    path: PathBuf,
    distance: Distance,
}

// TODO: Remove this once `quantization` supports f16.
macro_rules! convert_vectors_f32 {
    ($vectors:ident) => {
        #[cfg(not(feature = "use_f32"))]
        let vectors_vec = $vectors
            .map(|x| x.iter().map(|&x| x.to_f32()).collect::<Vec<f32>>())
            .collect::<Vec<Vec<f32>>>();
        #[cfg(not(feature = "use_f32"))]
        let $vectors = vectors_vec.iter().map(|x| x.as_slice());
    };
}

impl QuantizedVectors {
    pub fn default_rescoring(&self) -> bool {
        matches!(
            self.storage_impl,
            QuantizedVectorStorage::BinaryRam(_) | QuantizedVectorStorage::BinaryMmap(_)
        )
    }

    pub fn raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        point_deleted: &'a BitSlice,
        vec_deleted: &'a BitSlice,
        is_stopped: &'a AtomicBool,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        QuantizedScorerBuilder::new(
            &self.storage_impl,
            query,
            point_deleted,
            vec_deleted,
            is_stopped,
            &self.distance,
        )
        .build()
    }

    pub fn save_to(&self, path: &Path) -> OperationResult<()> {
        let data_path = path.join(QUANTIZED_DATA_PATH);
        let meta_path = path.join(QUANTIZED_META_PATH);
        match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::ScalarMmap(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::PQRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::PQMmap(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::BinaryRam(storage) => storage.save(&data_path, &meta_path)?,
            QuantizedVectorStorage::BinaryMmap(storage) => storage.save(&data_path, &meta_path)?,
        };
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![
            // Config files
            self.path.join(QUANTIZED_CONFIG_PATH),
            // Storage file
            self.path.join(QUANTIZED_DATA_PATH),
            // Meta file
            self.path.join(QUANTIZED_META_PATH),
        ]
    }

    pub fn create(
        vector_storage: &VectorStorageEnum,
        quantization_config: &QuantizationConfig,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        match vector_storage {
            VectorStorageEnum::DenseSimple(v) => {
                Self::create_impl(v, quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::Memmap(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::AppendableMemmap(v) => {
                Self::create_impl(v.as_ref(), quantization_config, path, max_threads, stopped)
            }
            VectorStorageEnum::SparseSimple(_) => Err(OperationError::WrongSparse),
        }
    }

    fn create_impl<TVectorStorage: DenseVectorStorage + Send + Sync>(
        vector_storage: &TVectorStorage,
        quantization_config: &QuantizationConfig,
        path: &Path,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let count = vector_storage.total_vector_count();
        let vectors = (0..count as PointOffsetType).map(|i| vector_storage.get_dense(i));
        let on_disk_vector_storage = vector_storage.is_on_disk();
        let distance = vector_storage.distance();
        let dim = vector_storage.vector_dim();

        let vector_parameters = Self::construct_vector_parameters(distance, dim, count);

        let quantized_storage = match quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization {
                scalar: scalar_config,
            }) => Self::create_scalar(
                vectors,
                &vector_parameters,
                scalar_config,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
            QuantizationConfig::Product(ProductQuantization { product: pq_config }) => {
                Self::create_pq(
                    vectors,
                    &vector_parameters,
                    pq_config,
                    path,
                    on_disk_vector_storage,
                    max_threads,
                    stopped,
                )?
            }
            QuantizationConfig::Binary(BinaryQuantization {
                binary: binary_config,
            }) => Self::create_binary(
                vectors,
                &vector_parameters,
                binary_config,
                path,
                on_disk_vector_storage,
                stopped,
            )?,
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

    pub fn config_exists(path: &Path) -> bool {
        path.join(QUANTIZED_CONFIG_PATH).exists()
    }

    pub fn load(vector_storage: &VectorStorageEnum, path: &Path) -> OperationResult<Self> {
        let on_disk_vector_storage = vector_storage.is_on_disk();
        let distance = vector_storage.distance();

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
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                if Self::is_ram(binary.always_ram, on_disk_vector_storage) {
                    QuantizedVectorStorage::BinaryRam(
                        EncodedVectorsBin::<ChunkedVectors<u8>>::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?,
                    )
                } else {
                    QuantizedVectorStorage::BinaryMmap(
                        EncodedVectorsBin::<QuantizedMmapStorage>::load(
                            &data_path,
                            &meta_path,
                            &config.vector_parameters,
                        )?,
                    )
                }
            }
        };

        Ok(QuantizedVectors {
            storage_impl: quantized_store,
            config,
            path: path.to_path_buf(),
            distance,
        })
    }

    fn create_scalar<'a>(
        vectors: impl Iterator<Item = &'a [VectorElementType]> + Clone,
        vector_parameters: &quantization::VectorParameters,
        scalar_config: &ScalarQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let quantized_vector_size =
            EncodedVectorsU8::<QuantizedMmapStorage>::get_quantized_vector_size(vector_parameters);
        let in_ram = Self::is_ram(scalar_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let mut storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
            storage_builder.try_set_capacity_exact(vector_parameters.count)?;
            convert_vectors_f32!(vectors);
            Ok(QuantizedVectorStorage::ScalarRam(EncodedVectorsU8::encode(
                vectors,
                storage_builder,
                vector_parameters,
                scalar_config.quantile,
                || stopped.load(Ordering::Relaxed),
            )?))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                vector_parameters.count,
                quantized_vector_size,
            )?;
            convert_vectors_f32!(vectors);
            Ok(QuantizedVectorStorage::ScalarMmap(
                EncodedVectorsU8::encode(
                    vectors,
                    storage_builder,
                    vector_parameters,
                    scalar_config.quantile,
                    || stopped.load(Ordering::Relaxed),
                )?,
            ))
        }
    }

    fn create_pq<'a>(
        vectors: impl Iterator<Item = &'a [VectorElementType]> + Clone + Send,
        vector_parameters: &quantization::VectorParameters,
        pq_config: &ProductQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let bucket_size = Self::get_bucket_size(pq_config.compression);
        let quantized_vector_size =
            EncodedVectorsPQ::<QuantizedMmapStorage>::get_quantized_vector_size(
                vector_parameters,
                bucket_size,
            );
        let in_ram = Self::is_ram(pq_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let mut storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
            storage_builder.try_set_capacity_exact(vector_parameters.count)?;
            convert_vectors_f32!(vectors);
            Ok(QuantizedVectorStorage::PQRam(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                bucket_size,
                max_threads,
                || stopped.load(Ordering::Relaxed),
            )?))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                vector_parameters.count,
                quantized_vector_size,
            )?;
            convert_vectors_f32!(vectors);
            Ok(QuantizedVectorStorage::PQMmap(EncodedVectorsPQ::encode(
                vectors,
                storage_builder,
                vector_parameters,
                bucket_size,
                max_threads,
                || stopped.load(Ordering::Relaxed),
            )?))
        }
    }

    fn create_binary<'a>(
        vectors: impl Iterator<Item = &'a [VectorElementType]> + Clone,
        vector_parameters: &quantization::VectorParameters,
        binary_config: &BinaryQuantizationConfig,
        path: &Path,
        on_disk_vector_storage: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<QuantizedVectorStorage> {
        let quantized_vector_size =
            EncodedVectorsBin::<QuantizedMmapStorage>::get_quantized_vector_size_from_params(
                vector_parameters,
            );
        let in_ram = Self::is_ram(binary_config.always_ram, on_disk_vector_storage);
        if in_ram {
            let mut storage_builder = ChunkedVectors::<u8>::new(quantized_vector_size);
            storage_builder.try_set_capacity_exact(vector_parameters.count)?;
            convert_vectors_f32!(vectors);
            Ok(QuantizedVectorStorage::BinaryRam(
                EncodedVectorsBin::encode(vectors, storage_builder, vector_parameters, || {
                    stopped.load(Ordering::Relaxed)
                })?,
            ))
        } else {
            let mmap_data_path = path.join(QUANTIZED_DATA_PATH);
            let storage_builder = QuantizedMmapStorageBuilder::new(
                mmap_data_path.as_path(),
                vector_parameters.count,
                quantized_vector_size,
            )?;
            convert_vectors_f32!(vectors);
            Ok(QuantizedVectorStorage::BinaryMmap(
                EncodedVectorsBin::encode(vectors, storage_builder, vector_parameters, || {
                    stopped.load(Ordering::Relaxed)
                })?,
            ))
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
                Distance::Manhattan => quantization::DistanceType::L1,
            },
            invert: distance == Distance::Euclid || distance == Distance::Manhattan,
        }
    }

    fn get_bucket_size(compression: CompressionRatio) -> usize {
        match compression {
            CompressionRatio::X4 => 1,
            CompressionRatio::X8 => 2,
            CompressionRatio::X16 => 4,
            CompressionRatio::X32 => 8,
            CompressionRatio::X64 => 16,
        }
    }
}
