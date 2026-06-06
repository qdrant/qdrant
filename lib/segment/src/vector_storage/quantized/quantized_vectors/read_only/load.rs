use std::path::Path;

use common::universal_io::{OkNotFound, UniversalRead, read_json_via};
use quantization::EncodedVectorsPQ;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;

use super::QuantizedVectorsRead;
use super::storage::QuantizedVectorStorageRead;
use crate::common::operation_error::OperationResult;
use crate::types::{
    BinaryQuantization, Distance, MultiVectorConfig, ProductQuantization, QuantizationConfig,
    ScalarQuantization, TurboQuantization, VectorStorageDatatype,
};
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorageChunkedRead, MultivectorOffsetsStorageMmap,
    MultivectorOffsetsStorageRam, QuantizedMultivectorStorage,
};
use crate::vector_storage::quantized::quantized_ram_storage::QuantizedRamStorage;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsConfig,
};

impl<S: UniversalRead> QuantizedVectorsRead<S> {
    /// Open existing quantized vectors read-only through the [`UniversalRead`] backend `S`.
    ///
    /// Returns `Ok(None)` when no quantization config is present at `path`. Every read —
    /// config, per-method metadata, quantized data and multivector offsets — goes through `S`;
    /// nothing is read with direct filesystem access and nothing is written. Both on-disk
    /// layouts are supported read-only: the immutable flat format and the appendable chunked
    /// format (the latter only produced by Binary/TurboQuant). Unlike
    /// [`QuantizedVectors::load`], this never creates or quantizes anything.
    ///
    /// `distance`, `datatype`, `multivector_config` and `on_disk_vector_storage` describe
    /// the original (source) vector storage this quantization was built for.
    pub fn open(
        fs: &S::Fs,
        path: &Path,
        distance: Distance,
        datatype: VectorStorageDatatype,
        multivector_config: Option<&MultiVectorConfig>,
        on_disk_vector_storage: bool,
    ) -> OperationResult<Option<Self>> {
        let config_path = QuantizedVectors::get_config_path(path);
        let config: Option<QuantizedVectorsConfig> =
            read_json_via(fs, &config_path).ok_not_found()?;
        let Some(config) = config else {
            return Ok(None);
        };

        let storage_impl = match multivector_config {
            Some(multivector_config) => Self::open_multi(
                fs,
                path,
                &config,
                multivector_config,
                on_disk_vector_storage,
            )?,
            None => Self::open_single(fs, path, &config, on_disk_vector_storage)?,
        };

        Ok(Some(Self::new(
            storage_impl,
            config,
            path.to_path_buf(),
            distance,
            datatype,
        )))
    }

    fn open_single(
        fs: &S::Fs,
        path: &Path,
        config: &QuantizedVectorsConfig,
        on_disk_vector_storage: bool,
    ) -> OperationResult<QuantizedVectorStorageRead<S>> {
        let data_path = QuantizedVectors::get_data_path(path, config.storage_type);
        let meta_path = QuantizedVectors::get_meta_path(path);
        let size = config.quantized_vector_size(false);

        match &config.quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                if QuantizedVectors::is_ram(scalar.always_ram, on_disk_vector_storage) {
                    let storage = QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::ScalarRam(
                        EncodedVectorsU8::load(fs, storage, &meta_path)?,
                    ))
                } else {
                    let storage = QuantizedStorage::<S>::from_file(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::ScalarMmap(
                        EncodedVectorsU8::load(fs, storage, &meta_path)?,
                    ))
                }
            }
            QuantizationConfig::Product(ProductQuantization { product }) => {
                if QuantizedVectors::is_ram(product.always_ram, on_disk_vector_storage) {
                    let storage = QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::PQRam(EncodedVectorsPQ::load(
                        fs, storage, &meta_path,
                    )?))
                } else {
                    let storage = QuantizedStorage::<S>::from_file(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::PQMmap(EncodedVectorsPQ::load(
                        fs, storage, &meta_path,
                    )?))
                }
            }
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                if !config.storage_type.is_immutable() {
                    let storage = QuantizedChunkedStorageRead::<S>::open(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::BinaryChunked(
                        EncodedVectorsBin::load(fs, storage, &meta_path)?,
                    ))
                } else if QuantizedVectors::is_ram(binary.always_ram, on_disk_vector_storage) {
                    let storage = QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::BinaryRam(
                        EncodedVectorsBin::load(fs, storage, &meta_path)?,
                    ))
                } else {
                    let storage = QuantizedStorage::<S>::from_file(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::BinaryMmap(
                        EncodedVectorsBin::load(fs, storage, &meta_path)?,
                    ))
                }
            }
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                if !config.storage_type.is_immutable() {
                    let storage = QuantizedChunkedStorageRead::<S>::open(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::TQChunked(
                        EncodedVectorsTQ::load(fs, storage, &meta_path)?,
                    ))
                } else if QuantizedVectors::is_ram(turbo.always_ram, on_disk_vector_storage) {
                    let storage = QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::TQRam(EncodedVectorsTQ::load(
                        fs, storage, &meta_path,
                    )?))
                } else {
                    let storage = QuantizedStorage::<S>::from_file(fs, &data_path, size)?;
                    Ok(QuantizedVectorStorageRead::TQMmap(EncodedVectorsTQ::load(
                        fs, storage, &meta_path,
                    )?))
                }
            }
        }
    }

    fn open_multi(
        fs: &S::Fs,
        path: &Path,
        config: &QuantizedVectorsConfig,
        multivector_config: &MultiVectorConfig,
        on_disk_vector_storage: bool,
    ) -> OperationResult<QuantizedVectorStorageRead<S>> {
        let data_path = QuantizedVectors::get_data_path(path, config.storage_type);
        let meta_path = QuantizedVectors::get_meta_path(path);
        let offsets_path = QuantizedVectors::get_offsets_path(path, config.storage_type);
        let dim = config.vector_parameters.dim;
        let size = config.quantized_vector_size(true);

        match &config.quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization { scalar }) => {
                if QuantizedVectors::is_ram(scalar.always_ram, on_disk_vector_storage) {
                    let inner = EncodedVectorsU8::load(
                        fs,
                        QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageRam::open::<S>(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::ScalarRamMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                } else {
                    let inner = EncodedVectorsU8::load(
                        fs,
                        QuantizedStorage::<S>::from_file(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageMmap::<S>::open(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::ScalarMmapMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                }
            }
            QuantizationConfig::Product(ProductQuantization { product }) => {
                if QuantizedVectors::is_ram(product.always_ram, on_disk_vector_storage) {
                    let inner = EncodedVectorsPQ::load(
                        fs,
                        QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageRam::open::<S>(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::PQRamMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                } else {
                    let inner = EncodedVectorsPQ::load(
                        fs,
                        QuantizedStorage::<S>::from_file(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageMmap::<S>::open(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::PQMmapMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                }
            }
            QuantizationConfig::Binary(BinaryQuantization { binary }) => {
                if !config.storage_type.is_immutable() {
                    let inner = EncodedVectorsBin::load(
                        fs,
                        QuantizedChunkedStorageRead::<S>::open(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets =
                        MultivectorOffsetsStorageChunkedRead::<S>::open(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::BinaryChunkedMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                } else if QuantizedVectors::is_ram(binary.always_ram, on_disk_vector_storage) {
                    let inner = EncodedVectorsBin::load(
                        fs,
                        QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageRam::open::<S>(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::BinaryRamMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                } else {
                    let inner = EncodedVectorsBin::load(
                        fs,
                        QuantizedStorage::<S>::from_file(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageMmap::<S>::open(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::BinaryMmapMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                }
            }
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                if !config.storage_type.is_immutable() {
                    let inner = EncodedVectorsTQ::load(
                        fs,
                        QuantizedChunkedStorageRead::<S>::open(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets =
                        MultivectorOffsetsStorageChunkedRead::<S>::open(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::TQChunkedMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                } else if QuantizedVectors::is_ram(turbo.always_ram, on_disk_vector_storage) {
                    let inner = EncodedVectorsTQ::load(
                        fs,
                        QuantizedRamStorage::from_file::<S>(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageRam::open::<S>(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::TQRamMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                } else {
                    let inner = EncodedVectorsTQ::load(
                        fs,
                        QuantizedStorage::<S>::from_file(fs, &data_path, size)?,
                        &meta_path,
                    )?;
                    let offsets = MultivectorOffsetsStorageMmap::<S>::open(fs, &offsets_path)?;
                    Ok(QuantizedVectorStorageRead::TQMmapMulti(
                        QuantizedMultivectorStorage::new(dim, inner, offsets, *multivector_config),
                    ))
                }
            }
        }
    }
}
