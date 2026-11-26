//! Remote indexing: quantized vectors as codes plus encoder parameters, and back. Independent
//! of how the quantized vectors are stored: core serves each point's code, the indexer
//! rebuilds an in-RAM storage from them.

use std::borrow::Cow;
use std::path::Path;

use common::types::PointOffsetType;
use quantization::EncodedStorageBuilder as _;
use quantization::encoded_vectors_binary::EncodedVectorsBin;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;

use super::config::{QUANTIZED_DATA_PATH, QuantizedVectorsConfig, QuantizedVectorsStorageType};
use super::{QuantizedVectorStorage, QuantizedVectors};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::remote::wire::{QuantizerParameters, RemoteQuantization};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_ram_storage::{
    QuantizedRamStorage, QuantizedRamStorageBuilder,
};

impl QuantizedVectors {
    /// How the codes were encoded, for the quantizations whose codes a remote indexer can
    /// rebuild quantized vectors from: scalar, binary and TurboQuant. `None` for product
    /// quantization and for multivectors. Whether a build can do without the original vectors
    /// is a separate question, see [`Self::builds_from_codes`].
    pub fn remote_quantization(&self) -> Option<RemoteQuantization> {
        let parameters = match &self.storage_impl {
            QuantizedVectorStorage::ScalarRam(q) => QuantizerParameters::Scalar(q.parameters()),
            QuantizedVectorStorage::ScalarMmap(q) => QuantizerParameters::Scalar(q.parameters()),
            QuantizedVectorStorage::ScalarChunkedMmap(q) => {
                QuantizerParameters::Scalar(q.parameters())
            }
            QuantizedVectorStorage::BinaryRam(q) => QuantizerParameters::Binary(q.parameters()),
            QuantizedVectorStorage::BinaryMmap(q) => QuantizerParameters::Binary(q.parameters()),
            QuantizedVectorStorage::BinaryChunkedMmap(q) => {
                QuantizerParameters::Binary(q.parameters())
            }
            QuantizedVectorStorage::TQRam(q) => QuantizerParameters::Turbo(q.parameters()),
            QuantizedVectorStorage::TQMmap(q) => QuantizerParameters::Turbo(q.parameters()),
            QuantizedVectorStorage::TQChunkedMmap(q) => QuantizerParameters::Turbo(q.parameters()),
            QuantizedVectorStorage::PQRam(_)
            | QuantizedVectorStorage::PQMmap(_)
            | QuantizedVectorStorage::PQChunkedMmap(_)
            | QuantizedVectorStorage::ScalarRamMulti(_)
            | QuantizedVectorStorage::ScalarMmapMulti(_)
            | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
            | QuantizedVectorStorage::PQRamMulti(_)
            | QuantizedVectorStorage::PQMmapMulti(_)
            | QuantizedVectorStorage::PQChunkedMmapMulti(_)
            | QuantizedVectorStorage::BinaryRamMulti(_)
            | QuantizedVectorStorage::BinaryMmapMulti(_)
            | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
            | QuantizedVectorStorage::TQRamMulti(_)
            | QuantizedVectorStorage::TQMmapMulti(_)
            | QuantizedVectorStorage::TQChunkedMmapMulti(_) => return None,
        };
        Some(RemoteQuantization {
            config: self.config.quantization_config.clone(),
            parameters,
        })
    }

    /// The code of `id`, as served to a remote indexer. Only for storages with a
    /// [`Self::remote_quantization`].
    pub fn remote_code(&self, id: PointOffsetType) -> Cow<'_, [u8]> {
        self.get_quantized_vector(id)
    }
}

/// Rebuilds quantized vectors from the codes core serves, in point order, on the indexer.
pub struct RemoteQuantizedBuilder {
    config: QuantizedVectorsConfig,
    parameters: QuantizerParameters,
    code_size: usize,
    count: usize,
    storage: QuantizedRamStorageBuilder,
    dir: std::path::PathBuf,
}

impl RemoteQuantizedBuilder {
    /// A builder for `count` codes of `quantization`, for vectors of `dim` and `distance`.
    /// Scratch files go to `dir`.
    pub fn new(
        quantization: &RemoteQuantization,
        distance: Distance,
        dim: usize,
        count: usize,
        dir: &Path,
    ) -> OperationResult<Self> {
        let RemoteQuantization { config, parameters } = quantization;
        let parameters_match = match (config, parameters) {
            (crate::types::QuantizationConfig::Scalar(_), QuantizerParameters::Scalar(_))
            | (crate::types::QuantizationConfig::Binary(_), QuantizerParameters::Binary(_))
            | (crate::types::QuantizationConfig::Turbo(_), QuantizerParameters::Turbo(_)) => true,
            (crate::types::QuantizationConfig::Product(_), _)
            | (crate::types::QuantizationConfig::Scalar(_), _)
            | (crate::types::QuantizationConfig::Binary(_), _)
            | (crate::types::QuantizationConfig::Turbo(_), _) => false,
        };
        if !parameters_match {
            return Err(OperationError::service_error(
                "quantization parameters do not match the quantization config",
            ));
        }
        let storage_type = QuantizedVectorsStorageType::Immutable;
        let vector_parameters = QuantizedVectors::construct_vector_parameters(
            config,
            distance,
            dim,
            count,
            storage_type,
        );
        let code_size = QuantizedVectors::quantized_vector_size(config, &vector_parameters, false);
        fs_err::create_dir_all(dir)?;
        let storage =
            QuantizedRamStorageBuilder::new(&dir.join(QUANTIZED_DATA_PATH), count, code_size)?;
        Ok(Self {
            config: QuantizedVectorsConfig {
                quantization_config: config.clone(),
                vector_parameters,
                storage_type,
            },
            parameters: parameters.clone(),
            code_size,
            count,
            storage,
            dir: dir.to_path_buf(),
        })
    }

    /// Size of every code, in bytes.
    pub fn code_size(&self) -> usize {
        self.code_size
    }

    /// Append the code of the next point. Points without a vector get any code of
    /// [`Self::code_size`], and are deleted in the vector storage.
    pub fn push(&mut self, code: &[u8]) -> OperationResult<()> {
        if code.len() != self.code_size {
            return Err(OperationError::service_error(format!(
                "quantized code of {} bytes, expected {}",
                code.len(),
                self.code_size,
            )));
        }
        self.storage.push_vector_data(code)?;
        Ok(())
    }

    /// The quantized vectors, once every point has its code.
    pub fn finish(
        self,
        distance: Distance,
        datatype: VectorStorageDatatype,
    ) -> OperationResult<QuantizedVectors> {
        let Self {
            config,
            parameters,
            code_size: _,
            count,
            storage,
            dir,
        } = self;
        if storage.vectors.len() != count {
            return Err(OperationError::service_error(format!(
                "{} quantized codes, expected {count}",
                storage.vectors.len(),
            )));
        }
        let storage: QuantizedRamStorage = storage.build()?;
        let vector_parameters = config.vector_parameters;
        let storage_impl = match parameters {
            QuantizerParameters::Scalar(parameters) => QuantizedVectorStorage::ScalarRam(
                EncodedVectorsU8::from_parameters(storage, vector_parameters, parameters)?,
            ),
            QuantizerParameters::Binary(parameters) => QuantizedVectorStorage::BinaryRam(
                EncodedVectorsBin::from_parameters(storage, vector_parameters, parameters)?,
            ),
            QuantizerParameters::Turbo(parameters) => QuantizedVectorStorage::TQRam(
                EncodedVectorsTQ::from_parameters(storage, vector_parameters, parameters)?,
            ),
        };
        Ok(QuantizedVectors {
            storage_impl,
            config,
            path: dir,
            distance,
            datatype,
        })
    }
}
