use std::fmt;

use serde::{Deserialize, Serialize};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{
    BinaryQuantization, ProductQuantization, QuantizationConfig, ScalarQuantization,
    TurboQuantization,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

pub const QUANTIZED_CONFIG_PATH: &str = "quantized.config.json";
pub const QUANTIZED_DATA_PATH: &str = "quantized.data";
pub const QUANTIZED_APPENDABLE_DATA_PATH: &str = "quantized_data";
pub const QUANTIZED_META_PATH: &str = "quantized.meta.json";
pub const QUANTIZED_OFFSETS_PATH: &str = "quantized.offsets.data";
pub const QUANTIZED_APPENDABLE_OFFSETS_PATH: &str = "quantized_offsets_data";

#[derive(Deserialize, Serialize, Clone)]
pub struct QuantizedVectorsConfig {
    pub quantization_config: QuantizationConfig,
    pub vector_parameters: quantization::VectorParameters,
    #[serde(default)]
    #[serde(skip_serializing_if = "QuantizedVectorsStorageType::is_immutable")]
    pub storage_type: QuantizedVectorsStorageType,
}

impl fmt::Debug for QuantizedVectorsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            quantization_config,
            vector_parameters: _,
            storage_type: _,
        } = self;
        f.debug_struct("QuantizedVectorsConfig")
            .field("quantization_config", quantization_config)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum QuantizedVectorsStorageType {
    #[default]
    Immutable,
    Mutable,
}

impl QuantizedVectorsStorageType {
    pub fn is_immutable(&self) -> bool {
        matches!(self, QuantizedVectorsStorageType::Immutable)
    }
}

/// Which concrete quantized storage variant a config maps to.
///
/// Computed once from the config (see [`QuantizedVectorsConfig::storage_kind`]) so the
/// read-only and read-write loaders can flat-match on it instead of re-deriving the
/// `quantization method × backend` decision in nested branches, keeping the two in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::vector_storage::quantized) enum QuantizedStorageKind {
    ScalarRam,
    ScalarMmap,
    PqRam,
    PqMmap,
    BinaryRam,
    BinaryMmap,
    BinaryChunked,
    TqRam,
    TqMmap,
    TqChunked,
}

impl QuantizedVectorsConfig {
    /// Size in bytes of a single quantized vector on disk, for this config.
    ///
    /// See [`QuantizedVectors::quantized_vector_size`].
    pub(in crate::vector_storage::quantized) fn quantized_vector_size(
        &self,
        is_multi: bool,
    ) -> usize {
        QuantizedVectors::quantized_vector_size(
            &self.quantization_config,
            &self.vector_parameters,
            is_multi,
        )
    }

    /// Whether this config should be materialized in RAM (vs. kept as a read-only mmap),
    /// given whether the source vector storage is on disk.
    pub(in crate::vector_storage::quantized) fn is_ram(
        &self,
        on_disk_vector_storage: bool,
    ) -> bool {
        let always_ram = match &self.quantization_config {
            QuantizationConfig::Scalar(ScalarQuantization { scalar }) => scalar.always_ram,
            QuantizationConfig::Product(ProductQuantization { product }) => product.always_ram,
            QuantizationConfig::Binary(BinaryQuantization { binary }) => binary.always_ram,
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => turbo.always_ram,
        };
        QuantizedVectors::is_ram(always_ram, on_disk_vector_storage)
    }

    /// Resolve which storage variant this config selects, given whether the source
    /// vector storage is on disk.
    ///
    /// This is the single place the `method × backend` decision lives, so the loaders
    /// only need a flat match over [`QuantizedStorageKind`]. The appendable (chunked)
    /// layout only exists for Binary/TurboQuant; Scalar/Product are always immutable.
    pub(in crate::vector_storage::quantized) fn storage_kind(
        &self,
        on_disk_vector_storage: bool,
    ) -> OperationResult<QuantizedStorageKind> {
        let mutable = !self.storage_type.is_immutable();
        let in_ram = self.is_ram(on_disk_vector_storage);
        let kind = match &self.quantization_config {
            QuantizationConfig::Scalar(_) => {
                if mutable {
                    return Err(OperationError::service_error(
                        "Mutable storage is not supported for Scalar Quantization",
                    ));
                }
                if in_ram {
                    QuantizedStorageKind::ScalarRam
                } else {
                    QuantizedStorageKind::ScalarMmap
                }
            }
            QuantizationConfig::Product(_) => {
                if mutable {
                    return Err(OperationError::service_error(
                        "Mutable storage is not supported for Product Quantization",
                    ));
                }
                if in_ram {
                    QuantizedStorageKind::PqRam
                } else {
                    QuantizedStorageKind::PqMmap
                }
            }
            QuantizationConfig::Binary(_) => {
                if mutable {
                    QuantizedStorageKind::BinaryChunked
                } else if in_ram {
                    QuantizedStorageKind::BinaryRam
                } else {
                    QuantizedStorageKind::BinaryMmap
                }
            }
            QuantizationConfig::Turbo(_) => {
                if mutable {
                    QuantizedStorageKind::TqChunked
                } else if in_ram {
                    QuantizedStorageKind::TqRam
                } else {
                    QuantizedStorageKind::TqMmap
                }
            }
        };
        Ok(kind)
    }
}
