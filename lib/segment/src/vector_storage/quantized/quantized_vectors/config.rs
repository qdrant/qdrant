use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::QuantizationConfig;

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
        f.debug_struct("QuantizedVectorsConfig")
            .field("quantization_config", &self.quantization_config)
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
