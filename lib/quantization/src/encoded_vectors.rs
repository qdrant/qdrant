use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::TBool;
use common::types::PointOffsetType;
use memory::mmap_type::MmapFlusher;
use serde::{Deserialize, Serialize};

use crate::EncodingError;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceType {
    Dot,
    L1,
    L2,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VectorParameters {
    pub dim: usize,
    pub distance_type: DistanceType,
    pub invert: bool,

    // Deprecated, use `EncodedVectors::vectors_count` from quantization instead.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "count")]
    pub deprecated_count: Option<usize>,
}

pub trait EncodedVectors: Sized {
    type EncodedQuery;

    fn load(
        data_path: &Path,
        meta_path: &Path,
        vector_parameters: &VectorParameters,
    ) -> std::io::Result<Self>;

    fn is_on_disk(&self) -> bool;

    fn encode_query(&self, query: &[f32]) -> Self::EncodedQuery;

    fn score_point(
        &self,
        query: &Self::EncodedQuery,
        i: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32;

    fn score_internal(
        &self,
        i: PointOffsetType,
        j: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> f32;

    /// Return size in bytes of a quantized vector
    fn quantized_vector_size(&self) -> usize;

    /// Construct a query from stored vector, so it can be used for scoring.
    /// Some implementations may not support this, in which case they should return `None`.
    fn encode_internal_vector(&self, id: PointOffsetType) -> Option<Self::EncodedQuery>;

    fn push_vector(
        &mut self,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn vectors_count(&self) -> usize;

    fn flusher(&self) -> MmapFlusher;

    type SupportsBytes: TBool;
    fn score_bytes(
        &self,
        enabled: Self::SupportsBytes,
        query: &Self::EncodedQuery,
        bytes: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32;
}

impl DistanceType {
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceType::Dot => a.iter().zip(b).map(|(a, b)| a * b).sum(),
            DistanceType::L1 => a.iter().zip(b).map(|(a, b)| (a - b).abs()).sum(),
            DistanceType::L2 => a.iter().zip(b).map(|(a, b)| (a - b) * (a - b)).sum(),
        }
    }
}

pub(crate) fn validate_vector_parameters<'a>(
    data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    vector_parameters: &VectorParameters,
) -> Result<(), EncodingError> {
    let mut count = 0;
    for vector in data {
        let vector = vector.as_ref();
        if vector.len() != vector_parameters.dim {
            return Err(EncodingError::ArgumentsError(format!(
                "Vector length {} does not match vector parameters dim {}",
                vector.len(),
                vector_parameters.dim
            )));
        }
        count += 1;
    }
    if let Some(vectors_count) = vector_parameters.deprecated_count
        && count != vectors_count
    {
        return Err(EncodingError::ArgumentsError(format!(
            "Vector count {count} does not match vector parameters count {vectors_count}"
        )));
    }
    Ok(())
}
