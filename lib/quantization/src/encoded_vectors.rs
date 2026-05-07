use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::MmapFlusher;
use common::typelevel::TBool;
use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};

use crate::EncodingError;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceType {
    // Warning!
    // Old Qdrant versions used `Dot` for both Cosine and Dot (since their implementations were equal).
    // However, TurboQuant needs to know the exact distance type and can't treat Cosine and Dot equally.
    // Because this distinction was introduced together with TQ, quantization storages created prior to
    // TQ might still store `Dot` even though the original vectors use `Cosine`.
    // Therefore, we can't rely on the exact type for any quantization storage other than TQ, and must *always* treat
    // Cosine and Dot the same for quantization types that were implemented prior to TQ.
    Cosine,

    Dot,
    L1,
    L2,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
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

    fn is_on_disk(&self) -> bool;

    fn encode_query(&self, query: &[f32]) -> Self::EncodedQuery;

    fn for_each_in_batch<F>(&self, offsets: &[PointOffsetType], callback: F)
    where
        F: FnMut(usize, &[u8]);

    fn score(
        &self,
        query: &Self::EncodedQuery,
        encoded_vector: &[u8],
        hw_counter: &HardwareCounterCell,
    ) -> f32;

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

    fn upsert_vector(
        &mut self,
        id: PointOffsetType,
        vector: &[f32],
        hw_counter: &HardwareCounterCell,
    ) -> std::io::Result<()>;

    fn vectors_count(&self) -> usize;

    fn flusher(&self) -> MmapFlusher;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

    /// Additional heap memory used by this encoded vectors instance
    /// beyond what's tracked in files (storage heap + metadata).
    fn heap_size_bytes(&self) -> usize {
        0
    }

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
            DistanceType::Dot | DistanceType::Cosine => a.iter().zip(b).map(|(a, b)| a * b).sum(),
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
