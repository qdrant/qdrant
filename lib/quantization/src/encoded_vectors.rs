use std::path::Path;

use common::counter::hardware_counter::HardwareCounterCell;
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
    pub count: usize,
    pub distance_type: DistanceType,
    pub invert: bool,
}

pub trait EncodedVectors<TEncodedQuery: Sized>: Sized {
    fn save(&self, data_path: &Path, meta_path: &Path) -> std::io::Result<()>;

    fn load(
        data_path: &Path,
        meta_path: &Path,
        vector_parameters: &VectorParameters,
    ) -> std::io::Result<Self>;

    fn encode_query(&self, query: &[f32]) -> TEncodedQuery;

    fn score_point(&self, query: &TEncodedQuery, i: u32, hw_couter: &HardwareCounterCell) -> f32;

    fn score_internal(&self, i: u32, j: u32, hw_couter: &HardwareCounterCell) -> f32;
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
    if count != vector_parameters.count {
        return Err(EncodingError::ArgumentsError(format!(
            "Vector count {} does not match vector parameters count {}",
            count, vector_parameters.count
        )));
    }
    Ok(())
}
