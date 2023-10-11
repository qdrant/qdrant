use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::types::{DimId, DimWeight};

#[derive(Clone, Default, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct SparseVector {
    pub indices: Vec<DimId>,
    pub weights: Vec<DimWeight>,
}

impl SparseVector {
    pub fn new(indices: Vec<DimId>, weights: Vec<DimWeight>) -> SparseVector {
        SparseVector { indices, weights }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut coded = bincode::serialize(self).unwrap();
        coded.push(0);
        coded
    }

    pub fn decode(bytes: &[u8]) -> SparseVector {
        bincode::deserialize(&bytes[0..bytes.len() - 1]).unwrap()
    }

    pub fn score(&self, other: &SparseVector) -> f32 {
        let mut score = 0.0;
        let mut i = 0;
        let mut j = 0;
        while i < self.indices.len() && j < other.indices.len() {
            match self.indices[i].cmp(&other.indices[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    score += self.weights[i] * other.weights[j];
                    i += 1;
                    j += 1;
                }
            }
        }
        score
    }
}
