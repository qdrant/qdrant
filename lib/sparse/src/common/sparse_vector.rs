use serde::{Deserialize, Serialize};

use crate::common::types::{DimId, DimWeight};

#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize)]
pub struct SparseVector {
    pub indices: Vec<DimId>,
    pub values: Vec<DimWeight>,
}

impl SparseVector {
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> SparseVector {
        SparseVector { indices, values }
    }

    /// Score this vector against another vector using dot product.
    pub fn score(&self, other: &SparseVector) -> f32 {
        let mut score = 0.0;
        let mut i = 0;
        let mut j = 0;
        while i < self.indices.len() && j < other.indices.len() {
            match self.indices[i].cmp(&other.indices[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    score += self.values[i] * other.values[j];
                    i += 1;
                    j += 1;
                }
            }
        }
        score
    }
}
impl From<Vec<(i32, f64)>> for SparseVector {
    fn from(v: Vec<(i32, f64)>) -> Self {
        let mut indices = Vec::with_capacity(v.len());
        let mut values = Vec::with_capacity(v.len());
        for (i, w) in v {
            indices.push(i as u32);
            values.push(w as f32);
        }
        SparseVector { indices, values }
    }
}
