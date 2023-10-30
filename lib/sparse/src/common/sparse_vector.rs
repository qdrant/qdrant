use serde::{Deserialize, Serialize};

use crate::common::types::{DimId, DimWeight};

#[derive(Debug, PartialEq, Clone, Default, Serialize, Deserialize)]
pub struct SparseVector {
    pub indices: Vec<DimId>,
    pub weights: Vec<DimWeight>,
}

impl SparseVector {
    pub fn new(indices: Vec<DimId>, weights: Vec<DimWeight>) -> SparseVector {
        SparseVector { indices, weights }
    }
}
impl From<Vec<(i32, f64)>> for SparseVector {
    fn from(v: Vec<(i32, f64)>) -> Self {
        let mut indices = Vec::with_capacity(v.len());
        let mut weights = Vec::with_capacity(v.len());
        for (i, w) in v {
            indices.push(i as u32);
            weights.push(w as f32);
        }
        SparseVector { indices, weights }
    }
}
