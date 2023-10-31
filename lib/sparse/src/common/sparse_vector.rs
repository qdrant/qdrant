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
