use std::collections::HashMap;
use std::mem;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

#[pyclass(name = "SparseVectorDataConfig")]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySparseVectorDataConfig(pub SparseVectorDataConfig); // TODO

impl PySparseVectorDataConfig {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, SparseVectorDataConfig>
    where
        Self: TransparentWrapper<SparseVectorDataConfig>,
    {
        unsafe { mem::transmute(map) }
    }
}
