use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

#[pyclass(name = "SparseVectorDataConfig")]
#[derive(Copy, Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySparseVectorDataConfig(SparseVectorDataConfig); // TODO
