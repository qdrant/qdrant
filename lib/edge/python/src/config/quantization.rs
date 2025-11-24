use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

#[pyclass(name = "QuantizationConfig")]
#[derive(Clone, Debug, Into)]
pub struct PyQuantizationConfig(QuantizationConfig); // TODO
