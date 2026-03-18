use std::collections::HashMap;
use std::hash::Hash;
use std::mem;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use edge::ShardInfo;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::PayloadIndexInfo;

use crate::repr::*;
use crate::types::PyJsonPath;
use crate::types::payload_schema::*;

#[pyclass(name = "ShardInfo", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyShardInfo(pub ShardInfo);

#[pyclass_repr]
#[pymethods]
impl PyShardInfo {
    #[getter]
    pub fn segments_count(&self) -> usize {
        self.0.segments_count
    }

    #[getter]
    pub fn points_count(&self) -> usize {
        self.0.points_count
    }

    #[getter]
    pub fn indexed_vectors_count(&self) -> usize {
        self.0.indexed_vectors_count
    }

    #[getter]
    pub fn payload_schema(&self) -> &HashMap<PyJsonPath, PyPayloadIndexInfo> {
        PyPayloadIndexInfo::wrap_map_ref(&self.0.payload_schema)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyShardInfo {
    fn _getters(self) {
        // Every field should have a getter method
        let ShardInfo {
            segments_count: _,
            points_count: _,
            indexed_vectors_count: _,
            payload_schema: _,
        } = self.0;
    }
}

#[pyclass(name = "PayloadIndexInfo", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPayloadIndexInfo(PayloadIndexInfo);

impl PyPayloadIndexInfo {
    fn wrap_map_ref(
        map: &HashMap<JsonPath, PayloadIndexInfo>,
    ) -> &HashMap<PyJsonPath, PyPayloadIndexInfo>
    where
        PyJsonPath: TransparentWrapper<JsonPath> + Eq + Hash,
        Self: TransparentWrapper<PayloadIndexInfo>,
    {
        unsafe { mem::transmute(map) }
    }
}

#[pyclass_repr]
#[pymethods]
impl PyPayloadIndexInfo {
    #[getter]
    pub fn data_type(&self) -> PyPayloadSchemaType {
        PyPayloadSchemaType::from(self.0.data_type)
    }

    #[getter]
    pub fn params(&self) -> Option<&PyPayloadSchemaParams> {
        self.0.params.as_ref().map(PyPayloadSchemaParams::wrap_ref)
    }

    #[getter]
    pub fn points(&self) -> usize {
        self.0.points
    }
}

impl PyPayloadIndexInfo {
    fn _getters(self) {
        // Every field should have a getter method
        let PayloadIndexInfo {
            data_type: _,
            params: _,
            points: _,
        } = self.0;
    }
}

impl<'py> IntoPyObject<'py> for &PyPayloadIndexInfo {
    type Target = PyPayloadIndexInfo;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}
