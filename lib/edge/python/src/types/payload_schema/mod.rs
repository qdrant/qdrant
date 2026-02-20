pub mod text_index;

use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::data_types::index::*;
use segment::types::{PayloadSchemaParams, PayloadSchemaType};

pub use self::text_index::*;
use crate::repr::*;

#[pyclass(name = "PayloadSchemaType", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyPayloadSchemaType {
    Keyword,
    Integer,
    Float,
    Geo,
    Text,
    Bool,
    Datetime,
    Uuid,
}

impl Repr for PyPayloadSchemaType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Keyword => "Keyword",
            Self::Integer => "Integer",
            Self::Float => "Float",
            Self::Geo => "Geo",
            Self::Text => "Text",
            Self::Bool => "Bool",
            Self::Datetime => "Datetime",
            Self::Uuid => "Uuid",
        };

        f.simple_enum::<Self>(repr)
    }
}

impl From<PayloadSchemaType> for PyPayloadSchemaType {
    fn from(schema_type: PayloadSchemaType) -> Self {
        match schema_type {
            PayloadSchemaType::Keyword => PyPayloadSchemaType::Keyword,
            PayloadSchemaType::Integer => PyPayloadSchemaType::Integer,
            PayloadSchemaType::Float => PyPayloadSchemaType::Float,
            PayloadSchemaType::Geo => PyPayloadSchemaType::Geo,
            PayloadSchemaType::Text => PyPayloadSchemaType::Text,
            PayloadSchemaType::Bool => PyPayloadSchemaType::Bool,
            PayloadSchemaType::Datetime => PyPayloadSchemaType::Datetime,
            PayloadSchemaType::Uuid => PyPayloadSchemaType::Uuid,
        }
    }
}

impl From<PyPayloadSchemaType> for PayloadSchemaType {
    fn from(schema_type: PyPayloadSchemaType) -> Self {
        match schema_type {
            PyPayloadSchemaType::Keyword => PayloadSchemaType::Keyword,
            PyPayloadSchemaType::Integer => PayloadSchemaType::Integer,
            PyPayloadSchemaType::Float => PayloadSchemaType::Float,
            PyPayloadSchemaType::Geo => PayloadSchemaType::Geo,
            PyPayloadSchemaType::Text => PayloadSchemaType::Text,
            PyPayloadSchemaType::Bool => PayloadSchemaType::Bool,
            PyPayloadSchemaType::Datetime => PayloadSchemaType::Datetime,
            PyPayloadSchemaType::Uuid => PayloadSchemaType::Uuid,
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPayloadSchemaParams(PayloadSchemaParams);

impl FromPyObject<'_, '_> for PyPayloadSchemaParams {
    type Error = PyErr;

    fn extract(schema_params: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Keyword(PyKeywordIndexParams),
            Integer(PyIntegerIndexParams),
            Float(PyFloatIndexParams),
            Geo(PyGeoIndexParams),
            Text(PyTextIndexParams),
            Bool(PyBoolIndexParams),
            Datetime(PyDatetimeIndexParams),
            Uuid(PyUuidIndexParams),
        }

        fn _variants(schema_params: PayloadSchemaParams) {
            match schema_params {
                PayloadSchemaParams::Keyword(_) => {}
                PayloadSchemaParams::Integer(_) => {}
                PayloadSchemaParams::Float(_) => {}
                PayloadSchemaParams::Geo(_) => {}
                PayloadSchemaParams::Text(_) => {}
                PayloadSchemaParams::Bool(_) => {}
                PayloadSchemaParams::Datetime(_) => {}
                PayloadSchemaParams::Uuid(_) => {}
            }
        }

        let schema_params = match schema_params.extract()? {
            Helper::Keyword(keyword) => PayloadSchemaParams::Keyword(keyword.into()),
            Helper::Integer(int) => PayloadSchemaParams::Integer(int.into()),
            Helper::Float(float) => PayloadSchemaParams::Float(float.into()),
            Helper::Geo(geo) => PayloadSchemaParams::Geo(geo.into()),
            Helper::Text(text) => PayloadSchemaParams::Text(text.into()),
            Helper::Bool(bool) => PayloadSchemaParams::Bool(bool.into()),
            Helper::Datetime(date_time) => PayloadSchemaParams::Datetime(date_time.into()),
            Helper::Uuid(uuid) => PayloadSchemaParams::Uuid(uuid.into()),
        };

        Ok(Self(schema_params))
    }
}

impl<'py> IntoPyObject<'py> for PyPayloadSchemaParams {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            PayloadSchemaParams::Keyword(keyword) => {
                PyKeywordIndexParams(keyword).into_bound_py_any(py)
            }
            PayloadSchemaParams::Integer(int) => PyIntegerIndexParams(int).into_bound_py_any(py),
            PayloadSchemaParams::Float(float) => PyFloatIndexParams(float).into_bound_py_any(py),
            PayloadSchemaParams::Geo(geo) => PyGeoIndexParams(geo).into_bound_py_any(py),
            PayloadSchemaParams::Text(text) => PyTextIndexParams(text).into_bound_py_any(py),
            PayloadSchemaParams::Bool(bool) => PyBoolIndexParams(bool).into_bound_py_any(py),
            PayloadSchemaParams::Datetime(date_time) => {
                PyDatetimeIndexParams(date_time).into_bound_py_any(py)
            }
            PayloadSchemaParams::Uuid(uuid) => PyUuidIndexParams(uuid).into_bound_py_any(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for &PyPayloadSchemaParams {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyPayloadSchemaParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            PayloadSchemaParams::Keyword(keyword) => PyKeywordIndexParams::wrap_ref(keyword).fmt(f),
            PayloadSchemaParams::Integer(int) => PyIntegerIndexParams::wrap_ref(int).fmt(f),
            PayloadSchemaParams::Float(float) => PyFloatIndexParams::wrap_ref(float).fmt(f),
            PayloadSchemaParams::Geo(geo) => PyGeoIndexParams::wrap_ref(geo).fmt(f),
            PayloadSchemaParams::Text(text) => PyTextIndexParams::wrap_ref(text).fmt(f),
            PayloadSchemaParams::Bool(bool) => PyBoolIndexParams::wrap_ref(bool).fmt(f),
            PayloadSchemaParams::Datetime(date_time) => {
                PyDatetimeIndexParams::wrap_ref(date_time).fmt(f)
            }
            PayloadSchemaParams::Uuid(uuid) => PyUuidIndexParams::wrap_ref(uuid).fmt(f),
        }
    }
}

#[pyclass(name = "KeywordIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyKeywordIndexParams(KeywordIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyKeywordIndexParams {
    #[new]
    #[pyo3(signature = (is_tenant = None, on_disk = None, enable_hnsw = None))]
    pub fn new(is_tenant: Option<bool>, on_disk: Option<bool>, enable_hnsw: Option<bool>) -> Self {
        Self(KeywordIndexParams {
            r#type: Default::default(),
            is_tenant,
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn is_tenant(&self) -> Option<bool> {
        self.0.is_tenant
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyKeywordIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let KeywordIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            is_tenant: _,
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "IntegerIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyIntegerIndexParams(IntegerIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyIntegerIndexParams {
    #[new]
    #[pyo3(signature = (lookup = None, range = None, is_principal = None, on_disk = None, enable_hnsw = None))]
    pub fn new(
        lookup: Option<bool>,
        range: Option<bool>,
        is_principal: Option<bool>,
        on_disk: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> Self {
        Self(IntegerIndexParams {
            r#type: Default::default(),
            lookup,
            range,
            is_principal,
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn lookup(&self) -> Option<bool> {
        self.0.lookup
    }

    #[getter]
    pub fn range(&self) -> Option<bool> {
        self.0.range
    }

    #[getter]
    pub fn is_principal(&self) -> Option<bool> {
        self.0.is_principal
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyIntegerIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let IntegerIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            lookup: _,
            range: _,
            is_principal: _,
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "FloatIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFloatIndexParams(FloatIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyFloatIndexParams {
    #[new]
    #[pyo3(signature = (is_principal = None, on_disk = None, enable_hnsw = None))]
    pub fn new(
        is_principal: Option<bool>,
        on_disk: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> Self {
        Self(FloatIndexParams {
            r#type: Default::default(),
            is_principal,
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn is_principal(&self) -> Option<bool> {
        self.0.is_principal
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyFloatIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let FloatIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            is_principal: _,
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "GeoIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyGeoIndexParams(GeoIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyGeoIndexParams {
    #[new]
    #[pyo3(signature = (on_disk = None, enable_hnsw = None))]
    pub fn new(on_disk: Option<bool>, enable_hnsw: Option<bool>) -> Self {
        Self(GeoIndexParams {
            r#type: Default::default(),
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyGeoIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let GeoIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "BoolIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyBoolIndexParams(BoolIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyBoolIndexParams {
    #[new]
    #[pyo3(signature = (on_disk = None, enable_hnsw = None))]
    pub fn new(on_disk: Option<bool>, enable_hnsw: Option<bool>) -> Self {
        Self(BoolIndexParams {
            r#type: Default::default(),
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyBoolIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let BoolIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "DatetimeIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyDatetimeIndexParams(DatetimeIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyDatetimeIndexParams {
    #[new]
    #[pyo3(signature = (is_principal = None, on_disk = None, enable_hnsw = None))]
    pub fn new(
        is_principal: Option<bool>,
        on_disk: Option<bool>,
        enable_hnsw: Option<bool>,
    ) -> Self {
        Self(DatetimeIndexParams {
            r#type: Default::default(),
            is_principal,
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn is_principal(&self) -> Option<bool> {
        self.0.is_principal
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyDatetimeIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let DatetimeIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            is_principal: _,
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "UuidIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyUuidIndexParams(UuidIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyUuidIndexParams {
    #[new]
    #[pyo3(signature = (is_tenant = None, on_disk = None, enable_hnsw = None))]
    pub fn new(is_tenant: Option<bool>, on_disk: Option<bool>, enable_hnsw: Option<bool>) -> Self {
        Self(UuidIndexParams {
            r#type: Default::default(),
            is_tenant,
            on_disk,
            enable_hnsw,
        })
    }

    #[getter]
    pub fn is_tenant(&self) -> Option<bool> {
        self.0.is_tenant
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyUuidIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let UuidIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            is_tenant: _,
            on_disk: _,
            enable_hnsw: _,
        } = self.0;
    }
}
