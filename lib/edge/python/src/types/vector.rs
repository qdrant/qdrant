use std::collections::HashMap;
use std::mem;

use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::data_types::vectors::{DenseVector, VectorStructInternal};
use segment::types::VectorNameBuf;
use shard::operations::point_ops::{VectorPersisted, VectorStructPersisted};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

#[derive(Clone, Debug, Into)]
pub struct PyVector(VectorStructPersisted);

impl<'py> FromPyObject<'py> for PyVector {
    fn extract_bound(vector: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Single(DenseVector),
            Multi(Vec<DenseVector>),
            Named(HashMap<VectorNameBuf, PyNamedVector>),
        }

        let vector = match vector.extract()? {
            Helper::Single(single) => VectorStructPersisted::Single(single),
            Helper::Multi(multi) => VectorStructPersisted::MultiDense(multi),
            Helper::Named(named) => {
                let named = PyNamedVector::into_rust_map(named);
                VectorStructPersisted::Named(named)
            }
        };

        Ok(Self(vector))
    }
}

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
struct PyNamedVector(VectorPersisted);

impl PyNamedVector {
    pub fn into_rust_map(
        vectors: HashMap<VectorNameBuf, Self>,
    ) -> HashMap<VectorNameBuf, VectorPersisted> {
        unsafe { mem::transmute(vectors) }
    }
}

impl<'py> FromPyObject<'py> for PyNamedVector {
    fn extract_bound(vector: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Dense(DenseVector),
            MultiDense(Vec<DenseVector>),
            Sparse(PySparseVector),
        }

        let vector = match vector.extract()? {
            Helper::Dense(dense) => VectorPersisted::Dense(dense),
            Helper::MultiDense(multi) => VectorPersisted::MultiDense(multi),
            Helper::Sparse(sparse) => {
                let sparse = PySparseVector::into_rust(sparse);
                VectorPersisted::Sparse(sparse)
            }
        };

        Ok(Self(vector))
    }
}

#[pyclass(name = "SparseVector")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PySparseVector(SparseVector);

impl PySparseVector {
    pub fn into_rust(self) -> SparseVector {
        unsafe { mem::transmute(self) }
    }
}

#[pymethods]
impl PySparseVector {
    #[new]
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Self {
        Self(SparseVector { indices, values })
    }

    #[getter]
    pub fn indices(&self) -> &[DimId] {
        &self.0.indices
    }

    #[getter]
    pub fn values(&self) -> &[DimWeight] {
        &self.0.values
    }
}

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyVectorInternal(pub VectorStructInternal);

impl PyVectorInternal {
    pub fn from_ref(vector: &VectorStructInternal) -> &Self {
        unsafe { mem::transmute(vector) }
    }
}

impl<'py> IntoPyObject<'py> for PyVectorInternal {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyVectorInternal {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match &self.0 {
            VectorStructInternal::Single(single) => single.into_bound_py_any(py),
            VectorStructInternal::MultiDense(_multi) => todo!(),
            VectorStructInternal::Named(_named) => todo!(),
        }
    }
}
