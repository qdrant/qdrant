use std::collections::HashMap;
use std::{fmt, mem};

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::types::VectorNameBuf;
use shard::operations::point_ops::{VectorPersisted, VectorStructPersisted};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

use crate::repr::*;
use crate::type_hint::Alias;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyVector(VectorStructPersisted);

pub const VECTOR: Alias = Alias {
    name: "Vector",
    definition: VectorHelper::INPUT_TYPE,
};
pub const NAMED_VECTOR: Alias = Alias {
    name: "NamedVector",
    definition: NamedVectorHelper::INPUT_TYPE,
};

#[derive(FromPyObject)]
enum VectorHelper {
    Single(Vec<f32>),
    MultiDense(Vec<Vec<f32>>),
    Named(HashMap<VectorNameBuf, PyNamedVector>),
}

#[derive(FromPyObject)]
enum NamedVectorHelper {
    Dense(Vec<f32>),
    Sparse(PySparseVector),
    MultiDense(Vec<Vec<f32>>),
}

impl FromPyObject<'_, '_> for PyVector {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = VECTOR.hint();

    fn extract(vector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(vector: VectorStructPersisted) {
            match vector {
                VectorStructPersisted::Single(_) => {}
                VectorStructPersisted::MultiDense(_) => {}
                VectorStructPersisted::Named(_) => {}
            }
        }

        let vector = match vector.extract()? {
            VectorHelper::Single(single) => VectorStructPersisted::Single(single),
            VectorHelper::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
            VectorHelper::Named(named) => {
                VectorStructPersisted::Named(PyNamedVector::peel_map(named))
            }
        };

        Ok(Self(vector))
    }
}

impl<'py> IntoPyObject<'py> for PyVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = VECTOR.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = VECTOR.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            VectorStructPersisted::Single(single) => single.into_bound_py_any(py),
            VectorStructPersisted::MultiDense(multi) => multi.into_bound_py_any(py),
            VectorStructPersisted::Named(named) => {
                PyNamedVector::wrap_map_ref(named).into_bound_py_any(py)
            }
        }
    }
}

impl Repr for PyVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            VectorStructPersisted::Single(single) => single.fmt(f),
            VectorStructPersisted::MultiDense(multi) => multi.fmt(f),
            VectorStructPersisted::Named(named) => PyNamedVector::wrap_map_ref(named).fmt(f),
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyNamedVector(VectorPersisted);

impl PyNamedVector {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, VectorPersisted>
    where
        Self: TransparentWrapper<VectorPersisted>,
    {
        unsafe { mem::transmute(map) }
    }

    pub fn wrap_map_ref(map: &HashMap<String, VectorPersisted>) -> &HashMap<String, Self>
    where
        Self: TransparentWrapper<VectorPersisted>,
    {
        unsafe { mem::transmute(map) }
    }
}

impl FromPyObject<'_, '_> for PyNamedVector {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = NAMED_VECTOR.hint();

    fn extract(vector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(vector: VectorPersisted) {
            match vector {
                VectorPersisted::Dense(_) => {}
                VectorPersisted::Sparse(_) => {}
                VectorPersisted::MultiDense(_) => {}
            }
        }

        let vector = match vector.extract()? {
            NamedVectorHelper::Dense(dense) => VectorPersisted::Dense(dense),
            NamedVectorHelper::Sparse(sparse) => {
                VectorPersisted::Sparse(SparseVector::from(sparse))
            }
            NamedVectorHelper::MultiDense(multi) => VectorPersisted::MultiDense(multi),
        };

        Ok(Self(vector))
    }
}

impl<'py> IntoPyObject<'py> for PyNamedVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = NAMED_VECTOR.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyNamedVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = NAMED_VECTOR.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            VectorPersisted::Dense(dense) => dense.into_bound_py_any(py),
            VectorPersisted::Sparse(sparse) => PySparseVector(sparse.clone()).into_bound_py_any(py),
            VectorPersisted::MultiDense(multi) => multi.into_bound_py_any(py),
        }
    }
}

impl Repr for PyNamedVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            VectorPersisted::Dense(dense) => dense.fmt(f),
            VectorPersisted::Sparse(sparse) => PySparseVector::wrap_ref(sparse).fmt(f),
            VectorPersisted::MultiDense(multi) => multi.fmt(f),
        }
    }
}

/// A sparse vector representation.
#[pyclass(name = "SparseVector", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySparseVector(pub SparseVector);

#[pyclass_repr]
#[pymethods]
impl PySparseVector {
    /// Create a SparseVector.
    ///
    /// Args:
    ///     indices: Non-zero dimension indices.
    ///     values: Values at the non-zero dimensions.
    #[new]
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Self {
        Self(SparseVector { indices, values })
    }

    /// Non-zero dimension indices.
    #[getter]
    pub fn indices(&self) -> &[DimId] {
        self.0.indices.as_slice()
    }

    /// Values at non-zero dimensions.
    #[getter]
    pub fn values(&self) -> &[DimWeight] {
        self.0.values.as_slice()
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PySparseVector {
    fn _getters(self) {
        // Every field should have a getter method
        let SparseVector {
            indices: _,
            values: _,
        } = self.0;
    }
}
