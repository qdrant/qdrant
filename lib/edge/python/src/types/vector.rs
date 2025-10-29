use std::collections::HashMap;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::data_types::vectors::*;
use segment::types::VectorNameBuf;
use shard::operations::point_ops::{VectorPersisted, VectorStructPersisted};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyVector {
    // Put Int first so ints don't get parsed as floats (since f64 can extract from ints).
    Single(DenseVector),
    MultiDense(Vec<DenseVector>),
    Named(HashMap<VectorNameBuf, PyVectorType>),
}

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyVectorType {
    // Put Int first so ints don't get parsed as floats (since f64 can extract from ints).
    Dense(DenseVector),
    MultiDense(Vec<DenseVector>),
    Sparse(PySparseVector),
}

#[pyclass(name = "SparseVector")]
#[derive(Clone, Debug, Into)]
pub struct PySparseVector(SparseVector);

#[pymethods]
impl PySparseVector {
    #[new]
    pub fn new(indices: Vec<DimId>, values: Vec<DimWeight>) -> Self {
        Self(SparseVector { indices, values })
    }

    #[getter]
    pub fn indices(&self) -> &[DimId] {
        self.0.indices.as_slice()
    }

    #[getter]
    pub fn values(&self) -> &[DimWeight] {
        self.0.values.as_slice()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "SparseVector(indices={:?}, values={:?})",
            self.indices(),
            self.values()
        ))
    }
}

impl From<VectorStructPersisted> for PyVector {
    fn from(value: VectorStructPersisted) -> Self {
        match value {
            VectorStructPersisted::Single(dense) => PyVector::Single(dense),
            VectorStructPersisted::MultiDense(multi) => PyVector::MultiDense(multi),
            VectorStructPersisted::Named(named) => PyVector::Named(
                named
                    .into_iter()
                    .map(|(k, v)| (k, PyVectorType::from(v)))
                    .collect::<HashMap<_, _>>(),
            ),
        }
    }
}

impl From<VectorPersisted> for PyVectorType {
    fn from(value: VectorPersisted) -> Self {
        match value {
            VectorPersisted::Dense(dense) => PyVectorType::Dense(dense),
            VectorPersisted::MultiDense(multi) => PyVectorType::MultiDense(multi),
            VectorPersisted::Sparse(sparse) => PyVectorType::Sparse(PySparseVector(sparse)),
        }
    }
}

impl From<PyVector> for VectorStructPersisted {
    fn from(value: PyVector) -> Self {
        match value {
            PyVector::Single(dense) => VectorStructPersisted::Single(dense),
            PyVector::MultiDense(multi) => VectorStructPersisted::MultiDense(multi),
            PyVector::Named(named) => VectorStructPersisted::Named(
                named
                    .into_iter()
                    .map(|(k, v)| (k, VectorPersisted::from(v)))
                    .collect::<HashMap<_, _>>(),
            ),
        }
    }
}
impl From<PyVectorType> for VectorPersisted {
    fn from(value: PyVectorType) -> Self {
        match value {
            PyVectorType::Dense(dense) => VectorPersisted::Dense(dense),
            PyVectorType::MultiDense(multi) => VectorPersisted::MultiDense(multi),
            PyVectorType::Sparse(sparse) => VectorPersisted::Sparse(sparse.0),
        }
    }
}

impl From<VectorStructInternal> for PyVector {
    fn from(value: VectorStructInternal) -> Self {
        match value {
            VectorStructInternal::Single(dense) => PyVector::Single(dense),
            VectorStructInternal::MultiDense(multi) => {
                PyVector::MultiDense(multi.into_multi_vectors())
            }
            VectorStructInternal::Named(named) => PyVector::Named(
                named
                    .into_iter()
                    .map(|(k, v)| (k, PyVectorType::from(v)))
                    .collect::<HashMap<_, _>>(),
            ),
        }
    }
}

impl From<VectorInternal> for PyVectorType {
    fn from(value: VectorInternal) -> Self {
        match value {
            VectorInternal::Dense(dense) => PyVectorType::Dense(dense),
            VectorInternal::MultiDense(multi) => {
                PyVectorType::MultiDense(multi.into_multi_vectors())
            }
            VectorInternal::Sparse(sparse) => PyVectorType::Sparse(PySparseVector(sparse)),
        }
    }
}

impl TryFrom<PyVector> for VectorStructInternal {
    type Error = PyErr;

    fn try_from(vector: PyVector) -> PyResult<Self> {
        let vector = match vector {
            PyVector::Single(dense) => VectorStructInternal::Single(dense),
            PyVector::MultiDense(multi) => {
                VectorStructInternal::MultiDense(flat_multi_dense_from_nested(multi)?)
            }
            PyVector::Named(named) => VectorStructInternal::Named(
                named
                    .into_iter()
                    .map(|(key, val)| VectorInternal::try_from(val).map(move |val| (key, val)))
                    .collect::<Result<_, _>>()?,
            ),
        };

        Ok(vector)
    }
}

impl TryFrom<PyVectorType> for VectorInternal {
    type Error = PyErr;

    fn try_from(value: PyVectorType) -> PyResult<Self> {
        let vector = match value {
            PyVectorType::Dense(dense) => VectorInternal::Dense(dense),
            PyVectorType::MultiDense(multi) => {
                VectorInternal::MultiDense(flat_multi_dense_from_nested(multi)?)
            }
            PyVectorType::Sparse(sparse) => VectorInternal::Sparse(sparse.0),
        };

        Ok(vector)
    }
}

fn flat_multi_dense_from_nested(multi: Vec<Vec<f32>>) -> PyResult<TypedMultiDenseVector<f32>> {
    TypedMultiDenseVector::try_from_matrix(multi)
        .map_err(|err| PyValueError::new_err(format!("invalid multi-dense vector: {err}")))
}
