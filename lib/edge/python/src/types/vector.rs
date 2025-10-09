use std::collections::HashMap;

use derive_more::Into;
use pyo3::{pyclass, pymethods};
use segment::data_types::vectors::DenseVector;
use segment::types::VectorNameBuf;
use shard::operations::point_ops::{VectorPersisted, VectorStructPersisted};
use sparse::common::sparse_vector::SparseVector;
use sparse::common::types::{DimId, DimWeight};

#[pyclass(name = "Vector")]
#[derive(Clone, Debug, Into)]
pub struct PyVector(pub VectorStructPersisted);

#[pymethods]
impl PyVector {
    #[staticmethod]
    pub fn single(vector: DenseVector) -> Self {
        Self(VectorStructPersisted::Single(vector))
    }

    #[staticmethod]
    pub fn multi_dense(vectors: Vec<DenseVector>) -> Self {
        Self(VectorStructPersisted::MultiDense(vectors))
    }

    #[staticmethod]
    pub fn named(vectors: HashMap<VectorNameBuf, PyNamedVector>) -> Self {
        // TODO: Transmute!?
        let vectors = vectors
            .into_iter()
            .map(|(name, vector)| (name, vector.into()))
            .collect();

        Self(VectorStructPersisted::Named(vectors))
    }
}

impl PyVector {
    fn _variants(vector: VectorStructPersisted) {
        match vector {
            VectorStructPersisted::Single(_) => (),
            VectorStructPersisted::MultiDense(_) => (),
            VectorStructPersisted::Named(_) => (),
        }
    }
}

#[pyclass(name = "NamedVector")]
#[derive(Clone, Debug, Into)]
pub struct PyNamedVector(VectorPersisted);

#[pymethods]
impl PyNamedVector {
    #[staticmethod]
    pub fn dense(vector: DenseVector) -> Self {
        Self(VectorPersisted::Dense(vector))
    }

    #[staticmethod]
    pub fn multi_dense(vectors: Vec<DenseVector>) -> Self {
        Self(VectorPersisted::MultiDense(vectors))
    }

    #[staticmethod]
    pub fn sparse(vector: PySparseVector) -> Self {
        Self(VectorPersisted::Sparse(vector.into()))
    }
}

impl PyNamedVector {
    fn _variants(vector: VectorPersisted) {
        match vector {
            VectorPersisted::Dense(_) => (),
            VectorPersisted::MultiDense(_) => (),
            VectorPersisted::Sparse(_) => (),
        }
    }
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
    pub fn indices(&self) -> Vec<DimId> {
        self.0.indices.clone()
    }

    #[getter]
    pub fn values(&self) -> Vec<DimWeight> {
        self.0.values.clone()
    }
}
