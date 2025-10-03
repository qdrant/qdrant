use std::collections::HashMap;

use derive_more::Into;
use pyo3::prelude::*;
use segment::data_types::vectors::*;
use shard::operations::point_ops::*;
use shard::operations::{CollectionUpdateOperations, point_ops};
use sparse::common::sparse_vector::SparseVector;

use super::*;

#[pyclass(name = "UpdateOperation")]
#[derive(Clone, Debug, Into)]
pub struct PyUpdateOperation(CollectionUpdateOperations);

#[pymethods]
impl PyUpdateOperation {
    #[staticmethod]
    pub fn upsert_points(points: Vec<PyPoint>) -> Self {
        let points = points.into_iter().map(Into::into).collect();

        let operation =
            CollectionUpdateOperations::PointOperation(point_ops::PointOperations::UpsertPoints(
                PointInsertOperationsInternal::PointsList(points),
            ));

        Self(operation)
    }
}

#[pyclass(name = "Point")]
#[derive(Clone, Debug, Into)]
pub struct PyPoint(PointStructPersisted);

#[pymethods]
impl PyPoint {
    #[new]
    pub fn new(id: PyPointId, vector: PyVector, payload: Option<PyPayload>) -> Self {
        let point = PointStructPersisted {
            id: id.into(),
            vector: vector.into(),
            payload: payload.map(Into::into),
        };

        Self(point)
    }
}

#[pyclass(name = "Vector")]
#[derive(Clone, Debug, Into)]
pub struct PyVector(VectorStructPersisted);

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
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Self {
        Self(SparseVector { indices, values })
    }

    #[getter]
    pub fn indices(&self) -> Vec<u32> {
        self.0.indices.clone()
    }

    #[getter]
    pub fn values(&self) -> Vec<f32> {
        self.0.values.clone()
    }
}
