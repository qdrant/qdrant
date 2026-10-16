use std::collections::HashMap;

use bytemuck::TransparentWrapperAlloc as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::types::VectorNameBuf;
use shard::operations::point_ops::{BatchVectorStructPersisted, VectorPersisted};

use super::ndarray::{BatchArray, Ndarray};
use super::vector::PyNamedVector;

/// Vectors of `UpdateOperation.upsert_batch`, one entry per point.
///
/// Either a NumPy array (2-D: one vector per point, 3-D: one multivector per point), a list of
/// vectors or multivectors, or a dict mapping vector names to any of those.
#[derive(Clone, Debug)]
pub struct PyBatchVectors(pub BatchVectorStructPersisted);

impl PyBatchVectors {
    /// Fail unless every vector name has exactly `points` entries.
    pub fn check_len(&self, points: usize) -> PyResult<()> {
        let check = |name: &str, len: usize| {
            if len == points {
                return Ok(());
            }
            Err(PyValueError::new_err(format!(
                "upsert_batch got {points} ids but {len} vectors{name}"
            )))
        };

        match &self.0 {
            BatchVectorStructPersisted::Single(vectors) => check("", vectors.len()),
            BatchVectorStructPersisted::MultiDense(vectors) => check("", vectors.len()),
            BatchVectorStructPersisted::Named(named) => named
                .iter()
                .try_for_each(|(name, vectors)| check(&format!(" for {name:?}"), vectors.len())),
        }
    }
}

impl FromPyObject<'_, '_> for PyBatchVectors {
    type Error = PyErr;

    fn extract(vectors: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Some(array) = Ndarray::extract(&vectors)? {
            return Ok(Self(match array.into_batch()? {
                BatchArray::Dense(dense) => BatchVectorStructPersisted::Single(dense),
                BatchArray::MultiDense(multi) => BatchVectorStructPersisted::MultiDense(multi),
            }));
        }

        #[derive(FromPyObject)]
        enum Helper {
            Single(Vec<Vec<f32>>),
            MultiDense(Vec<Vec<Vec<f32>>>),
            Named(HashMap<VectorNameBuf, PyBatchNamedVectors>),
        }

        let vectors = match vectors.extract()? {
            Helper::Single(single) => BatchVectorStructPersisted::Single(single),
            Helper::MultiDense(multi) => BatchVectorStructPersisted::MultiDense(multi),
            Helper::Named(named) => BatchVectorStructPersisted::Named(
                named
                    .into_iter()
                    .map(|(name, vectors)| (name, vectors.0))
                    .collect(),
            ),
        };

        Ok(Self(vectors))
    }
}

/// The vectors of one name in a named `upsert_batch`, one entry per point.
struct PyBatchNamedVectors(Vec<VectorPersisted>);

impl FromPyObject<'_, '_> for PyBatchNamedVectors {
    type Error = PyErr;

    fn extract(vectors: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        if let Some(array) = Ndarray::extract(&vectors)? {
            return Ok(Self(match array.into_batch()? {
                BatchArray::Dense(dense) => dense.into_iter().map(VectorPersisted::Dense).collect(),
                BatchArray::MultiDense(multi) => {
                    multi.into_iter().map(VectorPersisted::MultiDense).collect()
                }
            }));
        }

        let vectors: Vec<PyNamedVector> = vectors.extract()?;
        Ok(Self(PyNamedVector::peel_vec(vectors)))
    }
}
