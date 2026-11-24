use std::collections::HashMap;
use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict as _, PyList};
use segment::data_types::vectors::*;
use segment::types::VectorNameBuf;
use sparse::common::sparse_vector::SparseVector;

use super::vector::PySparseVector;
use crate::repr::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyVectorInternal(VectorStructInternal);

impl FromPyObject<'_, '_> for PyVectorInternal {
    type Error = PyErr;

    fn extract(vector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Single(Vec<f32>),
            MultiDense(#[pyo3(from_py_with = multi_dense_from_py)] MultiDenseVector),
            Named(HashMap<VectorNameBuf, PyNamedVectorInternal>),
        }

        fn _variants(vector: VectorStructInternal) {
            match vector {
                VectorStructInternal::Single(_) => {}
                VectorStructInternal::MultiDense(_) => {}
                VectorStructInternal::Named(_) => {}
            }
        }

        let vector = match vector.extract()? {
            Helper::Single(single) => VectorStructInternal::Single(single),
            Helper::MultiDense(multi) => {
                VectorStructInternal::MultiDense(MultiDenseVectorInternal::from(multi))
            }
            Helper::Named(named) => {
                VectorStructInternal::Named(PyNamedVectorInternal::peel_map(named))
            }
        };

        Ok(Self(vector))
    }
}

impl<'py> IntoPyObject<'py> for PyVectorInternal {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyVectorInternal {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            VectorStructInternal::Single(single) => single.into_bound_py_any(py),
            VectorStructInternal::MultiDense(multi) => multi_dense_into_py(multi, py),
            VectorStructInternal::Named(named) => Ok(PyNamedVectorInternal::wrap_map_ref(named)
                .into_py_dict(py)?
                .into_any()),
        }
    }
}

impl Repr for PyVectorInternal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            VectorStructInternal::Single(single) => single.fmt(f),
            VectorStructInternal::MultiDense(multi) => f.list(multi.multi_vectors()),
            VectorStructInternal::Named(named) => f.map(PyNamedVectorInternal::wrap_map_ref(named)),
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyNamedVectorInternal(pub VectorInternal);

impl PyNamedVectorInternal {
    pub fn peel_map(map: HashMap<String, Self>) -> HashMap<String, VectorInternal> {
        map.into_iter()
            .map(|(name, vector)| (name, vector.0))
            .collect()
    }

    pub fn wrap_map_ref(
        map: &HashMap<String, VectorInternal>,
    ) -> impl Iterator<Item = (&String, &Self)> {
        map.iter()
            .map(|(name, vector)| (name, Self::wrap_ref(vector)))
    }
}

impl FromPyObject<'_, '_> for PyNamedVectorInternal {
    type Error = PyErr;

    fn extract(vector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Dense(Vec<f32>),
            Sparse(PySparseVector),
            MultiDense(#[pyo3(from_py_with = multi_dense_from_py)] MultiDenseVector),
        }

        let vector = match vector.extract()? {
            Helper::Dense(dense) => VectorInternal::Dense(dense),
            Helper::Sparse(sparse) => VectorInternal::Sparse(SparseVector::from(sparse)),
            Helper::MultiDense(multi) => {
                VectorInternal::MultiDense(MultiDenseVectorInternal::from(multi))
            }
        };

        Ok(Self(vector))
    }
}

impl<'py> IntoPyObject<'py> for PyNamedVectorInternal {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyNamedVectorInternal {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            VectorInternal::Dense(dense) => dense.into_bound_py_any(py),
            VectorInternal::Sparse(sparse) => PySparseVector(sparse.clone()).into_bound_py_any(py),
            VectorInternal::MultiDense(multi) => multi_dense_into_py(multi, py),
        }
    }
}

impl Repr for PyNamedVectorInternal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            VectorInternal::Dense(dense) => dense.fmt(f),
            VectorInternal::Sparse(sparse) => PySparseVector::wrap_ref(sparse).fmt(f),
            VectorInternal::MultiDense(multi) => f.list(multi.multi_vectors()),
        }
    }
}

type MultiDenseVector = TypedMultiDenseVector<f32>;

fn multi_dense_from_py(matrix: &Bound<'_, PyAny>) -> PyResult<MultiDenseVector> {
    MultiDenseVector::try_from_matrix(matrix.extract()?)
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

fn multi_dense_into_py<'py>(
    multi: &MultiDenseVector,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let matrix = PyList::empty(py);

    for vector in multi.multi_vectors() {
        matrix.append(vector.into_pyobject(py)?)?;
    }

    Ok(matrix.into_any())
}
