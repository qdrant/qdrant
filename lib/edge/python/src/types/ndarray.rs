use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

/// A NumPy array converted to `float32`: its shape and its elements in row-major order.
///
/// Extracting a NumPy array as `Vec<f32>` goes through the sequence protocol and creates a
/// Python float per element, which costs more than the upsert itself. This reads the whole array
/// with one `tobytes()` call instead. The buffer protocol would avoid that copy, but it is not in
/// the stable ABI before Python 3.11, and the wheel targets 3.10.
pub struct Ndarray {
    shape: Vec<usize>,
    data: Vec<f32>,
}

/// A 1-D array is one dense vector, a 2-D array is a dense matrix.
pub enum DenseArray {
    Vector(Vec<f32>),
    Matrix(Vec<Vec<f32>>),
}

impl Ndarray {
    /// Read `obj` if it looks like a NumPy array (has `shape`, `astype` and `tobytes`).
    ///
    /// Returns `None` for anything else, so lists keep going through the regular extraction.
    pub fn extract(obj: &Bound<'_, PyAny>) -> PyResult<Option<Self>> {
        // A failed `hasattr` raises and discards an AttributeError, ~2% of a list upsert
        if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
            return Ok(None);
        }
        if !(obj.hasattr("shape")? && obj.hasattr("astype")? && obj.hasattr("tobytes")?) {
            return Ok(None);
        }

        let kwargs = PyDict::new(obj.py());
        kwargs.set_item("copy", false)?;
        let array = obj.call_method("astype", ("<f4",), Some(&kwargs))?;

        let shape: Vec<usize> = array.getattr("shape")?.extract()?;
        let bytes = array.call_method0("tobytes")?;
        let (elements, _) = bytes.cast::<PyBytes>()?.as_bytes().as_chunks();
        let data = elements.iter().copied().map(f32::from_le_bytes).collect();

        Ok(Some(Self { shape, data }))
    }

    pub fn into_dense(self) -> PyResult<DenseArray> {
        match self.shape.len() {
            1 => Ok(DenseArray::Vector(self.data)),
            2 => Ok(DenseArray::Matrix(self.into_rows())),
            n => Err(PyValueError::new_err(format!(
                "expected a 1-D vector or a 2-D multivector, got a {n}-D array"
            ))),
        }
    }

    /// Split along the first axis: one entry per point.
    pub fn into_batch(self) -> PyResult<BatchArray> {
        match self.shape.len() {
            2 => Ok(BatchArray::Dense(self.into_rows())),
            3 => {
                let (points, rows) = (self.shape[0], self.shape[1]);
                let mut matrices = self.into_rows().into_iter();
                Ok(BatchArray::MultiDense(
                    (0..points)
                        .map(|_| matrices.by_ref().take(rows).collect())
                        .collect(),
                ))
            }
            n => Err(PyValueError::new_err(format!(
                "expected a 2-D array (one vector per point) or a 3-D array (one multivector per \
                 point), got a {n}-D array"
            ))),
        }
    }

    /// Rows along the last axis.
    fn into_rows(self) -> Vec<Vec<f32>> {
        let width = self.shape.last().copied().unwrap_or(0);
        if width == 0 {
            let rows = self.shape[..self.shape.len() - 1].iter().product();
            return vec![Vec::new(); rows];
        }
        self.data.chunks_exact(width).map(<[f32]>::to_vec).collect()
    }
}

pub enum BatchArray {
    Dense(Vec<Vec<f32>>),
    MultiDense(Vec<Vec<Vec<f32>>>),
}
