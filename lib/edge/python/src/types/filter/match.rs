use std::hash::Hash;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use pyo3::types::PyList;
use segment::types::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatch(pub Match);

impl FromPyObject<'_, '_> for PyMatch {
    type Error = PyErr;

    fn extract(filter: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Value(PyMatchValue),
            Text(PyMatchText),
            TextAny(PyMatchTextAny),
            Phrase(PyMatchPhrase),
            Any(PyMatchAny),
            Except(PyMatchExcept),
        }

        fn _variants(filter: Match) {
            match filter {
                Match::Value(_) => {}
                Match::Text(_) => {}
                Match::TextAny(_) => {}
                Match::Phrase(_) => {}
                Match::Any(_) => {}
                Match::Except(_) => {}
            }
        }

        let filter = match filter.extract()? {
            Helper::Value(value) => Match::Value(MatchValue::from(value)),
            Helper::Text(text) => Match::Text(MatchText::from(text)),
            Helper::TextAny(text_any) => Match::TextAny(MatchTextAny::from(text_any)),
            Helper::Phrase(phrase) => Match::Phrase(MatchPhrase::from(phrase)),
            Helper::Any(any) => Match::Any(MatchAny::from(any)),
            Helper::Except(except) => Match::Except(MatchExcept::from(except)),
        };

        Ok(Self(filter))
    }
}

impl<'py> IntoPyObject<'py> for PyMatch {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Match::Value(value) => PyMatchValue(value).into_bound_py_any(py),
            Match::Text(text) => PyMatchText(text).into_bound_py_any(py),
            Match::TextAny(text_any) => PyMatchTextAny(text_any).into_bound_py_any(py),
            Match::Phrase(phrase) => PyMatchPhrase(phrase).into_bound_py_any(py),
            Match::Any(any) => PyMatchAny(any).into_bound_py_any(py),
            Match::Except(except) => PyMatchExcept(except).into_bound_py_any(py),
        }
    }
}

#[pyclass(name = "MatchValue")]
#[derive(Clone, Debug, Into)]
pub struct PyMatchValue(pub MatchValue);

#[pymethods]
impl PyMatchValue {
    #[new]
    pub fn new(value: PyValueVariants) -> Self {
        Self(MatchValue {
            value: ValueVariants::from(value),
        })
    }

    #[getter]
    pub fn value(&self) -> &PyValueVariants {
        PyValueVariants::wrap_ref(&self.0.value)
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyValueVariants(ValueVariants);

impl FromPyObject<'_, '_> for PyValueVariants {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            String(String),
            Integer(IntPayloadType),
            Bool(bool),
        }

        fn _variants(value: ValueVariants) {
            match value {
                ValueVariants::String(_) => {}
                ValueVariants::Integer(_) => {}
                ValueVariants::Bool(_) => {}
            }
        }

        let value = match value.extract()? {
            Helper::String(str) => ValueVariants::String(str),
            Helper::Integer(int) => ValueVariants::Integer(int),
            Helper::Bool(bool) => ValueVariants::Bool(bool),
        };

        Ok(Self(value))
    }
}

impl<'py> IntoPyObject<'py> for PyValueVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyValueVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            ValueVariants::String(str) => str.into_bound_py_any(py),
            ValueVariants::Integer(int) => int.into_bound_py_any(py),
            ValueVariants::Bool(bool) => bool.into_bound_py_any(py),
        }
    }
}

#[pyclass(name = "MatchText")]
#[derive(Clone, Debug, Into)]
pub struct PyMatchText(pub MatchText);

#[pymethods]
impl PyMatchText {
    #[new]
    pub fn new(text: String) -> Self {
        Self(MatchText { text })
    }

    #[getter]
    pub fn text(&self) -> &str {
        &self.0.text
    }
}

#[pyclass(name = "MatchTextAny")]
#[derive(Clone, Debug, Into)]
pub struct PyMatchTextAny(pub MatchTextAny);

#[pymethods]
impl PyMatchTextAny {
    #[new]
    pub fn new(text_any: String) -> Self {
        Self(MatchTextAny { text_any })
    }

    #[getter]
    pub fn text_any(&self) -> &str {
        &self.0.text_any
    }
}

#[pyclass(name = "MatchPhrase")]
#[derive(Clone, Debug, Into)]
pub struct PyMatchPhrase(pub MatchPhrase);

#[pymethods]
impl PyMatchPhrase {
    #[new]
    pub fn new(phrase: String) -> Self {
        Self(MatchPhrase { phrase })
    }

    #[getter]
    pub fn phrase(&self) -> &str {
        &self.0.phrase
    }
}

#[pyclass(name = "MatchAny")]
#[derive(Clone, Debug, Into)]
pub struct PyMatchAny(pub MatchAny);

#[pymethods]
impl PyMatchAny {
    #[new]
    pub fn new(any: PyAnyVariants) -> Self {
        Self(MatchAny {
            any: AnyVariants::from(any),
        })
    }

    #[getter]
    pub fn value(&self) -> &PyAnyVariants {
        PyAnyVariants::wrap_ref(&self.0.any)
    }
}

#[pyclass(name = "MatchExcept")]
#[derive(Clone, Debug, Into)]
pub struct PyMatchExcept(pub MatchExcept);

#[pymethods]
impl PyMatchExcept {
    #[new]
    pub fn new(except: PyAnyVariants) -> Self {
        Self(MatchExcept {
            except: AnyVariants::from(except),
        })
    }

    #[getter]
    pub fn value(&self) -> &PyAnyVariants {
        PyAnyVariants::wrap_ref(&self.0.except)
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyAnyVariants(AnyVariants);

impl FromPyObject<'_, '_> for PyAnyVariants {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Strings(#[pyo3(from_py_with = index_set_from_py)] IndexSet<String>),
            Integers(#[pyo3(from_py_with = index_set_from_py)] IndexSet<i64>),
        }

        fn _variants(value: AnyVariants) {
            match value {
                AnyVariants::Strings(_) => {}
                AnyVariants::Integers(_) => {}
            }
        }

        let value = match value.extract()? {
            Helper::Strings(str) => AnyVariants::Strings(str),
            Helper::Integers(int) => AnyVariants::Integers(int),
        };

        Ok(Self(value))
    }
}

impl<'py> IntoPyObject<'py> for PyAnyVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyAnyVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            AnyVariants::Strings(str) => index_set_into_py::<String>(str, py),
            AnyVariants::Integers(int) => index_set_into_py::<i64>(int, py),
        }
    }
}

type IndexSet<T, S = fnv::FnvBuildHasher> = indexmap::IndexSet<T, S>;

fn index_set_from_py<T>(list: &Bound<'_, PyAny>) -> PyResult<IndexSet<T>>
where
    T: for<'py> FromPyObjectOwned<'py, Error = PyErr> + Eq + Hash,
{
    let list = list.cast::<PyList>()?;

    let mut set = IndexSet::with_capacity_and_hasher(list.len(), Default::default());

    for value in list.iter() {
        let value = value.extract()?;
        set.insert(value);
    }

    Ok(set)
}

fn index_set_into_py<'py, T>(set: &IndexSet<T>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>
where
    for<'a> &'a T: IntoPyObject<'py>,
{
    let list = PyList::empty(py);

    for value in set {
        list.append(value)?;
    }

    Ok(list.into_any())
}
