use std::fmt;
use std::hash::Hash;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::{IntoPyObjectExt as _, PyTypeInfo, type_hint_subscript};
use segment::types::*;

use crate::repr::*;
use crate::type_hint::Alias;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatch(pub Match);

pub const MATCH: Alias = Alias {
    name: "MatchType",
    definition: MatchHelper::INPUT_TYPE,
};

#[derive(FromPyObject, IntoPyObject)]
enum MatchHelper {
    Value(PyMatchValue),
    Text(PyMatchText),
    TextAny(PyMatchTextAny),
    Phrase(PyMatchPhrase),
    Prefix(PyMatchPrefix),
    Substring(PyMatchSubstring),
    Any(PyMatchAny),
    Except(PyMatchExcept),
}

impl FromPyObject<'_, '_> for PyMatch {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = MATCH.hint();

    fn extract(filter: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(filter: Match) {
            match filter {
                Match::Value(_) => {}
                Match::Text(_) => {}
                Match::TextAny(_) => {}
                Match::Phrase(_) => {}
                Match::Prefix(_) => {}
                Match::Substring(_) => {}
                Match::Any(_) => {}
                Match::Except(_) => {}
            }
        }

        let filter = match filter.extract()? {
            MatchHelper::Value(value) => Match::Value(MatchValue::from(value)),
            MatchHelper::Text(text) => Match::Text(MatchText::from(text)),
            MatchHelper::TextAny(text_any) => Match::TextAny(MatchTextAny::from(text_any)),
            MatchHelper::Phrase(phrase) => Match::Phrase(MatchPhrase::from(phrase)),
            MatchHelper::Prefix(prefix) => Match::Prefix(MatchPrefix::from(prefix)),
            MatchHelper::Substring(substring) => Match::Substring(MatchSubstring::from(substring)),
            MatchHelper::Any(any) => Match::Any(MatchAny::from(any)),
            MatchHelper::Except(except) => Match::Except(MatchExcept::from(except)),
        };

        Ok(Self(filter))
    }
}

impl<'py> IntoPyObject<'py> for PyMatch {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = MATCH.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Match::Value(value) => MatchHelper::Value(PyMatchValue(value)),
            Match::Text(text) => MatchHelper::Text(PyMatchText(text)),
            Match::TextAny(text_any) => MatchHelper::TextAny(PyMatchTextAny(text_any)),
            Match::Phrase(phrase) => MatchHelper::Phrase(PyMatchPhrase(phrase)),
            Match::Prefix(prefix) => MatchHelper::Prefix(PyMatchPrefix(prefix)),
            Match::Substring(substring) => MatchHelper::Substring(PyMatchSubstring(substring)),
            Match::Any(any) => MatchHelper::Any(PyMatchAny(any)),
            Match::Except(except) => MatchHelper::Except(PyMatchExcept(except)),
        }
        .into_bound_py_any(py)
    }
}

impl Repr for PyMatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Match::Value(value) => PyMatchValue::wrap_ref(value).fmt(f),
            Match::Text(text) => PyMatchText::wrap_ref(text).fmt(f),
            Match::TextAny(text_any) => PyMatchTextAny::wrap_ref(text_any).fmt(f),
            Match::Phrase(phrase) => PyMatchPhrase::wrap_ref(phrase).fmt(f),
            Match::Prefix(prefix) => PyMatchPrefix::wrap_ref(prefix).fmt(f),
            Match::Substring(substring) => PyMatchSubstring::wrap_ref(substring).fmt(f),
            Match::Any(any) => PyMatchAny::wrap_ref(any).fmt(f),
            Match::Except(except) => PyMatchExcept::wrap_ref(except).fmt(f),
        }
    }
}

/// Match exact value.
///
/// Create a MatchValue.
///
/// Args:
///     value: Value to match.
#[pyclass(name = "MatchValue", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchValue(pub MatchValue);

#[pyclass_repr]
#[pymethods]
impl PyMatchValue {
    #[new]
    pub fn new(value: PyValueVariants) -> Self {
        Self(MatchValue {
            value: ValueVariants::from(value),
        })
    }

    /// Value.
    #[getter]
    pub fn value(&self) -> &PyValueVariants {
        PyValueVariants::wrap_ref(&self.0.value)
    }
}

impl PyMatchValue {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchValue { value: _ } = self.0;
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyValueVariants(ValueVariants);

#[derive(FromPyObject, IntoPyObject)]
enum ValueVariantsHelper {
    String(String),
    Integer(IntPayloadType),
    Bool(bool),
}

impl FromPyObject<'_, '_> for PyValueVariants {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = ValueVariantsHelper::INPUT_TYPE;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(value: ValueVariants) {
            match value {
                ValueVariants::String(_) => {}
                ValueVariants::Integer(_) => {}
                ValueVariants::Bool(_) => {}
            }
        }

        let value = match value.extract()? {
            ValueVariantsHelper::String(str) => ValueVariants::String(str),
            ValueVariantsHelper::Integer(int) => ValueVariants::Integer(int),
            ValueVariantsHelper::Bool(bool) => ValueVariants::Bool(bool),
        };

        Ok(Self(value))
    }
}

impl<'py> IntoPyObject<'py> for PyValueVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = <&PyValueVariants>::OUTPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyValueVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = ValueVariantsHelper::OUTPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            ValueVariants::String(str) => str.into_bound_py_any(py),
            ValueVariants::Integer(int) => int.into_bound_py_any(py),
            ValueVariants::Bool(bool) => bool.into_bound_py_any(py),
        }
    }
}

impl Repr for PyValueVariants {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ValueVariants::String(str) => str.fmt(f),
            ValueVariants::Integer(int) => int.fmt(f),
            ValueVariants::Bool(bool) => bool.fmt(f),
        }
    }
}

/// Full-text match.
///
/// Create a MatchText.
///
/// Args:
///     text: Text to search for.
#[pyclass(name = "MatchText", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchText(pub MatchText);

#[pyclass_repr]
#[pymethods]
impl PyMatchText {
    #[new]
    pub fn new(text: String) -> Self {
        Self(MatchText { text })
    }

    /// Text.
    #[getter]
    pub fn text(&self) -> &str {
        &self.0.text
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchText {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchText { text: _ } = self.0;
    }
}

/// Match any of the words in text.
///
/// Create a MatchTextAny.
///
/// Args:
///     text_any: Space-separated words to match any of.
#[pyclass(name = "MatchTextAny", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchTextAny(pub MatchTextAny);

#[pyclass_repr]
#[pymethods]
impl PyMatchTextAny {
    #[new]
    pub fn new(text_any: String) -> Self {
        Self(MatchTextAny { text_any })
    }

    /// Text.
    #[getter]
    pub fn text_any(&self) -> &str {
        &self.0.text_any
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchTextAny {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchTextAny { text_any: _ } = self.0;
    }
}

/// Match exact phrase.
///
/// Create a MatchPhrase.
///
/// Args:
///     phrase: Phrase to match.
#[pyclass(name = "MatchPhrase", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchPhrase(pub MatchPhrase);

#[pyclass_repr]
#[pymethods]
impl PyMatchPhrase {
    #[new]
    pub fn new(phrase: String) -> Self {
        Self(MatchPhrase { phrase })
    }

    /// Phrase.
    #[getter]
    pub fn phrase(&self) -> &str {
        &self.0.phrase
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchPhrase {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchPhrase { phrase: _ } = self.0;
    }
}

/// Match keyword values starting with the given prefix.
///
/// Create a MatchPrefix.
///
/// Args:
///     prefix: Prefix to match.
#[pyclass(name = "MatchPrefix", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchPrefix(pub MatchPrefix);

#[pyclass_repr]
#[pymethods]
impl PyMatchPrefix {
    #[new]
    pub fn new(prefix: String) -> Self {
        Self(MatchPrefix { prefix })
    }

    /// Prefix.
    #[getter]
    pub fn prefix(&self) -> &str {
        &self.0.prefix
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchPrefix {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchPrefix { prefix: _ } = self.0;
    }
}

/// Match keyword values containing the given substring.
///
/// Create a MatchSubstring.
///
/// Args:
///     substring: Substring to match.
#[pyclass(name = "MatchSubstring", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchSubstring(pub MatchSubstring);

#[pyclass_repr]
#[pymethods]
impl PyMatchSubstring {
    #[new]
    pub fn new(substring: String) -> Self {
        Self(MatchSubstring { substring })
    }

    /// Substring.
    #[getter]
    pub fn substring(&self) -> &str {
        &self.0.substring
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchSubstring {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchSubstring { substring: _ } = self.0;
    }
}

/// Match any of the values.
///
/// Create a MatchAny.
///
/// Args:
///     any: List of values to match any of.
#[pyclass(name = "MatchAny", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchAny(pub MatchAny);

#[pyclass_repr]
#[pymethods]
impl PyMatchAny {
    #[new]
    pub fn new(any: PyAnyVariants) -> Self {
        Self(MatchAny {
            any: AnyVariants::from(any),
        })
    }

    /// Values.
    #[getter]
    pub fn value(&self) -> &PyAnyVariants {
        PyAnyVariants::wrap_ref(&self.0.any)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchAny {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchAny { any: _value } = self.0;
    }
}

/// Match any value except these.
///
/// Create a MatchExcept.
///
/// Args:
///     except_: List of values to exclude.
#[pyclass(name = "MatchExcept", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyMatchExcept(pub MatchExcept);

#[pyclass_repr]
#[pymethods]
impl PyMatchExcept {
    #[new]
    pub fn new(value: PyAnyVariants) -> Self {
        Self(MatchExcept {
            except: AnyVariants::from(value),
        })
    }

    /// Excluded values.
    #[getter]
    pub fn value(&self) -> &PyAnyVariants {
        PyAnyVariants::wrap_ref(&self.0.except)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyMatchExcept {
    fn _getters(self) {
        // Every field should have a getter method
        let MatchExcept { except: _value } = self.0;
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyAnyVariants(AnyVariants);

#[derive(FromPyObject, IntoPyObject)]
enum AnyVariantsHelper {
    Strings(PyIndexSet<String>),
    Integers(PyIndexSet<i64>),
}

impl FromPyObject<'_, '_> for PyAnyVariants {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = AnyVariantsHelper::INPUT_TYPE;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(value: AnyVariants) {
            match value {
                AnyVariants::Strings(_) => {}
                AnyVariants::Integers(_) => {}
            }
        }

        let value = match value.extract()? {
            AnyVariantsHelper::Strings(str) => AnyVariants::Strings(str.into()),
            AnyVariantsHelper::Integers(int) => AnyVariants::Integers(int.into()),
        };

        Ok(Self(value))
    }
}

impl<'py> IntoPyObject<'py> for PyAnyVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = <&PyAnyVariants>::OUTPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyAnyVariants {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = AnyVariantsHelper::OUTPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            AnyVariants::Strings(str) => PyIndexSet::wrap_ref(str).into_pyobject(py),
            AnyVariants::Integers(int) => PyIndexSet::wrap_ref(int).into_pyobject(py),
        }
    }
}

impl Repr for PyAnyVariants {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            AnyVariants::Strings(str) => f.list(str),
            AnyVariants::Integers(int) => f.list(int),
        }
    }
}

type IndexSet<T, S = fnv::FnvBuildHasher> = indexmap::IndexSet<T, S>;

/// An order-preserving set, converted to and from a Python `list`.
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
struct PyIndexSet<T>(IndexSet<T>);

impl<'py, T> FromPyObject<'_, 'py> for PyIndexSet<T>
where
    T: FromPyObjectOwned<'py, Error = PyErr> + Eq + Hash,
{
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = type_hint_subscript!(PyList::TYPE_HINT, T::INPUT_TYPE);

    fn extract(list: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let list = list.cast::<PyList>()?;

        let mut set = IndexSet::with_capacity_and_hasher(list.len(), Default::default());

        for value in list.iter() {
            let value = value.extract()?;
            set.insert(value);
        }

        Ok(Self(set))
    }
}

impl<'py, T> IntoPyObject<'py> for PyIndexSet<T>
where
    for<'a> &'a T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = <&PyIndexSet<T>>::OUTPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'a, 'py, T> IntoPyObject<'py> for &'a PyIndexSet<T>
where
    &'a T: IntoPyObject<'py>,
{
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;
    const OUTPUT_TYPE: PyStaticExpr = type_hint_subscript!(PyList::TYPE_HINT, <&T>::OUTPUT_TYPE);

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let list = PyList::empty(py);

        for value in &self.0 {
            list.append(value)?;
        }

        Ok(list.into_any())
    }
}
