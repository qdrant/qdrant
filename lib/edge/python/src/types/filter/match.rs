use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub enum PyMatch {
    Value(PyMatchValue),
    Text(PyMatchText),
    TextAny(PyMatchTextAny),
    Phrase(PyMatchPhrase),
    Any(PyMatchAny),
    Except(PyMatchExcept),
}

impl From<PyMatch> for Match {
    fn from(value: PyMatch) -> Self {
        match value {
            PyMatch::Value(value) => Match::Value(MatchValue::from(value)),
            PyMatch::Text(text) => Match::Text(MatchText::from(text)),
            PyMatch::TextAny(text_any) => Match::TextAny(MatchTextAny::from(text_any)),
            PyMatch::Phrase(phrase) => Match::Phrase(MatchPhrase::from(phrase)),
            PyMatch::Any(any) => Match::Any(MatchAny::from(any)),
            PyMatch::Except(except) => Match::Except(MatchExcept::from(except)),
        }
    }
}

impl From<Match> for PyMatch {
    fn from(value: Match) -> Self {
        match value {
            Match::Value(value) => PyMatch::Value(PyMatchValue(value)),
            Match::Text(text) => PyMatch::Text(PyMatchText(text)),
            Match::TextAny(text_any) => PyMatch::TextAny(PyMatchTextAny(text_any)),
            Match::Phrase(phrase) => PyMatch::Phrase(PyMatchPhrase(phrase)),
            Match::Any(any) => PyMatch::Any(PyMatchAny(any)),
            Match::Except(except) => PyMatch::Except(PyMatchExcept(except)),
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
}

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub enum PyValueVariants {
    String(String),
    Integer(IntPayloadType),
    Bool(bool),
}

impl From<PyValueVariants> for ValueVariants {
    fn from(value: PyValueVariants) -> Self {
        match value {
            PyValueVariants::String(str) => ValueVariants::String(str),
            PyValueVariants::Integer(int) => ValueVariants::Integer(int),
            PyValueVariants::Bool(bool) => ValueVariants::Bool(bool),
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
}

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub enum PyAnyVariants {
    Strings(Vec<String>),
    Integers(Vec<IntPayloadType>),
}

impl From<PyAnyVariants> for AnyVariants {
    fn from(any: PyAnyVariants) -> Self {
        match any {
            PyAnyVariants::Strings(str) => AnyVariants::Strings(str.into_iter().collect()),
            PyAnyVariants::Integers(int) => AnyVariants::Integers(int.into_iter().collect()),
        }
    }
}
