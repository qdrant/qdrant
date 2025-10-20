use derive_more::Into;
use pyo3::{FromPyObject, IntoPyObject, pyclass, pymethods};
use segment::types::{
    AnyVariants, IntPayloadType, Match, MatchAny, MatchExcept, MatchPhrase, MatchText,
    MatchTextAny, MatchValue, ValueVariants,
};

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
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
            PyMatch::Value(v) => Match::Value(MatchValue::from(v)),
            PyMatch::Text(v) => Match::Text(MatchText::from(v)),
            PyMatch::TextAny(v) => Match::TextAny(MatchTextAny::from(v)),
            PyMatch::Phrase(v) => Match::Phrase(MatchPhrase::from(v)),
            PyMatch::Any(v) => Match::Any(MatchAny::from(v)),
            PyMatch::Except(v) => Match::Except(MatchExcept::from(v)),
        }
    }
}

impl From<Match> for PyMatch {
    fn from(value: Match) -> Self {
        match value {
            Match::Value(v) => PyMatch::Value(PyMatchValue(v)),
            Match::Text(v) => PyMatch::Text(PyMatchText(v)),
            Match::TextAny(v) => PyMatch::TextAny(PyMatchTextAny(v)),
            Match::Phrase(v) => PyMatch::Phrase(PyMatchPhrase(v)),
            Match::Any(v) => PyMatch::Any(PyMatchAny(v)),
            Match::Except(v) => PyMatch::Except(PyMatchExcept(v)),
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

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyValueVariants {
    String(String),
    Integer(IntPayloadType),
    Bool(bool),
}

impl From<PyValueVariants> for ValueVariants {
    fn from(value: PyValueVariants) -> Self {
        match value {
            PyValueVariants::String(s) => ValueVariants::String(s),
            PyValueVariants::Integer(i) => ValueVariants::Integer(i),
            PyValueVariants::Bool(b) => ValueVariants::Bool(b),
        }
    }
}

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyAnyVariants {
    Strings(Vec<String>),
    Integers(Vec<IntPayloadType>),
}

impl From<PyAnyVariants> for AnyVariants {
    fn from(value: PyAnyVariants) -> Self {
        match value {
            PyAnyVariants::Strings(s) => AnyVariants::Strings(s.into_iter().collect()),
            PyAnyVariants::Integers(i) => AnyVariants::Integers(i.into_iter().collect()),
        }
    }
}
