use derive_more::Into;
use pyo3::{FromPyObject, IntoPyObject, pyclass, pymethods};
use segment::types::{IntPayloadType, Match, MatchValue, ValueVariants};

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyMatch {
    Value(PyMatchValue),
}

impl From<PyMatch> for Match {
    fn from(value: PyMatch) -> Self {
        match value {
            PyMatch::Value(v) => Match::Value(MatchValue::from(v)),
        }
    }
}

impl From<Match> for PyMatch {
    fn from(value: Match) -> Self {
        match value {
            Match::Value(v) => PyMatch::Value(PyMatchValue(v)),
            Match::Text(_) => todo!(),
            Match::TextAny(_) => todo!(),
            Match::Phrase(_) => todo!(),
            Match::Any(_) => todo!(),
            Match::Except(_) => todo!(),
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
