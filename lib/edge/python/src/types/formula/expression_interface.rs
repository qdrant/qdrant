use pyo3::prelude::*;

use crate::*;

#[pyclass(name = "Expression")]
#[derive(Clone, Debug)]
#[expect(clippy::large_enum_variant)] // TODO: `FromPyObject`/`IntoPyObject` is not implemented for `Box<T>` 🙄
pub enum PyExpressionInterface {
    Constant(f32),
    Variable(String),
    Condition(PyCondition),
    GeoDistance {
        origin: PyGeoPoint,
        to: PyJsonPath,
    },
    Datetime(String),
    DatetimeKey(PyJsonPath),
    Mult(Vec<PyExpression>),
    Sum(Vec<PyExpression>),
    Neg(PyExpression),
    Div {
        left: PyExpression,
        right: PyExpression,
        by_zero_default: Option<f32>,
    },
    Sqrt(PyExpression),
    Pow {
        base: PyExpression,
        exponent: PyExpression,
    },
    Exp(PyExpression),
    Log10(PyExpression),
    Ln(PyExpression),
    Abs(PyExpression),
    Decay {
        kind: PyDecayKind,
        x: PyExpression,
        target: Option<PyExpression>,
        midpoint: Option<f32>,
        scale: Option<f32>,
    },
}
