use std::collections::HashMap;
use std::mem;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::index::query_optimization::rescore_formula::parsed_formula::{
    DecayKind, ParsedFormula,
};
use shard::query::formula::{ExpressionInternal, FormulaInternal};

use super::*;

#[pyclass(name = "Formula")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyFormula(ParsedFormula);

#[pymethods]
impl PyFormula {
    #[new]
    pub fn new(formula: PyExpression, defaults: HashMap<String, PyValue>) -> PyResult<Self> {
        let formula = FormulaInternal {
            formula: formula.into(),
            defaults: PyValue::into_rust_map(defaults),
        };

        let formula = ParsedFormula::try_from(formula)
            .map_err(|err| PyValueError::new_err(format!("failed to parse formula: {err}")))?;

        Ok(Self(formula))
    }
}

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyExpression(ExpressionInternal);

impl PyExpression {
    pub fn from_rust_vec(expressions: Vec<ExpressionInternal>) -> Vec<Self> {
        // `PyExpression` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(expressions) }
    }

    pub fn into_rust_vec(expressions: Vec<Self>) -> Vec<ExpressionInternal> {
        // `PyExpression` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(expressions) }
    }
}

impl<'py> FromPyObject<'py> for PyExpression {
    fn extract_bound(helper: &Bound<'py, PyAny>) -> PyResult<Self> {
        let expr = match helper.extract()? {
            PyExpressionInterface::Constant(val) => ExpressionInternal::Constant(val),
            PyExpressionInterface::Variable(var) => ExpressionInternal::Variable(var),
            PyExpressionInterface::Condition(cond) => {
                ExpressionInternal::Condition(Box::new(cond.into()))
            }
            PyExpressionInterface::GeoDistance { origin, to } => ExpressionInternal::GeoDistance {
                origin: origin.into(),
                to: to.into(),
            },
            PyExpressionInterface::Datetime(dt) => ExpressionInternal::Datetime(dt),
            PyExpressionInterface::DatetimeKey(path) => {
                ExpressionInternal::DatetimeKey(path.into())
            }
            PyExpressionInterface::Mult(exprs) => {
                ExpressionInternal::Mult(PyExpression::into_rust_vec(exprs))
            }
            PyExpressionInterface::Sum(exprs) => {
                ExpressionInternal::Sum(PyExpression::into_rust_vec(exprs))
            }
            PyExpressionInterface::Neg(expr) => ExpressionInternal::Neg(Box::new(expr.into())),
            PyExpressionInterface::Div {
                left,
                right,
                by_zero_default,
            } => ExpressionInternal::Div {
                left: Box::new(left.into()),
                right: Box::new(right.into()),
                by_zero_default,
            },
            PyExpressionInterface::Sqrt(expr) => ExpressionInternal::Sqrt(Box::new(expr.into())),
            PyExpressionInterface::Pow { base, exponent } => ExpressionInternal::Pow {
                base: Box::new(base.into()),
                exponent: Box::new(exponent.into()),
            },
            PyExpressionInterface::Exp(expr) => ExpressionInternal::Exp(Box::new(expr.into())),
            PyExpressionInterface::Log10(expr) => ExpressionInternal::Log10(Box::new(expr.into())),
            PyExpressionInterface::Ln(expr) => ExpressionInternal::Ln(Box::new(expr.into())),
            PyExpressionInterface::Abs(expr) => ExpressionInternal::Abs(Box::new(expr.into())),
            PyExpressionInterface::Decay {
                kind,
                x,
                target,
                midpoint,
                scale,
            } => ExpressionInternal::Decay {
                kind: kind.into(),
                x: Box::new(x.into()),
                target: target.map(|target| Box::new(target.into())),
                midpoint,
                scale,
            },
        };

        Ok(Self(expr))
    }
}

impl<'py> IntoPyObject<'py> for PyExpression {
    type Target = PyExpressionInterface;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let helper = match self.0 {
            ExpressionInternal::Constant(var) => PyExpressionInterface::Constant(var),
            ExpressionInternal::Variable(var) => PyExpressionInterface::Variable(var),
            ExpressionInternal::Condition(cond) => {
                PyExpressionInterface::Condition(PyCondition::from(*cond))
            }
            ExpressionInternal::GeoDistance { origin, to } => PyExpressionInterface::GeoDistance {
                origin: PyGeoPoint(origin),
                to: PyJsonPath(to),
            },
            ExpressionInternal::Datetime(dt) => PyExpressionInterface::Datetime(dt),
            ExpressionInternal::DatetimeKey(path) => {
                PyExpressionInterface::DatetimeKey(PyJsonPath(path))
            }
            ExpressionInternal::Mult(exprs) => {
                PyExpressionInterface::Mult(PyExpression::from_rust_vec(exprs))
            }
            ExpressionInternal::Sum(exprs) => {
                PyExpressionInterface::Sum(PyExpression::from_rust_vec(exprs))
            }
            ExpressionInternal::Neg(expr) => PyExpressionInterface::Neg(PyExpression(*expr)),
            ExpressionInternal::Div {
                left,
                right,
                by_zero_default,
            } => PyExpressionInterface::Div {
                left: PyExpression(*left),
                right: PyExpression(*right),
                by_zero_default,
            },
            ExpressionInternal::Sqrt(expr) => PyExpressionInterface::Sqrt(PyExpression(*expr)),
            ExpressionInternal::Pow { base, exponent } => PyExpressionInterface::Pow {
                base: PyExpression(*base),
                exponent: PyExpression(*exponent),
            },
            ExpressionInternal::Exp(expr) => PyExpressionInterface::Exp(PyExpression(*expr)),
            ExpressionInternal::Log10(expr) => PyExpressionInterface::Log10(PyExpression(*expr)),
            ExpressionInternal::Ln(expr) => PyExpressionInterface::Ln(PyExpression(*expr)),
            ExpressionInternal::Abs(expr) => PyExpressionInterface::Abs(PyExpression(*expr)),
            ExpressionInternal::Decay {
                kind,
                x,
                target,
                midpoint,
                scale,
            } => PyExpressionInterface::Decay {
                kind: kind.into(),
                x: PyExpression(*x),
                target: target.map(|target| PyExpression(*target)),
                midpoint,
                scale,
            },
        };

        Bound::new(py, helper)
    }
}

// TODO: `FromPyObject`/`IntoPyObject` is not implemented for `Box<T>`, I'll fix this later 🙄
#[expect(clippy::large_enum_variant)]
#[pyclass(name = "Expression")]
#[derive(Clone, Debug)]
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

#[pyclass(name = "DecayKind")]
#[derive(Copy, Clone, Debug)]
pub enum PyDecayKind {
    /// Linear decay function
    Lin,
    /// Gaussian decay function
    Gauss,
    /// Exponential decay function
    Exp,
}

impl From<DecayKind> for PyDecayKind {
    fn from(decay_kind: DecayKind) -> Self {
        match decay_kind {
            DecayKind::Lin => PyDecayKind::Lin,
            DecayKind::Gauss => PyDecayKind::Gauss,
            DecayKind::Exp => PyDecayKind::Exp,
        }
    }
}

impl From<PyDecayKind> for DecayKind {
    fn from(decay_kind: PyDecayKind) -> Self {
        match decay_kind {
            PyDecayKind::Lin => DecayKind::Lin,
            PyDecayKind::Gauss => DecayKind::Gauss,
            PyDecayKind::Exp => DecayKind::Exp,
        }
    }
}
