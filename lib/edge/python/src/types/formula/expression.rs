use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use shard::query::formula::ExpressionInternal;

use crate::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyExpression(ExpressionInternal);

impl FromPyObject<'_, '_> for PyExpression {
    type Error = PyErr;

    fn extract(helper: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
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
                ExpressionInternal::Mult(PyExpression::peel_vec(exprs))
            }
            PyExpressionInterface::Sum(exprs) => {
                ExpressionInternal::Sum(PyExpression::peel_vec(exprs))
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

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let helper = match self.0 {
            ExpressionInternal::Constant(var) => PyExpressionInterface::Constant(var),
            ExpressionInternal::Variable(var) => PyExpressionInterface::Variable(var),
            ExpressionInternal::Condition(cond) => {
                PyExpressionInterface::Condition(PyCondition(*cond))
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
                PyExpressionInterface::Mult(PyExpression::wrap_vec(exprs))
            }
            ExpressionInternal::Sum(exprs) => {
                PyExpressionInterface::Sum(PyExpression::wrap_vec(exprs))
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
