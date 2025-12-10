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
            PyExpressionInterface::Constant { val } => ExpressionInternal::Constant(val),
            PyExpressionInterface::Variable { var } => ExpressionInternal::Variable(var),

            PyExpressionInterface::Condition { cond } => {
                ExpressionInternal::Condition(cond.into_box())
            }

            PyExpressionInterface::GeoDistance { origin, to } => ExpressionInternal::GeoDistance {
                origin: origin.into(),
                to: to.into(),
            },

            PyExpressionInterface::Datetime { date_time } => {
                ExpressionInternal::Datetime(date_time)
            }

            PyExpressionInterface::DatetimeKey { path } => {
                ExpressionInternal::DatetimeKey(path.into())
            }

            PyExpressionInterface::Mult { exprs } => {
                ExpressionInternal::Mult(PyExpression::peel_vec(exprs))
            }

            PyExpressionInterface::Sum { exprs } => {
                ExpressionInternal::Sum(PyExpression::peel_vec(exprs))
            }

            PyExpressionInterface::Neg { expr } => ExpressionInternal::Neg(expr.into_box()),

            PyExpressionInterface::Div {
                left,
                right,
                by_zero_default,
            } => ExpressionInternal::Div {
                left: left.into_box(),
                right: right.into_box(),
                by_zero_default,
            },

            PyExpressionInterface::Sqrt { expr } => ExpressionInternal::Sqrt(expr.into_box()),

            PyExpressionInterface::Pow { base, exponent } => ExpressionInternal::Pow {
                base: base.into_box(),
                exponent: exponent.into_box(),
            },

            PyExpressionInterface::Exp { expr } => ExpressionInternal::Exp(expr.into_box()),
            PyExpressionInterface::Log10 { expr } => ExpressionInternal::Log10(expr.into_box()),
            PyExpressionInterface::Ln { expr } => ExpressionInternal::Ln(expr.into_box()),
            PyExpressionInterface::Abs { expr } => ExpressionInternal::Abs(expr.into_box()),

            PyExpressionInterface::Decay {
                kind,
                x,
                target,
                midpoint,
                scale,
            } => ExpressionInternal::Decay {
                kind: kind.into(),
                x: x.into_box(),
                target: target.map(Boxed::into_box),
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
            ExpressionInternal::Constant(val) => PyExpressionInterface::Constant { val },
            ExpressionInternal::Variable(var) => PyExpressionInterface::Variable { var },

            ExpressionInternal::Condition(cond) => PyExpressionInterface::Condition {
                cond: Boxed::from_box(cond),
            },

            ExpressionInternal::GeoDistance { origin, to } => PyExpressionInterface::GeoDistance {
                origin: PyGeoPoint(origin),
                to: PyJsonPath(to),
            },

            ExpressionInternal::Datetime(date_time) => {
                PyExpressionInterface::Datetime { date_time }
            }

            ExpressionInternal::DatetimeKey(path) => PyExpressionInterface::DatetimeKey {
                path: PyJsonPath(path),
            },

            ExpressionInternal::Mult(exprs) => PyExpressionInterface::Mult {
                exprs: PyExpression::wrap_vec(exprs),
            },

            ExpressionInternal::Sum(exprs) => PyExpressionInterface::Sum {
                exprs: PyExpression::wrap_vec(exprs),
            },

            ExpressionInternal::Neg(expr) => PyExpressionInterface::Neg {
                expr: Boxed::from_box(expr),
            },

            ExpressionInternal::Div {
                left,
                right,
                by_zero_default,
            } => PyExpressionInterface::Div {
                left: Boxed::from_box(left),
                right: Boxed::from_box(right),
                by_zero_default,
            },

            ExpressionInternal::Sqrt(expr) => PyExpressionInterface::Sqrt {
                expr: Boxed::from_box(expr),
            },

            ExpressionInternal::Pow { base, exponent } => PyExpressionInterface::Pow {
                base: Boxed::from_box(base),
                exponent: Boxed::from_box(exponent),
            },

            ExpressionInternal::Exp(expr) => PyExpressionInterface::Exp {
                expr: Boxed::from_box(expr),
            },

            ExpressionInternal::Log10(expr) => PyExpressionInterface::Log10 {
                expr: Boxed::from_box(expr),
            },

            ExpressionInternal::Ln(expr) => PyExpressionInterface::Ln {
                expr: Boxed::from_box(expr),
            },

            ExpressionInternal::Abs(expr) => PyExpressionInterface::Abs {
                expr: Boxed::from_box(expr),
            },

            ExpressionInternal::Decay {
                kind,
                x,
                target,
                midpoint,
                scale,
            } => PyExpressionInterface::Decay {
                kind: kind.into(),
                x: Boxed::from_box(x),
                target: target.map(Boxed::from_box),
                midpoint,
                scale,
            },
        };

        Bound::new(py, helper)
    }
}
