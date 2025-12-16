use std::fmt;

use bytemuck::TransparentWrapper;
use pyo3::prelude::*;

use crate::repr::*;
use crate::*;

#[pyclass(name = "Expression")]
#[derive(Clone, Debug)]
pub enum PyExpressionInterface {
    Constant {
        val: f32,
    },

    Variable {
        var: String,
    },

    Condition {
        cond: Boxed<PyCondition>,
    },

    GeoDistance {
        origin: PyGeoPoint,
        to: PyJsonPath,
    },

    Datetime {
        date_time: String,
    },

    DatetimeKey {
        path: PyJsonPath,
    },

    Mult {
        exprs: Vec<PyExpression>,
    },

    Sum {
        exprs: Vec<PyExpression>,
    },

    Neg {
        expr: Boxed<PyExpression>,
    },

    Div {
        left: Boxed<PyExpression>,
        right: Boxed<PyExpression>,
        by_zero_default: Option<f32>,
    },

    Sqrt {
        expr: Boxed<PyExpression>,
    },

    Pow {
        base: Boxed<PyExpression>,
        exponent: Boxed<PyExpression>,
    },

    Exp {
        expr: Boxed<PyExpression>,
    },

    Log10 {
        expr: Boxed<PyExpression>,
    },

    Ln {
        expr: Boxed<PyExpression>,
    },

    Abs {
        expr: Boxed<PyExpression>,
    },

    Decay {
        kind: PyDecayKind,
        x: Boxed<PyExpression>,
        target: Option<Boxed<PyExpression>>,
        midpoint: Option<f32>,
        scale: Option<f32>,
    },
}

impl Repr for PyExpressionInterface {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (repr, fields): (_, &[(_, &dyn Repr)]) = match self {
            PyExpressionInterface::Constant { val } => ("Constant", &[("val", val)]),
            PyExpressionInterface::Variable { var } => ("Variable", &[("var", var)]),
            PyExpressionInterface::Condition { cond } => ("Condition", &[("cond", cond)]),

            PyExpressionInterface::GeoDistance { origin, to } => {
                ("GeoDistance", &[("origin", origin), ("to", to)])
            }

            PyExpressionInterface::Datetime { date_time } => {
                ("Datetime", &[("date_time", date_time)])
            }

            PyExpressionInterface::DatetimeKey { path } => ("DatetimeKey", &[("path", path)]),
            PyExpressionInterface::Mult { exprs } => ("Mult", &[("exprs", exprs)]),
            PyExpressionInterface::Sum { exprs } => ("Sum", &[("exprs", exprs)]),
            PyExpressionInterface::Neg { expr } => ("Neg", &[("expr", expr)]),

            PyExpressionInterface::Div {
                left,
                right,
                by_zero_default,
            } => (
                "Div",
                &[
                    ("left", left),
                    ("right", right),
                    ("by_zero_default", by_zero_default),
                ],
            ),

            PyExpressionInterface::Sqrt { expr } => ("Sqrt", &[("expr", expr)]),

            PyExpressionInterface::Pow { base, exponent } => {
                ("Pow", &[("base", base), ("exponent", exponent)])
            }

            PyExpressionInterface::Exp { expr } => ("Exp", &[("expr", expr)]),
            PyExpressionInterface::Log10 { expr } => ("Log10", &[("expr", expr)]),
            PyExpressionInterface::Ln { expr } => ("Ln", &[("expr", expr)]),
            PyExpressionInterface::Abs { expr } => ("Abs", &[("expr", expr)]),

            PyExpressionInterface::Decay {
                kind,
                x,
                target,
                midpoint,
                scale,
            } => (
                "Decay",
                &[
                    ("kind", kind),
                    ("x", x),
                    ("target", target),
                    ("midpoint", midpoint),
                    ("scale", scale),
                ],
            ),
        };

        f.complex_enum::<Self>(repr, fields)
    }
}

#[derive(Clone, Debug)]
pub struct Boxed<T>(Box<T>);

impl<T> Boxed<T> {
    pub fn from_box<U>(boxed: Box<U>) -> Self
    where
        T: TransparentWrapper<U>,
    {
        Self(T::wrap_box(boxed))
    }

    pub fn into_box<U>(self) -> Box<U>
    where
        T: TransparentWrapper<U>,
    {
        T::peel_box(self.0)
    }

    pub fn from_inner(inner: T) -> Self {
        Self(Box::new(inner))
    }

    pub fn into_inner(self) -> T {
        *self.0
    }
}

impl<'a, 'py, T> FromPyObject<'a, 'py> for Boxed<T>
where
    T: FromPyObject<'a, 'py>,
{
    type Error = T::Error;

    fn extract(any: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
        any.extract().map(Boxed::from_inner)
    }
}

impl<'py, T> IntoPyObject<'py> for Boxed<T>
where
    T: IntoPyObject<'py>,
{
    type Target = T::Target;
    type Output = T::Output;
    type Error = T::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.into_inner().into_pyobject(py)
    }
}

impl<T: Repr> Repr for Boxed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
