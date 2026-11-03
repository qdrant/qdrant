use std::fmt;

use bytemuck::TransparentWrapper;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;

use crate::repr::*;
use crate::*;

/// Expression types for formulas.
#[pyclass(name = "Expression", from_py_object)]
#[derive(Clone, Debug)]
pub enum PyExpressionInterface {
    /// Create a constant expression.
    Constant { val: f32 },

    /// Create a variable expression.
    Variable { var: String },

    /// Create a condition expression (returns 1 if true, 0 if false).
    Condition { cond: Boxed<PyCondition> },

    /// Create a geo distance expression.
    GeoDistance { origin: PyGeoPoint, to: PyJsonPath },

    /// Create a datetime constant expression.
    Datetime { date_time: String },

    /// Create a datetime field expression.
    DatetimeKey { path: PyJsonPath },

    /// Create a multiplication expression.
    Mult { exprs: Vec<PyExpression> },

    /// Create a sum expression.
    Sum { exprs: Vec<PyExpression> },

    /// Create a maximum expression. Requires at least one operand.
    Max { exprs: Vec<PyExpression> },

    /// Create a minimum expression. Requires at least one operand.
    Min { exprs: Vec<PyExpression> },

    /// Create a negation expression.
    Neg { expr: Boxed<PyExpression> },

    /// Create a division expression.
    Div {
        left: Boxed<PyExpression>,
        right: Boxed<PyExpression>,
        by_zero_default: Option<f32>,
    },

    /// Create a square root expression.
    Sqrt { expr: Boxed<PyExpression> },

    /// Create a power expression.
    Pow {
        base: Boxed<PyExpression>,
        exponent: Boxed<PyExpression>,
    },

    /// Create an exponential expression.
    Exp { expr: Boxed<PyExpression> },

    /// Create a log10 expression.
    Log10 { expr: Boxed<PyExpression> },

    /// Create a natural log expression.
    Ln { expr: Boxed<PyExpression> },

    /// Create an inverse hyperbolic cosine expression.
    Acosh { expr: Boxed<PyExpression> },

    /// Create an absolute value expression.
    Abs { expr: Boxed<PyExpression> },

    /// Create a decay expression.
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
            PyExpressionInterface::Max { exprs } => ("Max", &[("exprs", exprs)]),
            PyExpressionInterface::Min { exprs } => ("Min", &[("exprs", exprs)]),
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
            PyExpressionInterface::Acosh { expr } => ("Acosh", &[("expr", expr)]),
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
    const INPUT_TYPE: PyStaticExpr = T::INPUT_TYPE;

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
    const OUTPUT_TYPE: PyStaticExpr = T::OUTPUT_TYPE;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        self.into_inner().into_pyobject(py)
    }
}

impl<T: Repr> Repr for Boxed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
