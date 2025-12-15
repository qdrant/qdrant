pub mod expression;
pub mod expression_interface;

use std::collections::HashMap;
use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::index::query_optimization::rescore_formula::parsed_formula::{
    DecayKind, ParsedFormula,
};
use shard::query::formula::{ExpressionInternal, FormulaInternal};

pub use self::expression::*;
pub use self::expression_interface::*;
use crate::repr::*;
use crate::types::PyValue;

#[pyclass(name = "Formula")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFormula(pub ParsedFormula);

#[pyclass_repr]
#[pymethods]
impl PyFormula {
    #[new]
    pub fn new(formula: PyExpression, defaults: HashMap<String, PyValue>) -> PyResult<Self> {
        let formula = FormulaInternal {
            formula: ExpressionInternal::from(formula),
            defaults: PyValue::peel_map(defaults),
        };

        let formula = ParsedFormula::try_from(formula)
            .map_err(|err| PyValueError::new_err(format!("failed to parse formula: {err}")))?;

        Ok(Self(formula))
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
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

#[pymethods]
impl PyDecayKind {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyDecayKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            PyDecayKind::Lin => "Lin",
            PyDecayKind::Gauss => "Gauss",
            PyDecayKind::Exp => "Exp",
        };

        f.simple_enum::<Self>(repr)
    }
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
