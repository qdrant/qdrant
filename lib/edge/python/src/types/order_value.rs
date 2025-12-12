use std::fmt;

use pyo3::prelude::*;
use segment::data_types::order_by::OrderValue;

use crate::repr::*;

#[derive(IntoPyObject)]
pub enum PyOrderValue {
    Int(i64),
    Float(f64),
}

impl Repr for PyOrderValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(int) => int.fmt(f),
            Self::Float(float) => float.fmt(f),
        }
    }
}

impl From<OrderValue> for PyOrderValue {
    fn from(value: OrderValue) -> Self {
        match value {
            OrderValue::Int(int) => Self::Int(int),
            OrderValue::Float(float) => Self::Float(float),
        }
    }
}
