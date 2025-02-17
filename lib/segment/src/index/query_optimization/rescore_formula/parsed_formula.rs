use std::collections::{HashMap, HashSet};

use common::types::ScoreType;
use serde_json::Value;

use crate::json_path::JsonPath;
use crate::types::{Condition, GeoPoint};

pub type ConditionId = usize;

pub struct ParsedFormula {
    /// Variables used in the formula
    pub(super) payload_vars: HashSet<JsonPath>,

    /// Conditions used in the formula. Their index in the array is used as a variable id
    pub(super) conditions: Vec<Condition>,

    /// Defaults to use when variable is not found
    pub(super) defaults: HashMap<VariableId, Value>,

    /// Root of the formula expression
    pub(super) formula: Expression,
}

#[derive(Clone)]
pub enum Expression {
    // Scalars
    Constant(ScoreType),
    Variable(VariableId),

    // Operations
    Mult(Vec<Expression>),
    Sum(Vec<Expression>),
    Div {
        left: Box<Expression>,
        right: Box<Expression>,
        by_zero_default: ScoreType,
    },
    Neg(Box<Expression>),
    GeoDistance {
        origin: GeoPoint,
        key: JsonPath,
    },
}

#[derive(Clone, Hash, Eq, PartialEq)]
pub enum VariableId {
    /// Score index
    Score(usize),
    /// Payload field
    Payload(JsonPath),
    /// Condition index
    Condition(ConditionId),
}

impl Expression {
    pub fn new_div(left: Expression, right: Expression, by_zero_default: ScoreType) -> Self {
        Expression::Div {
            left: Box::new(left),
            right: Box::new(right),
            by_zero_default,
        }
    }

    pub fn new_neg(expression: Expression) -> Self {
        Expression::Neg(Box::new(expression))
    }

    pub fn new_geo_distance(origin: GeoPoint, key: JsonPath) -> Self {
        Expression::GeoDistance { origin, key }
    }

    #[cfg(feature = "testing")]
    pub fn new_payload_id(path: &str) -> Self {
        Expression::Variable(VariableId::Payload(JsonPath::new(path)))
    }

    pub fn new_score_id(index: usize) -> Self {
        Expression::Variable(VariableId::Score(index))
    }

    pub fn new_condition_id(index: ConditionId) -> Self {
        Expression::Variable(VariableId::Condition(index))
    }
}
