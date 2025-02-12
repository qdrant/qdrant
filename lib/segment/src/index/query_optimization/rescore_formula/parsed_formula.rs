use std::collections::HashMap;

use common::types::ScoreType;
use serde_json::Value;

use crate::json_path::JsonPath;
use crate::types::{Condition, GeoPoint};

struct VarMetadata {
    /// Default value when it is not found
    default: Value,
}

pub type ConditionId = usize;

pub struct ParsedFormula {
    /// Variables used in the formula
    payload_vars: HashMap<JsonPath, VarMetadata>,

    /// Conditions used in the formula
    conditions: HashMap<ConditionId, Condition>,

    /// Root of the formula expression
    formula: Expression,
}

pub enum Expression {
    Constant(ScoreType),
    Variable(VariableId),
    Operation(Operation),
}

#[derive(Hash, Eq, PartialEq)]
pub enum VariableId {
    /// Score index
    Score(usize),
    /// Payload field
    Payload(JsonPath),
    /// Condition index
    Condition(ConditionId),
}

pub enum Operation {
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

impl Expression {
    pub fn new_sum(expressions: Vec<Expression>) -> Self {
        Expression::Operation(Operation::Sum(expressions))
    }

    pub fn new_mult(expressions: Vec<Expression>) -> Self {
        Expression::Operation(Operation::Mult(expressions))
    }

    pub fn new_div(left: Expression, right: Expression, by_zero_default: ScoreType) -> Self {
        Expression::Operation(Operation::Div {
            left: Box::new(left),
            right: Box::new(right),
            by_zero_default,
        })
    }

    pub fn new_neg(expression: Expression) -> Self {
        Expression::Operation(Operation::Neg(Box::new(expression)))
    }

    pub fn new_geo_distance(origin: GeoPoint, key: JsonPath) -> Self {
        Expression::Operation(Operation::GeoDistance { origin, key })
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
