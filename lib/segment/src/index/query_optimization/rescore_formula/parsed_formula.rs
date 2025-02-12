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
