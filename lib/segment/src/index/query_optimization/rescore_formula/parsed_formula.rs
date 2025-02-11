use std::collections::HashMap;
use std::ops::Neg;

use common::types::ScoreType;
use geo::{Distance, Haversine};
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
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
    pub fn evaluate(
        &self,
        scores: &[ScoreType],
        variables: &HashMap<JsonPath, Value>,
        conditions: &HashMap<ConditionId, bool>,
        defaults: &HashMap<VariableId, Value>,
    ) -> OperationResult<ScoreType> {
        match self {
            Expression::Constant(c) => Ok(*c),
            Expression::Variable(v) => match v {
                VariableId::Score(idx) => Ok(scores[*idx]),
                VariableId::Payload(path) => variables
                    .get(path)
                    .and_then(|value| value.as_f64())
                    .or_else(|| {
                        defaults
                            .get(&VariableId::Payload(path.clone()))
                            .and_then(|value| value.as_f64())
                    })
                    .ok_or_else(|| OperationError::VariableTypeError {
                        field_name: path.clone(),
                        expected_type: "number".into(),
                    })
                    .map(|v| v as ScoreType),
                VariableId::Condition(id) => {
                    let value = conditions
                        .get(id)
                        .expect("All conditions should be provided");
                    let score = if *value { 1.0 } else { 0.0 };
                    Ok(score)
                }
            },
            Expression::Operation(op) => op.evaluate(scores, variables, conditions, defaults),
        }
    }
}

impl Operation {
    fn evaluate(
        &self,
        scores: &[ScoreType],
        variables: &HashMap<JsonPath, Value>,
        conditions: &HashMap<ConditionId, bool>,
        defaults: &HashMap<VariableId, Value>,
    ) -> OperationResult<ScoreType> {
        match self {
            Operation::Mult(expressions) => expressions.iter().try_fold(1.0, |acc, expr| {
                let value = expr.evaluate(scores, variables, conditions, defaults)?;
                Ok(acc * value)
            }),
            Operation::Sum(expressions) => expressions.iter().try_fold(0.0, |acc, expr| {
                let value = expr.evaluate(scores, variables, conditions, defaults)?;
                Ok(acc + value)
            }),
            Operation::Div {
                left,
                right,
                by_zero_default,
            } => {
                let left = left.evaluate(scores, variables, conditions, defaults)?;
                let right = right.evaluate(scores, variables, conditions, defaults)?;
                if right == 0.0 {
                    Ok(*by_zero_default)
                } else {
                    Ok(left / right)
                }
            }
            Operation::Neg(expr) => {
                let value = expr.evaluate(scores, variables, conditions, defaults)?;
                Ok(value.neg())
            }
            Operation::GeoDistance { origin, key } => {
                let value: GeoPoint = variables
                    .get(key)
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                    .or_else(|| {
                        defaults
                            .get(&VariableId::Payload(key.clone()))
                            .and_then(|value| serde_json::from_value(value.clone()).ok())
                    })
                    .ok_or_else(|| OperationError::VariableTypeError {
                        field_name: key.clone(),
                        expected_type: "geo point".into(),
                    })?;

                Ok(Haversine::distance((*origin).into(), value.into()) as ScoreType)
            }
        }
    }
}
