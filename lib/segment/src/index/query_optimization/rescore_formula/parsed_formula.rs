use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use common::types::ScoreType;
use serde_json::Value;

use crate::json_path::{JsonPath, JsonPathItem};
use crate::types::{Condition, GeoPoint};

const SCORE_KEYWORD: &str = "score";

pub type ConditionId = usize;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedFormula {
    /// Variables used in the formula
    pub payload_vars: HashSet<JsonPath>,

    /// Conditions used in the formula. Their index in the array is used as a variable id
    pub conditions: Vec<Condition>,

    /// Defaults to use when variable is not found
    pub defaults: HashMap<VariableId, Value>,

    /// Root of the formula expression
    pub formula: ParsedExpression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedExpression {
    // Scalars
    Constant(ScoreType),
    Variable(VariableId),

    // Operations
    Mult(Vec<ParsedExpression>),
    Sum(Vec<ParsedExpression>),
    Div {
        left: Box<ParsedExpression>,
        right: Box<ParsedExpression>,
        by_zero_default: ScoreType,
    },
    Neg(Box<ParsedExpression>),
    GeoDistance {
        origin: GeoPoint,
        key: JsonPath,
    },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum VariableId {
    /// Score index
    Score(usize),
    /// Payload field
    Payload(JsonPath),
    /// Condition index
    Condition(ConditionId),
}

impl ParsedExpression {
    /// Default value for division by zero
    const fn by_zero_default() -> ScoreType {
        f32::INFINITY
    }

    pub fn new_div(
        left: ParsedExpression,
        right: ParsedExpression,
        by_zero_default: Option<ScoreType>,
    ) -> Self {
        ParsedExpression::Div {
            left: Box::new(left),
            right: Box::new(right),
            by_zero_default: by_zero_default.unwrap_or(Self::by_zero_default()),
        }
    }

    pub fn new_neg(expression: ParsedExpression) -> Self {
        ParsedExpression::Neg(Box::new(expression))
    }

    pub fn new_geo_distance(origin: GeoPoint, key: JsonPath) -> Self {
        ParsedExpression::GeoDistance { origin, key }
    }

    pub fn new_payload_id(path: JsonPath) -> Self {
        ParsedExpression::Variable(VariableId::Payload(path))
    }

    pub fn new_score_id(index: usize) -> Self {
        ParsedExpression::Variable(VariableId::Score(index))
    }

    pub fn new_condition_id(index: ConditionId) -> Self {
        ParsedExpression::Variable(VariableId::Condition(index))
    }
}

impl FromStr for VariableId {
    type Err = String;

    fn from_str(var_str: &str) -> Result<Self, Self::Err> {
        let var_id = match var_str.strip_prefix("$") {
            Some(score) => {
                // parse as reserved word
                let json_path = score
                    .parse::<JsonPath>()
                    .map_err(|_| format!("Invalid reserved variable: {var_str}"))?;
                match json_path.first_key.as_str() {
                    SCORE_KEYWORD => match &json_path.rest[..] {
                        // Default prefetch index, like "$score"
                        [] => VariableId::Score(0),
                        // Specifies prefetch index, like "$score[2]"
                        [JsonPathItem::Index(idx)] => VariableId::Score(*idx),
                        _ => {
                            // Only direct index is supported
                            return Err(format!("Invalid reserved variable: {var_str}"));
                        }
                    },
                    _ => {
                        // No other reserved words are supported
                        return Err(format!("Invalid reserved word: {var_str}"));
                    }
                }
            }
            None => {
                // parse as regular payload variable
                let parsed = var_str
                    .parse()
                    .map_err(|_| format!("Invalid payload variable: {var_str}"))?;
                VariableId::Payload(parsed)
            }
        };
        Ok(var_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_id_from_str() {
        // Test score variables
        assert_eq!(
            VariableId::from_str("$score").unwrap(),
            VariableId::Score(0)
        );
        assert_eq!(
            VariableId::from_str("$score[0]").unwrap(),
            VariableId::Score(0)
        );
        assert_eq!(
            VariableId::from_str("$score[1]").unwrap(),
            VariableId::Score(1)
        );
        assert!(VariableId::from_str("$score.invalid").is_err());
        assert!(VariableId::from_str("$score[1][2]").is_err());
        assert!(VariableId::from_str("$score[]").is_err());

        // Test invalid reserved words
        assert!(VariableId::from_str("$invalid").is_err());

        // Test payload variables
        assert_eq!(
            VariableId::from_str("field").unwrap(),
            VariableId::Payload("field".parse().unwrap())
        );
        assert_eq!(
            VariableId::from_str("field.nested").unwrap(),
            VariableId::Payload("field.nested".parse().unwrap())
        );
        assert_eq!(
            VariableId::from_str("field[0]").unwrap(),
            VariableId::Payload("field[0]".parse().unwrap())
        );
        assert!(VariableId::from_str("").is_err());
    }
}
