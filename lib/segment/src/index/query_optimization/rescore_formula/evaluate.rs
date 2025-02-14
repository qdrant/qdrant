use std::collections::HashMap;
use std::ops::Neg;

use common::types::ScoreType;
use geo::{Distance, Haversine};
use serde_json::Value;

use super::parsed_formula::{Expression, Operation, VariableId};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::json_path::JsonPath;
use crate::types::GeoPoint;

const DEFAULT_SCORE: ScoreType = 0.0;

/// Ephemeral type to hold all formula variables for a single point
pub(super) struct PointVariables<'a> {
    /// The scores in each prefetch for the point.
    prefetch_scores: Vec<Option<ScoreType>>,
    /// The retrieved values for the point. Not all jsonpaths in the formula may be present here.
    payload_values: HashMap<JsonPath, Value>,
    /// The evaluated conditions for the point. All conditions in the formula must be present.
    conditions: Vec<bool>,
    /// The default values for all variables
    defaults: &'a HashMap<VariableId, Value>,
}

impl Expression {
    /// Evaluate the expression with the given variables
    pub(super) fn evaluate(&self, vars: &PointVariables) -> OperationResult<ScoreType> {
        match self {
            Expression::Constant(c) => Ok(*c),
            Expression::Variable(v) => match v {
                VariableId::Score(prefetch_idx) => Ok(vars
                    .prefetch_scores
                    .get(*prefetch_idx)
                    .copied()
                    .flatten()
                    .or_else(|| {
                        vars.defaults
                            .get(&VariableId::Score(*prefetch_idx))
                            // if `as_f64` fails, we use the default score
                            .and_then(|v| v.as_f64())
                            .map(|v| v as ScoreType)
                    })
                    .unwrap_or(DEFAULT_SCORE)),
                VariableId::Payload(path) => Ok(vars
                    .payload_values
                    .get(path)
                    .and_then(|value| value.as_f64())
                    .or_else(|| {
                        vars.defaults
                            .get(&VariableId::Payload(path.clone()))
                            .and_then(|value| value.as_f64())
                    })
                    .map(|v| v as ScoreType)
                    .unwrap_or(DEFAULT_SCORE)),
                VariableId::Condition(id) => {
                    let value = vars.conditions[*id];
                    let score = if value { 1.0 } else { 0.0 };
                    Ok(score)
                }
            },
            Expression::Operation(op) => op.evaluate(vars),
        }
    }
}

impl Operation {
    /// Evaluate the operation with the given variables
    fn evaluate(&self, vars: &PointVariables) -> OperationResult<ScoreType> {
        match self {
            Operation::Mult(expressions) => expressions.iter().try_fold(1.0, |acc, expr| {
                let value = expr.evaluate(vars)?;
                Ok(acc * value)
            }),
            Operation::Sum(expressions) => expressions.iter().try_fold(0.0, |acc, expr| {
                let value = expr.evaluate(vars)?;
                Ok(acc + value)
            }),
            Operation::Div {
                left,
                right,
                by_zero_default,
            } => {
                let left = left.evaluate(vars)?;
                let right = right.evaluate(vars)?;
                if right == 0.0 {
                    Ok(*by_zero_default)
                } else {
                    Ok(left / right)
                }
            }
            Operation::Neg(expr) => {
                let value = expr.evaluate(vars)?;
                Ok(value.neg())
            }
            Operation::GeoDistance { origin, key } => {
                let value: GeoPoint = vars
                    .payload_values
                    .get(key)
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                    .or_else(|| {
                        vars.defaults
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

#[cfg(test)]
#[cfg(feature = "testing")]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::json_path::JsonPath;

    const FIELD_NAME: &str = "number";
    const NO_VALUE_FIELD_NAME: &str = "no_number";
    const GEO_FIELD_NAME: &str = "geo_point";
    const NO_VALUE_GEO_POINT: &str = "no_value_geo_point";

    fn make_point_variables(defaults: &HashMap<VariableId, Value>) -> PointVariables {
        let mut payload_values = HashMap::new();
        payload_values.insert(JsonPath::new(FIELD_NAME), json!(85.0));
        payload_values.insert(
            JsonPath::new(GEO_FIELD_NAME),
            json!({"lat": 25.628482424190565, "lon": -100.23881855976}),
        );

        let conditions = vec![true, false];

        PointVariables {
            prefetch_scores: vec![Some(1.0), Some(2.0)],
            payload_values,
            conditions,
            defaults,
        }
    }

    #[rstest]
    // Basic expressions, just variables
    #[case(Expression::Constant(5.0), 5.0)]
    #[case(Expression::new_score_id(0), 1.0)]
    #[case(Expression::new_score_id(1), 2.0)]
    #[case(Expression::new_payload_id(FIELD_NAME), 85.0)]
    #[case(Expression::new_condition_id(0), 1.0)]
    #[case(Expression::new_condition_id(1), 0.0)]
    // Operations
    #[case(Expression::new_sum(vec![
        Expression::Constant(1.0),
        Expression::new_score_id(0),
        Expression::new_payload_id(FIELD_NAME),
        Expression::new_condition_id(0),
    ]), 1.0 + 1.0 + 85.0 + 1.0)]
    #[case(Expression::new_mult(vec![
        Expression::Constant(2.0),
        Expression::new_score_id(0),
        Expression::new_payload_id(FIELD_NAME),
        Expression::new_condition_id(0),
    ]), 2.0 * 1.0 * 85.0 * 1.0)]
    #[case(Expression::Operation(Operation::Div {
        left: Box::new(Expression::Constant(10.0)),
        right: Box::new(Expression::new_score_id(0)),
        by_zero_default: f32::INFINITY,
    }), 10.0 / 1.0)]
    #[case(Expression::new_neg(Expression::Constant(10.0)), -10.0)]
    #[case(Expression::new_geo_distance(GeoPoint { lat: 25.717877679163667, lon: -100.43383200156751 }, JsonPath::new(GEO_FIELD_NAME)), 21926.494)]
    #[should_panic(
        expected = r#"called `Result::unwrap()` on an `Err` value: VariableTypeError { field_name: JsonPath { first_key: "number", rest: [] }, expected_type: "geo point" }"#
    )]
    #[case(Expression::new_geo_distance(GeoPoint { lat: 25.717877679163667, lon: -100.43383200156751 }, JsonPath::new(FIELD_NAME)), 0.0)]
    #[test]
    fn test_evaluation(#[case] expr: Expression, #[case] expected: ScoreType) {
        let defaults = HashMap::new();
        let vars = make_point_variables(&defaults);

        assert_eq!(expr.evaluate(&vars).unwrap(), expected);
    }

    // Default values
    #[rstest]
    // Defined default score
    #[case(Expression::new_score_id(3), 1.5)]
    // score idx not defined
    #[case(Expression::new_score_id(10), DEFAULT_SCORE)]
    // missing value in payload
    #[case(Expression::new_payload_id(NO_VALUE_FIELD_NAME), 85.0)]
    // missing value and no default value provided
    #[case(Expression::new_payload_id("missing_field"), DEFAULT_SCORE)]
    // geo distance with default value
    #[case(Expression::new_geo_distance(GeoPoint { lat: 25.717877679163667, lon: -100.43383200156751 }, JsonPath::new(NO_VALUE_GEO_POINT)), 90951.3)]
    #[test]
    fn test_default_values(#[case] expr: Expression, #[case] expected: ScoreType) {
        let defaults = [
            (VariableId::Score(3), json!(1.5)),
            (
                VariableId::Payload(JsonPath::new(NO_VALUE_FIELD_NAME)),
                json!(85.0),
            ),
            (
                VariableId::Payload(JsonPath::new(NO_VALUE_GEO_POINT)),
                json!({"lat": 25.0, "lon": -100.0}),
            ),
        ]
        .into_iter()
        .collect();

        let vars = make_point_variables(&defaults);

        assert_eq!(expr.evaluate(&vars).unwrap(), expected);
    }
}
