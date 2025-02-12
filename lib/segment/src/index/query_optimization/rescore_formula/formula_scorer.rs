use std::collections::HashMap;
use std::ops::Neg;

use common::types::{PointOffsetType, ScoreType};
use geo::{Distance, Haversine};
use serde_json::Value;

use super::parsed_formula::{ConditionId, Expression, Operation, VariableId};
use super::value_retriever::VariableRetrieverFn;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::json_path::JsonPath;
use crate::types::GeoPoint;

const DEFAULT_SCORE: ScoreType = 0.0;

pub struct FormulaScorer<'a> {
    /// The formula to evaluate
    formula: Expression,
    /// One hashmap for each prefetch results
    prefetches_scores: &'a [HashMap<PointOffsetType, ScoreType>],
    /// Payload key -> retriever function
    payload_retrievers: HashMap<JsonPath, VariableRetrieverFn<'a>>,
    /// Condition id -> checker function
    condition_checkers: HashMap<ConditionId, ConditionCheckerFn<'a>>,
    /// Default values for all variables
    defaults: HashMap<VariableId, Value>,
}

struct PointVariables<'a> {
    /// The scores in each prefetch for the point.
    prefetch_scores: Vec<Option<ScoreType>>,
    /// The retrieved values for the point. Not all jsonpaths in the formula may be present here.
    payload_values: HashMap<JsonPath, Value>,
    /// The evaluated conditions for the point. All conditions in the formula must be present.
    conditions: HashMap<ConditionId, bool>,
    /// The default values for all variables
    defaults: &'a HashMap<VariableId, Value>,
}

impl FormulaScorer<'_> {
    pub fn score(&self, point_id: PointOffsetType) -> OperationResult<ScoreType> {
        // Collect all variables
        let mut payload_values = HashMap::new();
        for (path, retriever) in &self.payload_retrievers {
            if let Some(value) = retriever(point_id) {
                payload_values.insert(path.clone(), value);
            }
        }

        // Collect all evaluated conditions
        let mut conditions = HashMap::new();
        for (condition_id, checker) in &self.condition_checkers {
            let result = checker(point_id);
            conditions.insert(*condition_id, result);
        }

        // Collect all scores for this point in the prefetch results
        let mut prefetch_scores = Vec::with_capacity(self.prefetches_scores.len());
        for score_map in self.prefetches_scores.iter() {
            let score = score_map.get(&point_id).copied();
            prefetch_scores.push(score);
        }

        let vars = PointVariables {
            prefetch_scores,
            payload_values,
            conditions,
            defaults: &self.defaults,
        };

        self.formula.evaluate(&vars)
    }
}

impl Expression {
    /// Evaluate the expression with the given scores, variables and conditions
    fn evaluate(&self, vars: &PointVariables) -> OperationResult<ScoreType> {
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
                    let value = vars
                        .conditions
                        .get(id)
                        .expect("All conditions should be provided");
                    let score = if *value { 1.0 } else { 0.0 };
                    Ok(score)
                }
            },
            Expression::Operation(op) => op.evaluate(vars),
        }
    }
}

impl Operation {
    /// Evaluate the operation with the given scores, variables and conditions
    ///
    /// # Arguments
    ///
    /// * `scores` - The scores in each prefetch for the point
    /// * `variables` - The retrieved variables for the point. If a variable is not found, the default will be used.
    /// * `conditions` - The evaluated conditions for the point. All conditions must be provided.
    /// * `defaults` - The default values for all variables
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

    const FIELD_NAME: &str = "field_name";
    const NO_VALUE_FIELD_NAME: &str = "no_value_field_name";

    fn make_point_variables(defaults: &HashMap<VariableId, Value>) -> PointVariables {
        let payload_values = [(JsonPath::new(FIELD_NAME), json!(85.0))]
            .into_iter()
            .collect();

        let conditions = [(0, true), (1, false)].into_iter().collect();

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
    #[test]
    fn test_default_values(#[case] expr: Expression, #[case] expected: ScoreType) {
        let defaults = [
            (VariableId::Score(3), json!(1.5)),
            (
                VariableId::Payload(JsonPath::new(NO_VALUE_FIELD_NAME)),
                json!(85.0),
            ),
        ]
        .into_iter()
        .collect();

        let vars = make_point_variables(&defaults);

        assert_eq!(expr.evaluate(&vars).unwrap(), expected);
    }
}
