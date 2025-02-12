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

impl FormulaScorer<'_> {
    pub fn score(&self, point_id: PointOffsetType) -> OperationResult<ScoreType> {
        // Collect all variables
        let mut variables = HashMap::new();
        for (path, retriever) in &self.payload_retrievers {
            if let Some(value) = retriever(point_id) {
                variables.insert(path.clone(), value);
            }
        }

        // Collect all evaluated conditions
        let mut conditions = HashMap::new();
        for (condition_id, checker) in &self.condition_checkers {
            let result = checker(point_id);
            conditions.insert(*condition_id, result);
        }

        // Collect all scores for this point in the prefetch results
        let mut scores = Vec::with_capacity(self.prefetches_scores.len());
        for score_map in self.prefetches_scores.iter() {
            let score = score_map.get(&point_id).copied();
            scores.push(score);
        }

        self.formula
            .evaluate(&scores, &variables, &conditions, &self.defaults)
    }
}

impl Expression {
    /// Evaluate the expression with the given scores, variables and conditions
    ///
    /// # Arguments
    ///
    /// * `scores` - The scores in each prefetch for the point
    /// * `variables` - The retrieved variables for the point. If a variable is not found, the default will be used.
    /// * `conditions` - The evaluated conditions for the point. All conditions must be provided.
    /// * `defaults` - The default values for all variables
    pub fn evaluate(
        &self,
        scores: &[Option<ScoreType>],
        variables: &HashMap<JsonPath, Value>,
        conditions: &HashMap<ConditionId, bool>,
        defaults: &HashMap<VariableId, Value>,
    ) -> OperationResult<ScoreType> {
        match self {
            Expression::Constant(c) => Ok(*c),
            Expression::Variable(v) => match v {
                VariableId::Score(idx) => Ok(scores[*idx]
                    .or_else(|| {
                        defaults
                            .get(&VariableId::Score(*idx))
                            // if `as_f64` fails, we use the default score
                            .and_then(|v| v.as_f64())
                            .map(|v| v as ScoreType)
                    })
                    .unwrap_or(DEFAULT_SCORE)),
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
    /// Evaluate the operation with the given scores, variables and conditions
    ///
    /// # Arguments
    ///
    /// * `scores` - The scores in each prefetch for the point
    /// * `variables` - The retrieved variables for the point. If a variable is not found, the default will be used.
    /// * `conditions` - The evaluated conditions for the point. All conditions must be provided.
    /// * `defaults` - The default values for all variables
    fn evaluate(
        &self,
        scores: &[Option<ScoreType>],
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