use std::collections::HashMap;
use std::ops::Neg;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use geo::{Distance, Haversine};
use serde_json::Value;

use super::parsed_formula::{ParsedExpression, ParsedFormula, VariableId};
use super::value_retriever::VariableRetrieverFn;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::query_optimization::optimized_filter::{check_condition, OptimizedCondition};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::GeoPoint;

const DEFAULT_SCORE: ScoreType = 0.0;

/// A scorer to evaluate the same formula for many points
pub struct FormulaScorer<'a> {
    /// The formula to evaluate
    formula: ParsedExpression,
    /// One hashmap for each prefetch results
    prefetches_scores: &'a [AHashMap<PointOffsetType, ScoreType>],
    /// Payload key -> retriever function
    payload_retrievers: HashMap<JsonPath, VariableRetrieverFn<'a>>,
    /// Condition id -> checker function
    condition_checkers: Vec<OptimizedCondition<'a>>,
    /// Default values for all variables
    defaults: HashMap<VariableId, Value>,
}

impl StructPayloadIndex {
    pub fn formula_scorer<'s, 'q>(
        &'s self,
        parsed_formula: &'q ParsedFormula,
        prefetches_scores: &'q [AHashMap<PointOffsetType, ScoreType>],
        hw_counter: &'q HardwareCounterCell,
    ) -> FormulaScorer<'q>
    where
        's: 'q,
    {
        let ParsedFormula {
            payload_vars,
            conditions,
            defaults,
            formula,
        } = parsed_formula;

        let payload_retrievers = self.retrievers_map(payload_vars.clone(), hw_counter);

        let payload_provider = PayloadProvider::new(self.payload.clone());
        let total = self.available_point_count();
        let condition_checkers = self
            .convert_conditions(conditions, payload_provider, total, hw_counter)
            .into_iter()
            .map(|(checker, _estimation)| checker)
            .collect();

        FormulaScorer {
            formula: formula.clone(),
            prefetches_scores,
            payload_retrievers,
            condition_checkers,
            defaults: defaults.clone(),
        }
    }
}

impl FormulaScorer<'_> {
    /// Evaluate the formula for the given point
    pub fn score(&self, point_id: PointOffsetType) -> OperationResult<ScoreType> {
        self.eval_expression(&self.formula, point_id)
    }

    /// Evaluate the expression recursively
    fn eval_expression(
        &self,
        expression: &ParsedExpression,
        point_id: PointOffsetType,
    ) -> OperationResult<ScoreType> {
        match expression {
            ParsedExpression::Constant(c) => Ok(*c),
            ParsedExpression::Variable(v) => match v {
                VariableId::Score(prefetch_idx) => Ok(self
                    .prefetches_scores
                    .get(*prefetch_idx)
                    .and_then(|scores| scores.get(&point_id))
                    .copied()
                    .or_else(|| {
                        self.defaults
                            // if there is no score, or it isn't a number, we use the default score
                            .get(&VariableId::Score(*prefetch_idx))
                            .and_then(|value| value.as_f64())
                            .map(|score| score as ScoreType)
                    })
                    .unwrap_or(DEFAULT_SCORE)),
                VariableId::Payload(path) => Ok(self
                    .payload_retrievers
                    .get(path)
                    .and_then(|retriever| retriever(point_id))
                    .and_then(|value| value.as_f64())
                    .or_else(|| {
                        self.defaults
                            .get(&VariableId::Payload(path.clone()))
                            .and_then(|value| value.as_f64())
                    })
                    .map(|v| v as ScoreType)
                    .unwrap_or(DEFAULT_SCORE)),
                VariableId::Condition(id) => {
                    let value = check_condition(&self.condition_checkers[*id], point_id);
                    let score = if value { 1.0 } else { 0.0 };
                    Ok(score)
                }
            },
            ParsedExpression::Mult(expressions) => {
                let mut product = 1.0;
                for expr in expressions {
                    let value = self.eval_expression(expr, point_id)?;
                    // shortcut on multiplication by zero
                    if value == 0.0 {
                        return Ok(0.0);
                    }
                    product *= value;
                }
                Ok(product)
            }
            ParsedExpression::Sum(expressions) => expressions.iter().try_fold(0.0, |acc, expr| {
                let value = self.eval_expression(expr, point_id)?;
                Ok(acc + value)
            }),
            ParsedExpression::Div {
                left,
                right,
                by_zero_default,
            } => {
                let left = self.eval_expression(left, point_id)?;
                // shortcut on numerator zero
                if left == 0.0 {
                    return Ok(0.0);
                }
                let right = self.eval_expression(right, point_id)?;
                // avoid division by zero
                if right == 0.0 {
                    Ok(*by_zero_default)
                } else {
                    Ok(left / right)
                }
            }
            ParsedExpression::Neg(expr) => {
                let value = self.eval_expression(expr, point_id)?;
                Ok(value.neg())
            }
            ParsedExpression::GeoDistance { origin, key } => {
                let value: GeoPoint = self
                    .payload_retrievers
                    .get(key)
                    .and_then(|retriever| retriever(point_id))
                    .and_then(|value| serde_json::from_value(value).ok())
                    .or_else(|| {
                        self.defaults
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

    // self_cell just to be able to create FormulaScorer with a "reference" to fixture scores
    self_cell::self_cell!(
        struct ScorerFixture {
            owner: Vec<AHashMap<u32, f32>>,
            #[covariant]
            dependent: FormulaScorer,
        }
    );

    fn make_formula_scorer(defaults: &HashMap<VariableId, Value>) -> ScorerFixture {
        let scores = vec![
            [(0, 1.0)].into_iter().collect(),
            [(0, 2.0)].into_iter().collect(),
        ];

        ScorerFixture::new(scores, |prefetches_scores| {
            let mut payload_retrievers: HashMap<JsonPath, VariableRetrieverFn> = HashMap::new();
            payload_retrievers.insert(JsonPath::new(FIELD_NAME), Box::new(|_| Some(json!(85.0))));
            payload_retrievers.insert(
                JsonPath::new(GEO_FIELD_NAME),
                Box::new(|_| Some(json!({"lat": 25.628482424190565, "lon": -100.23881855976}))),
            );

            let condition_checkers = vec![
                OptimizedCondition::Checker(Box::new(|_| true)),
                OptimizedCondition::Checker(Box::new(|_| false)),
            ];

            FormulaScorer {
                formula: ParsedExpression::Constant(0.0),
                prefetches_scores,
                payload_retrievers,
                condition_checkers,
                defaults: defaults.clone(),
            }
        })
    }

    #[rstest]
    // Basic expressions, just variables
    #[case(ParsedExpression::Constant(5.0), 5.0)]
    #[case(ParsedExpression::new_score_id(0), 1.0)]
    #[case(ParsedExpression::new_score_id(1), 2.0)]
    #[case(ParsedExpression::new_payload_id(JsonPath::new(FIELD_NAME)), 85.0)]
    #[case(ParsedExpression::new_condition_id(0), 1.0)]
    #[case(ParsedExpression::new_condition_id(1), 0.0)]
    // Operations
    #[case(ParsedExpression::Sum(vec![
        ParsedExpression::Constant(1.0),
        ParsedExpression::new_score_id(0),
        ParsedExpression::new_payload_id(JsonPath::new(FIELD_NAME)),
        ParsedExpression::new_condition_id(0),
    ]), 1.0 + 1.0 + 85.0 + 1.0)]
    #[case(ParsedExpression::Mult(vec![
        ParsedExpression::Constant(2.0),
        ParsedExpression::new_score_id(0),
        ParsedExpression::new_payload_id(JsonPath::new(FIELD_NAME)),
        ParsedExpression::new_condition_id(0),
    ]), 2.0 * 1.0 * 85.0 * 1.0)]
    #[case(ParsedExpression::Div {
        left: Box::new(ParsedExpression::Constant(10.0)),
        right: Box::new(ParsedExpression::new_score_id(0)),
        by_zero_default: f32::INFINITY,
    }, 10.0 / 1.0)]
    #[case(ParsedExpression::new_neg(ParsedExpression::Constant(10.0)), -10.0)]
    #[case(ParsedExpression::new_geo_distance(GeoPoint { lat: 25.717877679163667, lon: -100.43383200156751 }, JsonPath::new(GEO_FIELD_NAME)), 21926.494)]
    #[should_panic(
        expected = r#"called `Result::unwrap()` on an `Err` value: VariableTypeError { field_name: JsonPath { first_key: "number", rest: [] }, expected_type: "geo point" }"#
    )]
    #[case(ParsedExpression::new_geo_distance(GeoPoint { lat: 25.717877679163667, lon: -100.43383200156751 }, JsonPath::new(FIELD_NAME)), 0.0)]
    #[test]
    fn test_evaluation(#[case] expr: ParsedExpression, #[case] expected: ScoreType) {
        let defaults = HashMap::new();
        let scorer_fixture = make_formula_scorer(&defaults);

        let scorer = scorer_fixture.borrow_dependent();

        assert_eq!(scorer.eval_expression(&expr, 0).unwrap(), expected);
    }

    // Default values
    #[rstest]
    // Defined default score
    #[case(ParsedExpression::new_score_id(3), 1.5)]
    // score idx not defined
    #[case(ParsedExpression::new_score_id(10), DEFAULT_SCORE)]
    // missing value in payload
    #[case(
        ParsedExpression::new_payload_id(JsonPath::new(NO_VALUE_FIELD_NAME)),
        85.0
    )]
    // missing value and no default value provided
    #[case(
        ParsedExpression::new_payload_id(JsonPath::new("missing_field")),
        DEFAULT_SCORE
    )]
    // geo distance with default value
    #[case(ParsedExpression::new_geo_distance(GeoPoint { lat: 25.717877679163667, lon: -100.43383200156751 }, JsonPath::new(NO_VALUE_GEO_POINT)), 90951.3)]
    #[test]
    fn test_default_values(#[case] expr: ParsedExpression, #[case] expected: ScoreType) {
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

        let scorer_fixture = make_formula_scorer(&defaults);

        let scorer = scorer_fixture.borrow_dependent();

        assert_eq!(scorer.eval_expression(&expr, 0).unwrap(), expected);
    }
}
