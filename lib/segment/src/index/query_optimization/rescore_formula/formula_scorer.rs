use std::collections::HashMap;
use std::ops::Neg;

use ahash::AHashMap;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use geo::{Distance, Haversine};
use serde_json::Value;

use super::parsed_formula::{
    DatetimeExpression, DecayKind, ParsedExpression, ParsedFormula, PreciseScore, VariableId,
};
use super::value_retriever::VariableRetrieverFn;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::query_optimization::optimized_filter::{OptimizedCondition, check_condition};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::{DateTimePayloadType, GeoPoint};

const DEFAULT_SCORE: PreciseScore = 0.0;
const DEFAULT_DECAY_TARGET: PreciseScore = 0.0;

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

pub trait FriendlyName {
    fn friendly_name() -> &'static str;
}

impl FriendlyName for PreciseScore {
    fn friendly_name() -> &'static str {
        "number"
    }
}

impl FriendlyName for GeoPoint {
    fn friendly_name() -> &'static str {
        "geo point"
    }
}

impl FriendlyName for DateTimePayloadType {
    fn friendly_name() -> &'static str {
        "datetime"
    }
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
            .and_then(|score| {
                let score_f32 = score as f32;
                if !score_f32.is_finite() {
                    return Err(OperationError::NonFiniteNumber {
                        expression: format!("{score} as f32 = {score_f32}"),
                    });
                }
                Ok(score_f32)
            })
    }

    /// Evaluate the expression recursively
    fn eval_expression(
        &self,
        expression: &ParsedExpression,
        point_id: PointOffsetType,
    ) -> OperationResult<PreciseScore> {
        match expression {
            ParsedExpression::Constant(c) => Ok(c.0),
            ParsedExpression::Variable(v) => match v {
                VariableId::Score(prefetch_idx) => Ok(self
                    .prefetches_scores
                    .get(*prefetch_idx)
                    .and_then(|scores| scores.get(&point_id))
                    .map(|score| PreciseScore::from(*score))
                    .or_else(|| {
                        self.defaults
                            // if there is no score, or it isn't a number, we use the default score
                            .get(&VariableId::Score(*prefetch_idx))
                            .and_then(|value| value.as_f64())
                    })
                    .unwrap_or(DEFAULT_SCORE)),
                VariableId::Payload(path) => {
                    self.get_parsed_payload_value(path, point_id, |value| {
                        value.as_f64().ok_or("Value is not a number")
                    })
                }
                VariableId::Condition(id) => {
                    let value = check_condition(&self.condition_checkers[*id], point_id);
                    let score = if value { 1.0 } else { 0.0 };
                    Ok(score)
                }
            },
            ParsedExpression::GeoDistance { origin, key } => {
                let value = self.get_parsed_payload_value(
                    key,
                    point_id,
                    serde_json::from_value::<GeoPoint>,
                )?;

                Ok(Haversine.distance((*origin).into(), value.into()))
            }
            ParsedExpression::Datetime(dt_expr) => {
                let datetime = match dt_expr {
                    DatetimeExpression::Constant(dt) => *dt,
                    DatetimeExpression::PayloadVariable(json_path) => {
                        self.get_parsed_payload_value(json_path, point_id, |value| {
                            value
                                // datetime index also returns the Serialize impl of datetime which is a string
                                .as_str()
                                .ok_or("Value is not a string")?
                                .parse::<DateTimePayloadType>()
                                .map_err(|e| e.to_string())
                        })?
                    }
                };
                // Convert from i64 to f64.
                // f64's 53 bits of sign + mantissa for microseconds means a span of exact equivalence of
                // about 285 years, after which precision starts dropping
                let float_micros = datetime.timestamp() as PreciseScore;

                // Convert to seconds
                let float_seconds = float_micros / 1_000_000.0;

                Ok(float_seconds)
            }
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

                if right == 0.0
                    && let Some(default) = by_zero_default
                {
                    return Ok(default.0);
                }

                let div_value = left / right;
                if div_value.is_finite() {
                    return Ok(div_value);
                }
                Err(OperationError::NonFiniteNumber {
                    expression: format!("{left}/{right} = {div_value}"),
                })
            }
            ParsedExpression::Neg(expr) => {
                let value = self.eval_expression(expr, point_id)?;
                Ok(value.neg())
            }
            ParsedExpression::Sqrt(expr) => {
                let value = self.eval_expression(expr, point_id)?;
                let sqrt_value = value.sqrt();
                if sqrt_value.is_finite() {
                    return Ok(sqrt_value);
                }
                Err(OperationError::NonFiniteNumber {
                    expression: format!("√{value} = {sqrt_value}"),
                })
            }
            ParsedExpression::Pow { base, exponent } => {
                let base_value = self.eval_expression(base, point_id)?;
                let exponent_value = self.eval_expression(exponent, point_id)?;
                let power = base_value.powf(exponent_value);
                if power.is_finite() {
                    return Ok(power);
                }
                Err(OperationError::NonFiniteNumber {
                    expression: format!("{base_value}^{exponent_value} = {power}"),
                })
            }
            ParsedExpression::Exp(parsed_expression) => {
                let value = self.eval_expression(parsed_expression, point_id)?;
                let exp_value = value.exp();
                if exp_value.is_finite() {
                    return Ok(exp_value);
                }
                Err(OperationError::NonFiniteNumber {
                    expression: format!("exp({value}) = {exp_value}"),
                })
            }
            ParsedExpression::Log10(expr) => {
                let value = self.eval_expression(expr, point_id)?;
                let log_value = value.log10();
                if log_value.is_finite() {
                    return Ok(log_value);
                }
                Err(OperationError::NonFiniteNumber {
                    expression: format!("log10({value}) = {log_value}"),
                })
            }
            ParsedExpression::Ln(expr) => {
                let value = self.eval_expression(expr, point_id)?;
                let ln_value = value.ln();
                if ln_value.is_finite() {
                    return Ok(ln_value);
                }
                Err(OperationError::NonFiniteNumber {
                    expression: format!("ln({value}) = {ln_value}"),
                })
            }
            ParsedExpression::Abs(expr) => {
                let value = self.eval_expression(expr, point_id)?;
                Ok(value.abs())
            }
            // Interactive formulas in https://www.desmos.com/calculator/htg0vrfmks
            ParsedExpression::Decay {
                kind,
                target,
                lambda,
                x,
            } => {
                let x = self.eval_expression(x, point_id)?;
                let target = if let Some(target) = target {
                    self.eval_expression(target, point_id)?
                } else {
                    DEFAULT_DECAY_TARGET
                };
                let decay = match kind {
                    DecayKind::Exp => exp_decay(x, target, lambda.0),
                    DecayKind::Gauss => gauss_decay(x, target, lambda.0),
                    DecayKind::Lin => linear_decay(x, target, lambda.0),
                };

                // All decay functions have a range of [0, 1], no need to check for bounds
                debug_assert!((0.0..=1.0).contains(&decay));

                Ok(decay)
            }
        }
    }

    fn get_payload_value(&self, json_path: &JsonPath, point_id: PointOffsetType) -> Option<Value> {
        self.payload_retrievers
            .get(json_path)
            .and_then(|retriever| {
                let mut multi_value = retriever(point_id);
                match multi_value.len() {
                    0 => None,
                    1 => Some(multi_value.pop().unwrap()),
                    _ => Some(Value::Array(multi_value.into_iter().collect())),
                }
            })
    }

    /// Tries to get a value from payload or from the defaults. Then tries to convert it to the desired type.
    fn get_parsed_payload_value<T, F, E>(
        &self,
        json_path: &JsonPath,
        point_id: PointOffsetType,
        from_value: F,
    ) -> OperationResult<T>
    where
        F: Fn(Value) -> Result<T, E>,
        E: ToString,
        T: FriendlyName,
    {
        let value = self
            .get_payload_value(json_path, point_id)
            .or_else(|| {
                self.defaults
                    .get(&VariableId::Payload(json_path.clone()))
                    .cloned()
            })
            .ok_or_else(|| OperationError::VariableTypeError {
                field_name: json_path.clone(),
                expected_type: T::friendly_name().to_owned(),
                description: "No value found in a payload nor defaults".to_string(),
            })?;

        from_value(value).map_err(|e| OperationError::VariableTypeError {
            field_name: json_path.clone(),
            expected_type: T::friendly_name().to_owned(),
            description: e.to_string(),
        })
    }
}

fn exp_decay(x: PreciseScore, target: PreciseScore, lambda: PreciseScore) -> PreciseScore {
    let diff = (x - target).abs();
    (lambda * diff).exp()
}

fn gauss_decay(x: PreciseScore, target: PreciseScore, lambda: PreciseScore) -> PreciseScore {
    let diff = x - target;
    (lambda * diff * diff).exp()
}

fn linear_decay(x: PreciseScore, target: PreciseScore, lambda: PreciseScore) -> PreciseScore {
    let diff = (x - target).abs();
    (-lambda * diff + 1.0).max(0.0)
}

#[cfg(test)]
#[cfg(feature = "testing")]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;
    use serde_json::json;
    use smallvec::smallvec;

    use super::*;
    use crate::index::query_optimization::rescore_formula::parsed_formula::PreciseScoreOrdered;
    use crate::json_path::JsonPath;

    const FIELD_NAME: &str = "number";
    const NO_VALUE_FIELD_NAME: &str = "no_number";
    const ARRAY_OF_ONE_FIELD_NAME: &str = "array_of_one";
    const ARRAY_FIELD_NAME: &str = "array";
    const GEO_FIELD_NAME: &str = "geo_point";
    const NO_VALUE_GEO_POINT: &str = "no_value_geo_point";
    const NO_VALUE_DATETIME: &str = "no_value_datetime";

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
            payload_retrievers.insert(
                JsonPath::new(FIELD_NAME),
                Box::new(|_| smallvec![json!(85.0)]),
            );
            payload_retrievers.insert(
                JsonPath::new(ARRAY_OF_ONE_FIELD_NAME),
                Box::new(|_| smallvec![json!(1.2)]),
            );
            payload_retrievers.insert(
                JsonPath::new(ARRAY_FIELD_NAME),
                Box::new(|_| smallvec![json!(1.2), json!(2.3)]),
            );
            payload_retrievers.insert(
                JsonPath::new(GEO_FIELD_NAME),
                Box::new(|_| {
                    smallvec![json!({"lat": 25.628482424190565, "lon": -100.23881855976})]
                }),
            );

            let condition_checkers = vec![
                OptimizedCondition::Checker(Box::new(|_| true)),
                OptimizedCondition::Checker(Box::new(|_| false)),
            ];

            FormulaScorer {
                formula: ParsedExpression::Constant(PreciseScoreOrdered::from(0.0)),
                prefetches_scores,
                payload_retrievers,
                condition_checkers,
                defaults: defaults.clone(),
            }
        })
    }

    #[rstest]
    // Basic expressions, just variables
    #[case(ParsedExpression::Constant(PreciseScoreOrdered::from(5.0)), 5.0)]
    #[case(ParsedExpression::new_score_id(0), 1.0)]
    #[case(ParsedExpression::new_score_id(1), 2.0)]
    #[case(ParsedExpression::new_payload_id(JsonPath::new(FIELD_NAME)), 85.0)]
    #[case(ParsedExpression::new_condition_id(0), 1.0)]
    #[case(ParsedExpression::new_condition_id(1), 0.0)]
    // Operations
    #[case(ParsedExpression::Sum(vec![
        ParsedExpression::Constant(PreciseScoreOrdered::from(1.0)),
        ParsedExpression::new_score_id(0),
        ParsedExpression::new_payload_id(JsonPath::new(FIELD_NAME)),
        ParsedExpression::new_condition_id(0),
    ]), 1.0 + 1.0 + 85.0 + 1.0)]
    #[case(ParsedExpression::Mult(vec![
        ParsedExpression::Constant(PreciseScoreOrdered::from(2.0)),
        ParsedExpression::new_score_id(0),
        ParsedExpression::new_payload_id(JsonPath::new(FIELD_NAME)),
        ParsedExpression::new_condition_id(0),
    ]), 2.0 * 1.0 * 85.0 * 1.0)]
    #[case(ParsedExpression::new_div(
        ParsedExpression::Constant(PreciseScoreOrdered::from(10.0)), ParsedExpression::new_score_id(0), None
    ), 10.0 / 1.0)]
    #[case(ParsedExpression::new_neg(ParsedExpression::Constant(PreciseScoreOrdered::from(10.0))), -10.0)]
    // Error cases
    #[case(ParsedExpression::new_geo_distance(
        GeoPoint::new_unchecked(-100.43383200156751, 25.717877679163667), JsonPath::new(GEO_FIELD_NAME)
    ), 21926.494151786308)]
    #[should_panic(
        expected = r#"VariableTypeError { field_name: JsonPath { first_key: "number", rest: [] }, expected_type: "geo point", "#
    )]
    #[case(ParsedExpression::new_geo_distance(GeoPoint::new_unchecked(-100.43383200156751, 25.717877679163667), JsonPath::new(FIELD_NAME)), 0.0)]
    #[should_panic(expected = r#"NonFiniteNumber { expression: "-1^0.4 = NaN" }"#)]
    #[case(ParsedExpression::new_pow(ParsedExpression::Constant(PreciseScoreOrdered::from(-1.0)), ParsedExpression::Constant(PreciseScoreOrdered::from(0.4))), 0.0)]
    #[should_panic(expected = r#"NonFiniteNumber { expression: "√-3 = NaN" }"#)]
    #[case(ParsedExpression::new_sqrt(ParsedExpression::Constant(PreciseScoreOrdered::from(-3.0))), 0.0)]
    #[should_panic(expected = r#"NonFiniteNumber { expression: "1/0 = inf" }"#)]
    #[case(
        ParsedExpression::new_div(
            ParsedExpression::Constant(PreciseScoreOrdered::from(1.0)),
            ParsedExpression::Constant(PreciseScoreOrdered::from(0.0)),
            None
        ),
        0.0
    )]
    #[should_panic(expected = r#"NonFiniteNumber { expression: "log10(0) = -inf" }"#)]
    #[case(
        ParsedExpression::new_log10(ParsedExpression::Constant(PreciseScoreOrdered::from(0.0))),
        0.0
    )]
    #[should_panic(expected = r#"NonFiniteNumber { expression: "ln(0) = -inf" }"#)]
    #[case(
        ParsedExpression::new_ln(ParsedExpression::Constant(PreciseScoreOrdered::from(0.0))),
        0.0
    )]
    #[test]
    fn test_evaluation(#[case] expr: ParsedExpression, #[case] expected: PreciseScore) {
        let defaults = HashMap::new();
        let scorer_fixture = make_formula_scorer(&defaults);

        let scorer = scorer_fixture.borrow_dependent();

        assert_eq!(scorer.eval_expression(&expr, 0).unwrap(), expected);
    }

    // Default values
    #[rstest]
    // Defined default score
    #[case(ParsedExpression::new_score_id(3), Ok(1.5))]
    // score idx not defined
    #[case(ParsedExpression::new_score_id(10), Ok(DEFAULT_SCORE))]
    // missing value in payload
    #[case(
        ParsedExpression::new_payload_id(JsonPath::new(ARRAY_OF_ONE_FIELD_NAME)),
        Ok(1.2)
    )]
    #[case(
        ParsedExpression::new_payload_id(JsonPath::new(ARRAY_FIELD_NAME)),
        Err(OperationError::VariableTypeError { field_name: JsonPath::new("array"), expected_type: "number".into(), description: "Value is not a number".into() })
    )]
    #[case(
        ParsedExpression::new_payload_id(JsonPath::new(NO_VALUE_FIELD_NAME)),
        Ok(85.0)
    )]
    // missing value and no default value provided
    #[case(
        ParsedExpression::new_payload_id(JsonPath::new("missing_field")),
        Err(OperationError::VariableTypeError {
            field_name: JsonPath::new("missing_field"),
            expected_type: PreciseScore::friendly_name().to_string(),
            description: "No value found in a payload nor defaults".to_string(),
        })
    )]
    // geo distance with default value
    #[case(ParsedExpression::new_geo_distance(GeoPoint::new_unchecked(-100.43383200156751, 25.717877679163667), JsonPath::new(NO_VALUE_GEO_POINT)), Ok(90951.29600298218))]
    // datetime expression constant
    #[case(
        ParsedExpression::Datetime(DatetimeExpression::Constant("2025-03-18".parse().unwrap())),
        Ok("2025-03-18".parse::<DateTimePayloadType>().unwrap().timestamp() as PreciseScore / 1_000_000.0)
    )]
    // datetime expression with payload variable that doesn't exist in payload and no default
    #[case(
        ParsedExpression::Datetime(DatetimeExpression::PayloadVariable(JsonPath::new("missing_datetime"))),
        Err(OperationError::VariableTypeError {
            field_name: JsonPath::new("missing_datetime"),
            expected_type: DateTimePayloadType::friendly_name().to_string(),
            description: "No value found in a payload nor defaults".to_string(),
        })
    )]
    // datetime expression with payload variable that doesn't exist in payload but has default
    #[case(
        ParsedExpression::Datetime(DatetimeExpression::PayloadVariable(JsonPath::new(NO_VALUE_DATETIME))),
        Ok("2025-03-19T12:00:00".parse::<DateTimePayloadType>().unwrap().timestamp() as PreciseScore / 1_000_000.0)
    )]
    #[test]
    fn test_default_values(
        #[case] expr: ParsedExpression,
        #[case] expected: OperationResult<PreciseScore>,
    ) {
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
            (
                VariableId::Payload(JsonPath::new(NO_VALUE_DATETIME)),
                json!("2025-03-19T12:00:00"),
            ),
        ]
        .into_iter()
        .collect();

        let scorer_fixture = make_formula_scorer(&defaults);

        let scorer = scorer_fixture.borrow_dependent();

        assert_eq!(scorer.eval_expression(&expr, 0), expected);
    }
}
