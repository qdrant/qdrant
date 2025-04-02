use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use common::types::ScoreType;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::json_path::{JsonPath, JsonPathItem};
use crate::types::{Condition, DateTimePayloadType, GeoPoint};

const SCORE_KEYWORD: &str = "score";
const DEFAULT_DECAY_MIDPOINT: f32 = 0.5;
const DEFAULT_DECAY_SCALE: f32 = 1.0;

pub type ConditionId = usize;
pub type PreciseScore = f64;

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
    // Terminal
    Constant(PreciseScore),
    Variable(VariableId),
    GeoDistance {
        origin: GeoPoint,
        key: JsonPath,
    },
    Datetime(DatetimeExpression),

    // Nested
    Mult(Vec<ParsedExpression>),
    Sum(Vec<ParsedExpression>),
    Div {
        left: Box<ParsedExpression>,
        right: Box<ParsedExpression>,
        by_zero_default: Option<PreciseScore>,
    },
    Neg(Box<ParsedExpression>),
    Sqrt(Box<ParsedExpression>),
    Pow {
        base: Box<ParsedExpression>,
        exponent: Box<ParsedExpression>,
    },
    Exp(Box<ParsedExpression>),
    Log10(Box<ParsedExpression>),
    Ln(Box<ParsedExpression>),
    Abs(Box<ParsedExpression>),
    Decay {
        kind: DecayKind,
        /// Value to decay
        x: Box<ParsedExpression>,
        /// Value at which the decay function is the highest
        target: Option<Box<ParsedExpression>>,
        /// Constant to shape the decay function
        lambda: PreciseScore,
    },
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DecayKind {
    /// Linear decay function
    Lin,
    /// Gaussian decay function
    Gauss,
    /// Exponential decay function
    Exp,
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

impl VariableId {
    pub fn unparse(self) -> String {
        match self {
            VariableId::Score(index) => format!("${SCORE_KEYWORD}[{index}]"),
            VariableId::Payload(path) => path.to_string(),
            VariableId::Condition(_) => unreachable!("there are no defaults for conditions"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DatetimeExpression {
    Constant(DateTimePayloadType),
    PayloadVariable(JsonPath),
}

impl ParsedExpression {
    pub fn new_div(
        left: ParsedExpression,
        right: ParsedExpression,
        by_zero_default: Option<PreciseScore>,
    ) -> Self {
        ParsedExpression::Div {
            left: Box::new(left),
            right: Box::new(right),
            by_zero_default,
        }
    }

    pub fn new_neg(expression: ParsedExpression) -> Self {
        ParsedExpression::Neg(Box::new(expression))
    }

    pub fn new_geo_distance(origin: GeoPoint, key: JsonPath) -> Self {
        ParsedExpression::GeoDistance { origin, key }
    }

    pub fn new_pow(base: ParsedExpression, exponent: ParsedExpression) -> Self {
        ParsedExpression::Pow {
            base: Box::new(base),
            exponent: Box::new(exponent),
        }
    }

    pub fn new_sqrt(expression: ParsedExpression) -> Self {
        ParsedExpression::Sqrt(Box::new(expression))
    }

    pub fn new_log10(expression: ParsedExpression) -> Self {
        ParsedExpression::Log10(Box::new(expression))
    }

    pub fn new_ln(expression: ParsedExpression) -> Self {
        ParsedExpression::Ln(Box::new(expression))
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

    /// Transforms the constant part of the decay function into a single `lambda` value.
    ///
    /// Graphical representation of the formulas:
    /// https://www.desmos.com/calculator/htg0vrfmks
    pub fn decay_params_to_lambda(
        midpoint: Option<f32>,
        scale: Option<f32>,
        kind: DecayKind,
    ) -> OperationResult<PreciseScore> {
        let midpoint = PreciseScore::from(midpoint.unwrap_or(DEFAULT_DECAY_MIDPOINT));
        let scale = PreciseScore::from(scale.unwrap_or(DEFAULT_DECAY_SCALE));

        if midpoint <= 0.0 || midpoint >= 1.0 {
            return Err(OperationError::validation_error(format!(
                "Decay midpoint should be between 0.0 and 1.0 (exclusive), got {midpoint}."
            )));
        }

        if scale <= 0.0 {
            return Err(OperationError::validation_error(format!(
                "Decay scale should be non-zero positive, got {scale}."
            )));
        }

        let lambda = match kind {
            DecayKind::Lin => (1.0 - midpoint) / scale,
            DecayKind::Exp => midpoint.ln() / scale,
            DecayKind::Gauss => midpoint.ln() / scale.powi(2),
        };

        Ok(lambda)
    }

    /// Converts the already computed lambda value to parameters which will result in
    /// the same lambda when used in a decay function on the peer node.
    ///
    /// Returns a tuple of (midpoint, scale) parameters.
    pub fn decay_lambda_to_params(lambda: PreciseScore, kind: DecayKind) -> (ScoreType, ScoreType) {
        match kind {
            DecayKind::Lin => {
                // We assume lambda is in the range (0, 1)
                debug_assert!(0.0 < lambda && lambda < 1.0);

                // Linear lambda is (1.0 - midpoint) / scale,
                // setting scale to 1.0 allows us to ignore the division,
                // and only set the midpoint to some value.
                //
                // (1.0 - midpoint) / 1.0 = lambda
                // 1.0 - midpoint = lambda
                // midpoint = 1.0 - lambda
                (1.0 - lambda as ScoreType, 1.0)
            }

            DecayKind::Gauss => {
                // We assume lambda is non-zero negative
                debug_assert!(lambda < 0.0);

                // Gauss lambda is scale^2 / ln(midpoint)
                // setting midpoint to 1/e (0.3678...) allows us to ignore the division, since ln(1/e) = -1
                // Then we set scale to sqrt(-lambda)
                //
                // ln(1/e) / scale^2 = lambda
                // -1.0 / scale^2 = lambda
                // scale^2 = -1.0 / lambda
                // scale = sqrt(-1.0 / lambda)
                (
                    1.0 / std::f32::consts::E,
                    (-1.0 / lambda).sqrt() as ScoreType,
                )
            }

            DecayKind::Exp => {
                // We assume lambda is non-zero negative
                debug_assert!(lambda < 0.0);

                // Exponential lambda is ln(midpoint) / scale
                // setting midpoint to 1/e (0.3678...) allows us to ignore the division, since ln(1/e) = -1
                // Then we set scale to -1 / lambda
                //
                // ln(1/e) / scale = lambda
                // -1.0 / scale = lambda
                // scale = -1.0 / lambda
                (1.0 / std::f32::consts::E, -1.0 / lambda as ScoreType)
            }
        }
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
    use common::math::is_close;

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

    /// Tests that lambda can be communicated to peers in the form of its components, and be recalculated appropriately
    fn check_lambda_round_trip(lambda: PreciseScore, kind: DecayKind) {
        let (midpoint, scale) = ParsedExpression::decay_lambda_to_params(lambda, kind);

        // Convert back to lambda
        let lambda_roundtrip =
            ParsedExpression::decay_params_to_lambda(Some(midpoint), Some(scale), kind).unwrap();

        // Check that the roundtrip conversion preserves the value
        assert!(
            is_close(lambda, lambda_roundtrip),
            "Lambda roundtrip failed for {kind:?}: {lambda} -> ({midpoint}, {scale}) -> {lambda_roundtrip}",
        );
    }

    proptest::proptest! {
        #[test]
        fn test_lin_decay_lambda_params_roundtrip(
            lambda in 0.000001..1.0f64
        ) {
            check_lambda_round_trip(lambda, DecayKind::Lin);
        }

        #[test]
        fn test_exp_decay_lambda_params_roundtrip(
            lambda in -1_000_000.0..-0.000_000_1f64
        ) {
            check_lambda_round_trip(lambda, DecayKind::Exp);
        }

        #[test]
        fn test_gauss_decay_lambda_params_roundtrip(
            lambda in -100_000_000.0..-0.0f64
        ) {
            check_lambda_round_trip(lambda, DecayKind::Gauss);
        }
    }
}
