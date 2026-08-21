use std::collections::{HashMap, HashSet};
use std::fmt;

use common::types::ScoreType;
use itertools::Itertools;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::index::query_optimization::rescore_formula::parsed_formula::*;
use segment::json_path::JsonPath;
use segment::types::{Condition, GeoPoint};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FormulaInternal {
    pub formula: ExpressionInternal,
    pub defaults: HashMap<String, Value>,
}

impl TryFrom<FormulaInternal> for ParsedFormula {
    type Error = OperationError;

    fn try_from(value: FormulaInternal) -> Result<Self, Self::Error> {
        let FormulaInternal { formula, defaults } = value;

        let mut payload_vars = HashSet::new();
        let mut conditions = Vec::new();

        let parsed_expression = formula.parse_and_convert(&mut payload_vars, &mut conditions)?;

        let defaults = defaults
            .into_iter()
            .map(|(key, value)| {
                let key = key
                    .as_str()
                    .parse()
                    .map_err(|msg| failed_to_parse("variable ID", &key, &msg))?;
                OperationResult::Ok((key, value))
            })
            .try_collect()?;

        Ok(ParsedFormula {
            formula: parsed_expression,
            payload_vars,
            conditions,
            defaults,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ExpressionInternal {
    Constant(f32),
    Variable(String),
    Condition(Box<Condition>),
    GeoDistance {
        origin: GeoPoint,
        to: JsonPath,
    },
    Datetime(String),
    DatetimeKey(JsonPath),
    Mult(Vec<ExpressionInternal>),
    Sum(Vec<ExpressionInternal>),
    Max(Vec<ExpressionInternal>),
    Min(Vec<ExpressionInternal>),
    Neg(Box<ExpressionInternal>),
    Div {
        left: Box<ExpressionInternal>,
        right: Box<ExpressionInternal>,
        by_zero_default: Option<ScoreType>,
    },
    Sqrt(Box<ExpressionInternal>),
    Pow {
        base: Box<ExpressionInternal>,
        exponent: Box<ExpressionInternal>,
    },
    Exp(Box<ExpressionInternal>),
    Log10(Box<ExpressionInternal>),
    Ln(Box<ExpressionInternal>),
    Acosh(Box<ExpressionInternal>),
    Abs(Box<ExpressionInternal>),
    Decay {
        kind: DecayKind,
        x: Box<ExpressionInternal>,
        target: Option<Box<ExpressionInternal>>,
        midpoint: Option<f32>,
        scale: Option<f32>,
    },
}

impl ExpressionInternal {
    fn parse_and_convert(
        self,
        payload_vars: &mut HashSet<JsonPath>,
        conditions: &mut Vec<Condition>,
    ) -> OperationResult<ParsedExpression> {
        let expr = match self {
            ExpressionInternal::Constant(c) => {
                ParsedExpression::Constant(PreciseScoreOrdered::from(PreciseScore::from(c)))
            }
            ExpressionInternal::Variable(var) => {
                let var: VariableId = var
                    .parse()
                    .map_err(|msg| failed_to_parse("variable ID", &var, &msg))?;
                if let VariableId::Payload(payload_var) = var.clone() {
                    payload_vars.insert(payload_var);
                }
                ParsedExpression::Variable(var)
            }
            ExpressionInternal::Condition(condition) => {
                let condition_id = conditions.len();
                conditions.push(*condition);
                ParsedExpression::new_condition_id(condition_id)
            }
            ExpressionInternal::GeoDistance { origin, to } => {
                payload_vars.insert(to.clone());
                ParsedExpression::new_geo_distance(origin, to)
            }
            ExpressionInternal::Datetime(dt_str) => {
                ParsedExpression::Datetime(DatetimeExpression::Constant(
                    dt_str
                        .parse()
                        .map_err(|err| failed_to_parse("date-time", &dt_str, err))?,
                ))
            }
            ExpressionInternal::DatetimeKey(json_path) => {
                payload_vars.insert(json_path.clone());
                ParsedExpression::Datetime(DatetimeExpression::PayloadVariable(json_path))
            }
            ExpressionInternal::Mult(internal_expressions) => ParsedExpression::Mult(
                internal_expressions
                    .into_iter()
                    .map(|expr| expr.parse_and_convert(payload_vars, conditions))
                    .try_collect()?,
            ),
            ExpressionInternal::Sum(expression_internals) => ParsedExpression::Sum(
                expression_internals
                    .into_iter()
                    .map(|expr| expr.parse_and_convert(payload_vars, conditions))
                    .try_collect()?,
            ),
            ExpressionInternal::Max(expression_internals) => ParsedExpression::Max(
                parse_non_empty_operands("max", expression_internals, payload_vars, conditions)?,
            ),
            ExpressionInternal::Min(expression_internals) => ParsedExpression::Min(
                parse_non_empty_operands("min", expression_internals, payload_vars, conditions)?,
            ),
            ExpressionInternal::Neg(expression_internal) => ParsedExpression::new_neg(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            ),
            ExpressionInternal::Div {
                left,
                right,
                by_zero_default,
            } => ParsedExpression::new_div(
                left.parse_and_convert(payload_vars, conditions)?,
                right.parse_and_convert(payload_vars, conditions)?,
                by_zero_default.map(PreciseScore::from),
            ),
            ExpressionInternal::Sqrt(expression_internal) => ParsedExpression::Sqrt(Box::new(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            )),
            ExpressionInternal::Pow { base, exponent } => ParsedExpression::Pow {
                base: Box::new(base.parse_and_convert(payload_vars, conditions)?),
                exponent: Box::new(exponent.parse_and_convert(payload_vars, conditions)?),
            },
            ExpressionInternal::Exp(expression_internal) => ParsedExpression::Exp(Box::new(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            )),
            ExpressionInternal::Log10(expression_internal) => ParsedExpression::Log10(Box::new(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            )),
            ExpressionInternal::Ln(expression_internal) => ParsedExpression::Ln(Box::new(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            )),
            ExpressionInternal::Acosh(expression_internal) => ParsedExpression::Acosh(Box::new(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            )),
            ExpressionInternal::Abs(expression_internal) => ParsedExpression::Abs(Box::new(
                expression_internal.parse_and_convert(payload_vars, conditions)?,
            )),
            ExpressionInternal::Decay {
                kind,
                x,
                target,
                midpoint,
                scale,
            } => {
                let lambda = ParsedExpression::decay_params_to_lambda(midpoint, scale, kind)?;

                let x = x.parse_and_convert(payload_vars, conditions)?;

                let target = target
                    .map(|t| t.parse_and_convert(payload_vars, conditions))
                    .transpose()?
                    .map(Box::new);

                ParsedExpression::Decay {
                    kind,
                    x: Box::new(x),
                    target,
                    lambda: PreciseScoreOrdered::from(lambda),
                }
            }
        };

        Ok(expr)
    }
}

/// Parses the operands of a variadic operator which has no identity element to fall back on when
/// given nothing. `sum` and `mult` can define the empty case as `0` and `1`, but `max` and `min`
/// cannot: folding over no operands would yield -inf or +inf, and score every point with a
/// non-finite value instead of reporting the mistake.
fn parse_non_empty_operands(
    operator: &str,
    operands: Vec<ExpressionInternal>,
    payload_vars: &mut HashSet<JsonPath>,
    conditions: &mut Vec<Condition>,
) -> OperationResult<Vec<ParsedExpression>> {
    if operands.is_empty() {
        return Err(OperationError::validation_error(format!(
            "`{operator}` needs at least one operand"
        )));
    }

    operands
        .into_iter()
        .map(|expr| expr.parse_and_convert(payload_vars, conditions))
        .try_collect()
}

fn failed_to_parse(what: &str, value: &str, message: impl fmt::Display) -> OperationError {
    OperationError::validation_error(format!("failed to parse {what} {value}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(expression: ExpressionInternal) -> OperationResult<ParsedFormula> {
        FormulaInternal {
            formula: expression,
            defaults: HashMap::new(),
        }
        .try_into()
    }

    /// `sum` and `mult` have identity elements for the empty case (0 and 1), but `max` and `min`
    /// do not: folding over nothing would silently yield ±infinity. Reject it at parse time
    /// instead, which covers every entry point (REST, gRPC and the Edge FFI).
    #[test]
    fn empty_max_is_rejected() {
        let err = parse(ExpressionInternal::Max(vec![])).unwrap_err();
        assert!(
            matches!(err, OperationError::ValidationError { .. }),
            "expected a validation error, got {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("max"),
            "error should name the operator, got {message:?}"
        );
    }

    #[test]
    fn empty_min_is_rejected() {
        let err = parse(ExpressionInternal::Min(vec![])).unwrap_err();
        assert!(
            matches!(err, OperationError::ValidationError { .. }),
            "expected a validation error, got {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("min"),
            "error should name the operator, got {message:?}"
        );
    }

    #[test]
    fn single_operand_min_is_accepted() {
        let parsed = parse(ExpressionInternal::Min(vec![ExpressionInternal::Constant(
            1.0,
        )]))
        .unwrap();
        assert_eq!(
            parsed.formula,
            ParsedExpression::Min(vec![ParsedExpression::Constant(PreciseScoreOrdered::from(
                1.0
            ))])
        );
    }

    /// Payload variables and conditions nested inside `min` must still be collected, otherwise
    /// the scorer would have no retriever for them.
    #[test]
    fn min_collects_nested_payload_vars() {
        let parsed = parse(ExpressionInternal::Min(vec![
            ExpressionInternal::Variable("popularity".to_string()),
            ExpressionInternal::Variable("$score".to_string()),
        ]))
        .unwrap();
        assert_eq!(
            parsed.payload_vars,
            HashSet::from([JsonPath::new("popularity")])
        );
    }

    #[test]
    fn single_operand_max_is_accepted() {
        let parsed = parse(ExpressionInternal::Max(vec![ExpressionInternal::Constant(
            1.0,
        )]))
        .unwrap();
        assert_eq!(
            parsed.formula,
            ParsedExpression::Max(vec![ParsedExpression::Constant(PreciseScoreOrdered::from(
                1.0
            ))])
        );
    }

    /// Payload variables and conditions nested inside `max` must still be collected, otherwise
    /// the scorer would have no retriever for them.
    #[test]
    fn max_collects_nested_payload_vars() {
        let parsed = parse(ExpressionInternal::Max(vec![
            ExpressionInternal::Variable("popularity".to_string()),
            ExpressionInternal::Variable("$score".to_string()),
        ]))
        .unwrap();
        assert_eq!(
            parsed.payload_vars,
            HashSet::from([JsonPath::new("popularity")])
        );
    }
}
