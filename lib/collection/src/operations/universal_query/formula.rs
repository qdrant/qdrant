use std::collections::{HashMap, HashSet};

use api::rest;
use api::rest::GeoDistance;
use common::types::ScoreType;
use itertools::Itertools;
use segment::index::query_optimization::rescore_formula::parsed_formula::{
    DatetimeExpression, DecayKind, ParsedExpression, ParsedFormula, PreciseScore, VariableId,
};
use segment::json_path::JsonPath;
use segment::types::{Condition, GeoPoint};
use serde_json::Value;

use crate::operations::types::{CollectionError, CollectionResult};

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaInternal {
    pub formula: ExpressionInternal,
    pub defaults: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
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
    ) -> CollectionResult<ParsedExpression> {
        let expr = match self {
            ExpressionInternal::Constant(c) => ParsedExpression::Constant(PreciseScore::from(c)),
            ExpressionInternal::Variable(var) => {
                let var: VariableId = var.parse()?;
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
                        .map_err(|err: chrono::ParseError| err.to_string())?,
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
                    lambda,
                }
            }
        };

        Ok(expr)
    }
}

impl TryFrom<FormulaInternal> for ParsedFormula {
    type Error = CollectionError;

    fn try_from(value: FormulaInternal) -> Result<Self, Self::Error> {
        let FormulaInternal { formula, defaults } = value;

        let mut payload_vars = HashSet::new();
        let mut conditions = Vec::new();

        let parsed_expression = formula.parse_and_convert(&mut payload_vars, &mut conditions)?;

        let defaults = defaults
            .into_iter()
            .map(|(key, value)| {
                let key = key.as_str().parse()?;
                CollectionResult::Ok((key, value))
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

impl From<rest::FormulaQuery> for FormulaInternal {
    fn from(value: rest::FormulaQuery) -> Self {
        let rest::FormulaQuery { formula, defaults } = value;

        FormulaInternal {
            formula: ExpressionInternal::from(formula),
            defaults,
        }
    }
}

impl From<rest::Expression> for ExpressionInternal {
    fn from(value: rest::Expression) -> Self {
        match value {
            rest::Expression::Constant(c) => ExpressionInternal::Constant(c),
            rest::Expression::Variable(key) => ExpressionInternal::Variable(key),
            rest::Expression::Condition(condition) => ExpressionInternal::Condition(condition),
            rest::Expression::GeoDistance(GeoDistance {
                geo_distance: rest::GeoDistanceParams { origin, to },
            }) => ExpressionInternal::GeoDistance { origin, to },
            rest::Expression::Datetime(rest::DatetimeExpression { datetime }) => {
                ExpressionInternal::Datetime(datetime)
            }
            rest::Expression::DatetimeKey(rest::DatetimeKeyExpression { datetime_key }) => {
                ExpressionInternal::DatetimeKey(datetime_key)
            }
            rest::Expression::Mult(rest::MultExpression { mult: exprs }) => {
                ExpressionInternal::Mult(exprs.into_iter().map(ExpressionInternal::from).collect())
            }
            rest::Expression::Sum(rest::SumExpression { sum: exprs }) => {
                ExpressionInternal::Sum(exprs.into_iter().map(ExpressionInternal::from).collect())
            }
            rest::Expression::Neg(rest::NegExpression { neg: expr }) => {
                ExpressionInternal::Neg(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Div(rest::DivExpression {
                div:
                    rest::DivParams {
                        left,
                        right,
                        by_zero_default,
                    },
            }) => {
                let left = Box::new((*left).into());
                let right = Box::new((*right).into());
                ExpressionInternal::Div {
                    left,
                    right,
                    by_zero_default,
                }
            }
            rest::Expression::Sqrt(sqrt_expression) => {
                ExpressionInternal::Sqrt(Box::new(ExpressionInternal::from(*sqrt_expression.sqrt)))
            }
            rest::Expression::Pow(rest::PowExpression { pow }) => ExpressionInternal::Pow {
                base: Box::new(ExpressionInternal::from(*pow.base)),
                exponent: Box::new(ExpressionInternal::from(*pow.exponent)),
            },
            rest::Expression::Exp(rest::ExpExpression { exp: expr }) => {
                ExpressionInternal::Exp(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Log10(rest::Log10Expression { log10: expr }) => {
                ExpressionInternal::Log10(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Ln(rest::LnExpression { ln: expr }) => {
                ExpressionInternal::Ln(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Abs(rest::AbsExpression { abs: expr }) => {
                ExpressionInternal::Abs(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::LinDecay(rest::LinDecayExpression {
                lin_decay:
                    rest::DecayParamsExpression {
                        x,
                        target,
                        midpoint,
                        scale,
                    },
            }) => ExpressionInternal::Decay {
                kind: DecayKind::Lin,
                x: Box::new(ExpressionInternal::from(*x)),
                target: target.map(|t| Box::new(ExpressionInternal::from(*t))),
                midpoint,
                scale,
            },
            rest::Expression::ExpDecay(rest::ExpDecayExpression {
                exp_decay:
                    rest::DecayParamsExpression {
                        x,
                        target,
                        midpoint,
                        scale,
                    },
            }) => ExpressionInternal::Decay {
                kind: DecayKind::Exp,
                x: Box::new(ExpressionInternal::from(*x)),
                target: target.map(|t| Box::new(ExpressionInternal::from(*t))),
                midpoint,
                scale,
            },
            rest::Expression::GaussDecay(rest::GaussDecayExpression {
                gauss_decay:
                    rest::DecayParamsExpression {
                        x,
                        target,
                        midpoint,
                        scale,
                    },
            }) => ExpressionInternal::Decay {
                kind: DecayKind::Gauss,
                x: Box::new(ExpressionInternal::from(*x)),
                target: target.map(|t| Box::new(ExpressionInternal::from(*t))),
                midpoint,
                scale,
            },
        }
    }
}
