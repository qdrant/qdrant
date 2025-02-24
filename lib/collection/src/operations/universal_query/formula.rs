use std::collections::HashMap;

use api::rest;
use api::rest::GeoDistance;
use common::types::ScoreType;
use segment::json_path::JsonPath;
use segment::types::{Condition, GeoPoint};
use serde_json::Value;

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
    GeoDistance {
        origin: GeoPoint,
        to: JsonPath,
    },
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
            rest::Expression::GeoDistance(GeoDistance {
                geo_distance: rest::GeoDistanceParams { origin, to },
            }) => ExpressionInternal::GeoDistance { origin, to },
        }
    }
}
