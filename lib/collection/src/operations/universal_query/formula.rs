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
        by_zero_default: ScoreType,
    },
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
            rest::Expression::GeoDistance(GeoDistance {
                geo_distance: rest::GeoDistanceParams { origin, to },
            }) => ExpressionInternal::GeoDistance { origin, to },
        }
    }
}
