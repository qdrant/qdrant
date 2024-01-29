use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::types::Range;

pub const INTERNAL_KEY_OF_ORDER_BY_VALUE: &str = "____ordered_with____";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

impl Direction {
    pub fn as_range(&self, from: f64) -> Range {
        match self {
            Direction::Asc => Range {
                gte: Some(from),
                ..Default::default()
            },
            Direction::Desc => Range {
                lte: Some(from),
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct OrderBy {
    /// Payload key to order by
    pub key: String,

    /// Direction of ordering: `asc` or `desc`. Default is ascending.
    pub direction: Option<Direction>,

    /// Which payload value to start scrolling from. Default is the lowest value for `asc` and the highest for `desc`
    pub start_from: Option<f64>,
}

impl OrderBy {
    pub fn as_range(&self) -> Range {
        match self.start_from {
            Some(offset) => self.direction.unwrap().as_range(offset),
            None => Range {
                ..Default::default()
            },
        }
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or_default()
    }

    pub fn value_offset(&self) -> f64 {
        self.start_from.unwrap_or_else(|| match self.direction() {
            Direction::Asc => f64::NEG_INFINITY,
            Direction::Desc => f64::INFINITY,
        })
    }
}
