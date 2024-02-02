use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::types::{FloatPayloadType, Payload, Range};

const INTERNAL_KEY_OF_ORDER_BY_VALUE: &str = "____ordered_with____";

#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

impl Direction {
    pub fn as_range(&self, from: FloatPayloadType) -> Range<FloatPayloadType> {
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
    pub start_from: Option<FloatPayloadType>,
}

impl OrderBy {
    pub fn as_range(&self) -> Range<FloatPayloadType> {
        match self.start_from {
            Some(start_from) => self.direction.unwrap().as_range(start_from),
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

    pub fn insert_order_value_in_payload(payload: Option<Payload>, value: f64) -> Payload {
        let mut new_payload = payload.unwrap_or_default();
        new_payload
            .0
            .insert(INTERNAL_KEY_OF_ORDER_BY_VALUE.to_string(), value.into());
        new_payload
    }

    pub fn remove_order_value_from_payload(&self, payload: Option<&mut Payload>) -> f64 {
        payload
            .and_then(|payload| payload.0.remove(INTERNAL_KEY_OF_ORDER_BY_VALUE))
            .and_then(|v| v.as_f64())
            .unwrap_or_else(|| match self.direction() {
                Direction::Asc => std::f64::MAX,
                Direction::Desc => std::f64::MIN,
            })
    }
}
