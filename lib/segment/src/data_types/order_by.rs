use num_cmp::NumCmp;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, Payload, Range, RangeInterface,
};

const INTERNAL_KEY_OF_ORDER_BY_VALUE: &str = "____ordered_with____";

#[derive(Deserialize, Serialize, JsonSchema, Copy, Clone, Debug, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

impl Direction {
    pub fn as_range_from<T>(&self, from: T) -> Range<T> {
        match self {
            Direction::Asc => Range {
                gte: Some(from),
                gt: None,
                lte: None,
                lt: None,
            },
            Direction::Desc => Range {
                lte: Some(from),
                gt: None,
                gte: None,
                lt: None,
            },
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum StartFrom {
    Integer(IntPayloadType),

    Float(FloatPayloadType),

    Datetime(DateTimePayloadType),
}

#[derive(Deserialize, Serialize, JsonSchema, Validate, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct OrderBy {
    /// Payload key to order by
    pub key: JsonPath,

    /// Direction of ordering: `asc` or `desc`. Default is ascending.
    pub direction: Option<Direction>,

    /// Which payload value to start scrolling from. Default is the lowest value for `asc` and the highest for `desc`
    pub start_from: Option<StartFrom>,
}

impl OrderBy {
    /// Returns a range representation of OrderBy.
    pub fn as_range(&self) -> RangeInterface {
        self.start_from
            .as_ref()
            .map(|start_from| match start_from {
                // TODO: When we introduce integer ranges, we'll stop doing lossy conversion to f64 here
                // Accepting an integer as start_from simplifies the client generation.
                StartFrom::Integer(i) => {
                    RangeInterface::Float(self.direction().as_range_from(*i as f64))
                }
                StartFrom::Float(f) => RangeInterface::Float(self.direction().as_range_from(*f)),
                StartFrom::Datetime(dt) => {
                    RangeInterface::DateTime(self.direction().as_range_from(*dt))
                }
            })
            .unwrap_or_else(|| RangeInterface::Float(Range::default()))
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or_default()
    }

    pub fn start_from(&self) -> OrderedValue {
        self.start_from
            .as_ref()
            .map(|start_from| match start_from {
                StartFrom::Integer(i) => OrderedValue::Int(*i),
                StartFrom::Float(f) => OrderedValue::Float(*f),
                StartFrom::Datetime(dt) => OrderedValue::Int(dt.timestamp()),
            })
            .unwrap_or_else(|| match self.direction() {
                Direction::Asc => OrderedValue::MIN,
                Direction::Desc => OrderedValue::MAX,
            })
    }

    pub fn insert_order_value_in_payload(
        payload: Option<Payload>,
        value: impl Into<serde_json::Value>,
    ) -> Payload {
        let mut new_payload = payload.unwrap_or_default();
        new_payload
            .0
            .insert(INTERNAL_KEY_OF_ORDER_BY_VALUE.to_string(), value.into());
        new_payload
    }

    fn json_value_to_ordering_value(&self, value: Option<serde_json::Value>) -> OrderedValue {
        value
            .and_then(|v| OrderedValue::try_from(v).ok())
            .unwrap_or_else(|| match self.direction() {
                Direction::Asc => OrderedValue::MAX,
                Direction::Desc => OrderedValue::MIN,
            })
    }

    pub fn get_order_value_from_payload(&self, payload: Option<&Payload>) -> OrderedValue {
        self.json_value_to_ordering_value(
            payload.and_then(|payload| payload.0.get(INTERNAL_KEY_OF_ORDER_BY_VALUE).cloned()),
        )
    }

    pub fn remove_order_value_from_payload(&self, payload: Option<&mut Payload>) -> OrderedValue {
        self.json_value_to_ordering_value(
            payload.and_then(|payload| payload.0.remove(INTERNAL_KEY_OF_ORDER_BY_VALUE)),
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum OrderedValue {
    Int(IntPayloadType),
    Float(FloatPayloadType),
}

impl OrderedValue {
    const MAX: Self = Self::Float(f64::NAN);
    const MIN: Self = Self::Float(f64::MIN);
}

impl From<OrderedValue> for serde_json::Value {
    fn from(value: OrderedValue) -> Self {
        match value {
            OrderedValue::Float(value) => serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            OrderedValue::Int(value) => serde_json::Value::Number(serde_json::Number::from(value)),
        }
    }
}

impl TryFrom<serde_json::Value> for OrderedValue {
    type Error = ();

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        value
            .as_i64()
            .map(Self::from)
            .or_else(|| value.as_f64().map(Self::from))
            .ok_or(())
    }
}

impl From<FloatPayloadType> for OrderedValue {
    fn from(value: FloatPayloadType) -> Self {
        OrderedValue::Float(value)
    }
}

impl From<IntPayloadType> for OrderedValue {
    fn from(value: IntPayloadType) -> Self {
        OrderedValue::Int(value)
    }
}

impl Eq for OrderedValue {}

impl PartialEq for OrderedValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (OrderedValue::Float(a), OrderedValue::Float(b)) => {
                OrderedFloat(*a) == OrderedFloat(*b)
            }
            (OrderedValue::Int(a), OrderedValue::Int(b)) => a == b,
            (OrderedValue::Float(a), OrderedValue::Int(b)) => a.num_eq(*b),
            (OrderedValue::Int(a), OrderedValue::Float(b)) => a.num_eq(*b),
        }
    }
}

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (OrderedValue::Float(a), OrderedValue::Float(b)) => {
                OrderedFloat(*a).cmp(&OrderedFloat(*b))
            }
            (OrderedValue::Int(a), OrderedValue::Int(b)) => a.cmp(b),
            (OrderedValue::Float(a), OrderedValue::Int(b)) => {
                // num_cmp() might return None only if the float value is NaN. We follow the
                // OrderedFloat logic here: the NaN is always greater than any other value.
                a.num_cmp(*b).unwrap_or(std::cmp::Ordering::Greater)
            }
            (OrderedValue::Int(a), OrderedValue::Float(b)) => {
                // Ditto, but the NaN is on the right side of the comparison.
                a.num_cmp(*b).unwrap_or(std::cmp::Ordering::Less)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::proptest;

    use crate::data_types::order_by::OrderedValue;

    proptest! {

        #[test]
        fn test_min_ordering_value(a in i64::MIN..0, b in f64::MIN..0.0) {
            assert!(OrderedValue::MIN.cmp(&OrderedValue::from(a)).is_le());
            assert!(OrderedValue::MIN.cmp(&OrderedValue::from(b)).is_le());
            assert!(OrderedValue::MIN.cmp(&OrderedValue::from(f64::NAN)).is_le());
        }

        #[test]
        fn test_max_ordering_value(a in 0..i64::MAX, b in 0.0..f64::MAX) {
            assert!(OrderedValue::MAX.cmp(&OrderedValue::from(a)).is_ge());
            assert!(OrderedValue::MAX.cmp(&OrderedValue::from(b)).is_ge());
            assert!(OrderedValue::MAX.cmp(&OrderedValue::from(f64::NAN)).is_ge());
        }
    }
}
