use std::hash::Hash;

use num_cmp::NumCmp;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::json_path::JsonPath;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, Order, Range, RangeInterface,
};

#[derive(Deserialize, Serialize, JsonSchema, Copy, Clone, Debug, Default, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

/// Largest magnitude below which every integer is exactly representable as `f64`.
const MAX_EXACT_F64_INT: u64 = 1 << 53;

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

    /// `from` as `f64`, rounded away from the scan direction when that conversion is
    /// lossy, so a range starting there never excludes `from` itself.
    fn widen_to_include(&self, from: IntPayloadType) -> f64 {
        let rounded = from as f64;
        if from.unsigned_abs() <= MAX_EXACT_F64_INT {
            return rounded;
        }
        match self {
            Direction::Asc => rounded.next_down(),
            Direction::Desc => rounded.next_up(),
        }
    }
}

impl From<Direction> for Order {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Asc => Order::SmallBetter,
            Direction::Desc => Order::LargeBetter,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum StartFrom {
    Integer(IntPayloadType),

    Float(FloatPayloadType),

    Datetime(DateTimePayloadType),
}

impl Hash for StartFrom {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            StartFrom::Integer(i) => i.hash(state),
            StartFrom::Float(f) => OrderedFloat(*f).hash(state),
            StartFrom::Datetime(dt) => dt.hash(state),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
#[serde(expecting = "Expected a string, or an object with a key, direction and/or start_from")]
pub enum OrderByInterface {
    Key(JsonPath),
    Struct(OrderBy),
}

impl From<OrderByInterface> for OrderBy {
    fn from(interface: OrderByInterface) -> Self {
        match interface {
            OrderByInterface::Key(key) => OrderBy {
                key,
                direction: None,
                start_from: None,
            },
            OrderByInterface::Struct(order_by) => order_by,
        }
    }
}

impl Validate for OrderByInterface {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            OrderByInterface::Key(_) => Ok(()), // validated during parsing
            OrderByInterface::Struct(order_by) => order_by.validate(),
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Validate, Clone, Debug, PartialEq, Hash)]
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
    ///
    /// For an integer `start_from` above 2^53 the range is only a superset: the bound is
    /// rounded outward because `f64` cannot hold the exact value. Callers must re-check
    /// each value against [`OrderBy::start_from`].
    pub fn as_range(&self) -> RangeInterface {
        self.start_from
            .as_ref()
            .map(|start_from| match start_from {
                // TODO: When we introduce integer ranges, we'll stop doing lossy conversion to f64 here
                // (widened below so it never excludes the start value)
                // Accepting an integer as start_from simplifies the client generation.
                StartFrom::Integer(i) => {
                    let from = self.direction().widen_to_include(*i);
                    RangeInterface::Float(self.direction().as_range_from(OrderedFloat(from)))
                }
                StartFrom::Float(f) => {
                    RangeInterface::Float(self.direction().as_range_from(OrderedFloat(*f)))
                }
                StartFrom::Datetime(dt) => {
                    RangeInterface::DateTime(self.direction().as_range_from(*dt))
                }
            })
            .unwrap_or_else(|| RangeInterface::Float(Range::default()))
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or_default()
    }

    pub fn start_from(&self) -> OrderValue {
        self.start_from
            .as_ref()
            .map(|start_from| match start_from {
                StartFrom::Integer(i) => OrderValue::Int(*i),
                StartFrom::Float(f) => OrderValue::Float(*f),
                StartFrom::Datetime(dt) => OrderValue::Int(dt.timestamp()),
            })
            .unwrap_or_else(|| match self.direction() {
                Direction::Asc => OrderValue::MIN,
                Direction::Desc => OrderValue::MAX,
            })
    }
}

fn order_value_int_example() -> IntPayloadType {
    42
}

fn order_value_float_example() -> FloatPayloadType {
    42.5
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum OrderValue {
    #[schemars(example = "order_value_int_example")]
    Int(IntPayloadType),
    #[schemars(example = "order_value_float_example")]
    Float(FloatPayloadType),
}

#[cfg(any(test, feature = "testing"))]
impl std::hash::Hash for OrderValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            OrderValue::Int(i) => i.hash(state),
            OrderValue::Float(f) => f.to_bits().hash(state),
        }
    }
}
impl OrderValue {
    const MAX: Self = Self::Float(f64::NAN);
    const MIN: Self = Self::Float(f64::MIN);
}

impl From<OrderValue> for serde_json::Value {
    fn from(value: OrderValue) -> Self {
        match value {
            OrderValue::Float(value) => serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            OrderValue::Int(value) => serde_json::Value::Number(serde_json::Number::from(value)),
        }
    }
}

impl TryFrom<serde_json::Value> for OrderValue {
    type Error = ();

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        value
            .as_i64()
            .map(Self::from)
            .or_else(|| value.as_f64().map(Self::from))
            .ok_or(())
    }
}

impl From<FloatPayloadType> for OrderValue {
    fn from(value: FloatPayloadType) -> Self {
        OrderValue::Float(value)
    }
}

impl From<IntPayloadType> for OrderValue {
    fn from(value: IntPayloadType) -> Self {
        OrderValue::Int(value)
    }
}

impl Eq for OrderValue {}

impl PartialEq for OrderValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (OrderValue::Float(a), OrderValue::Float(b)) => OrderedFloat(*a) == OrderedFloat(*b),
            (OrderValue::Int(a), OrderValue::Int(b)) => a == b,
            (OrderValue::Float(a), OrderValue::Int(b)) => a.num_eq(*b),
            (OrderValue::Int(a), OrderValue::Float(b)) => a.num_eq(*b),
        }
    }
}

impl PartialOrd for OrderValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (OrderValue::Float(a), OrderValue::Float(b)) => OrderedFloat(*a).cmp(&OrderedFloat(*b)),
            (OrderValue::Int(a), OrderValue::Int(b)) => a.cmp(b),
            (OrderValue::Float(a), OrderValue::Int(b)) => {
                // num_cmp() might return None only if the float value is NaN. We follow the
                // OrderedFloat logic here: the NaN is always greater than any other value.
                a.num_cmp(*b).unwrap_or(std::cmp::Ordering::Greater)
            }
            (OrderValue::Int(a), OrderValue::Float(b)) => {
                // Ditto, but the NaN is on the right side of the comparison.
                a.num_cmp(*b).unwrap_or(std::cmp::Ordering::Less)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::proptest;

    use crate::data_types::order_by::{Direction, OrderBy, OrderValue, StartFrom};
    use crate::json_path::JsonPath;
    use crate::types::RangeInterface;

    /// The f64 range built for an integer `start_from` must never exclude that value,
    /// including above 2^53 where `i as f64` rounds.
    #[test]
    fn as_range_includes_large_integer_start_values() {
        let starts = [
            0,
            (1 << 53) - 1,
            1 << 53,
            (1 << 53) + 1,
            1 << 60,
            (1 << 60) + 1,
            1_780_000_000_003_000_000,
            i64::MAX,
            -(1 << 53) - 1,
            -(1 << 60) - 1,
            i64::MIN + 1,
        ];
        for start in starts {
            for direction in [Direction::Asc, Direction::Desc] {
                let order_by = OrderBy {
                    key: JsonPath::new("n"),
                    direction: Some(direction),
                    start_from: Some(StartFrom::Integer(start)),
                };
                let RangeInterface::Float(range) = order_by.as_range() else {
                    panic!("integer start_from must produce a float range");
                };
                // f64 values this large are whole numbers, so i128 compares them exactly.
                match direction {
                    Direction::Asc => {
                        let gte = range.gte.unwrap().0 as i128;
                        assert!(gte <= i128::from(start), "{start}: gte {gte} excludes it");
                    }
                    Direction::Desc => {
                        let lte = range.lte.unwrap().0 as i128;
                        assert!(lte >= i128::from(start), "{start}: lte {lte} excludes it");
                    }
                }
            }
        }
    }

    proptest! {

        #[test]
        fn test_min_ordering_value(a in i64::MIN..0, b in f64::MIN..0.0) {
            assert!(OrderValue::MIN.cmp(&OrderValue::from(a)).is_le());
            assert!(OrderValue::MIN.cmp(&OrderValue::from(b)).is_le());
            assert!(OrderValue::MIN.cmp(&OrderValue::from(f64::NAN)).is_le());
        }

        #[test]
        fn test_max_ordering_value(a in 0..i64::MAX, b in 0.0..f64::MAX) {
            assert!(OrderValue::MAX.cmp(&OrderValue::from(a)).is_ge());
            assert!(OrderValue::MAX.cmp(&OrderValue::from(b)).is_ge());
            assert!(OrderValue::MAX.cmp(&OrderValue::from(f64::NAN)).is_ge());
        }
    }
}
