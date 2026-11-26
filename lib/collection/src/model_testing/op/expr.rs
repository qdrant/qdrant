//! Generated boolean filters over the indexed payload fields, with an evaluator of their own.
//!
//! The other filter ops each probe one condition. Here a filter is a small tree: leaves are
//! conditions on `num`, `f`, `b`, `d`, `g`, `tag`, `url` and `e`, and the nodes are the four
//! clauses of a `Filter`. The model evaluates the tree on the payload JSON without going through
//! the engine's condition checker, so index, payload fallback and the checker are all on the
//! tested side.
//!
//! Bounds are drawn from the pools the payload values come from (`generators::FLOATS` and the
//! like), or halfway between two of them,
//! so no comparison depends on float rounding.

use std::str::FromStr;

use ordered_float::OrderedFloat;
use rand::seq::IndexedRandom;
use rand::{Rng, RngExt};
use segment::types::{
    AnyVariants, Condition, DateTimePayloadType, FieldCondition, Filter, GeoBoundingBox, GeoPoint,
    Match, MatchAny, MatchExcept, MinShould, Payload, Range,
};
use serde_json::Value;

use super::generators::{DATETIMES, TAGS, random_emptiness_key, random_url_prefix_probe};
use super::{ModelFilter, is_empty_filter, is_empty_matches, is_null_filter, is_null_matches};

/// Bounds between and at the values of `FLOATS`.
const FLOAT_BOUNDS: [f64; 9] = [0.0, 0.125, 0.25, 0.375, 0.5, 0.625, 0.75, 0.875, 1.0];

#[derive(Debug, Clone)]
pub(in super::super) enum FilterExpr {
    Leaf(Leaf),
    Must(Vec<FilterExpr>),
    Should(Vec<FilterExpr>),
    MustNot(Vec<FilterExpr>),
    MinShould {
        conditions: Vec<FilterExpr>,
        min_count: usize,
    },
}

#[derive(Debug, Clone)]
pub(in super::super) enum Leaf {
    NumEq(i64),
    /// `gte <= num < lt`
    NumRange {
        gte: Option<i64>,
        lt: Option<i64>,
    },
    NumAny(Vec<i64>),
    NumExcept(Vec<i64>),
    /// `gte <= f < lt`
    FloatRange {
        gte: Option<f64>,
        lt: Option<f64>,
    },
    BoolEq(bool),
    /// `gte <= d < lt`
    DatetimeRange {
        gte: Option<&'static str>,
        lt: Option<&'static str>,
    },
    TagEq(&'static str),
    TagAny(Vec<&'static str>),
    UrlPrefix(&'static str),
    /// Corners sit on `.05` offsets, between the `.1` grid the points are snapped to.
    GeoBox {
        lat_lo: f64,
        lat_hi: f64,
        lon_lo: f64,
        lon_hi: f64,
    },
    IsEmpty(&'static str),
    IsNull(&'static str),
}

impl FilterExpr {
    pub(in super::super) fn random(rng: &mut impl Rng) -> Self {
        Self::random_at(rng, 0)
    }

    fn random_at(rng: &mut impl Rng, depth: usize) -> Self {
        let leaf = depth >= 2 || rng.random_bool(if depth == 0 { 0.3 } else { 0.7 });
        if leaf {
            return FilterExpr::Leaf(Leaf::random(rng));
        }
        let count = rng.random_range(1..=3);
        let children: Vec<_> = (0..count)
            .map(|_| Self::random_at(rng, depth + 1))
            .collect();
        match rng.random_range(0..4) {
            0 => FilterExpr::Must(children),
            1 => FilterExpr::Should(children),
            2 => FilterExpr::MustNot(children),
            _ => FilterExpr::MinShould {
                min_count: rng.random_range(1..=children.len()),
                conditions: children,
            },
        }
    }

    fn children(&self) -> &[FilterExpr] {
        match self {
            FilterExpr::Leaf(_) => &[],
            FilterExpr::Must(children)
            | FilterExpr::Should(children)
            | FilterExpr::MustNot(children) => children,
            FilterExpr::MinShould { conditions, .. } => conditions,
        }
    }

    /// Whether the filter holds an `except` on `num`. The integer index and the payload check
    /// disagree on a `num` that is not an integer (an object, after `SetPayloadByKey`): the
    /// check says such a value is "not in the list", the index never returns the point. Which
    /// one answers depends on the query plan, so such points are ambiguous for the model.
    pub(in super::super) fn has_except(&self) -> bool {
        match self {
            FilterExpr::Leaf(Leaf::NumExcept(_)) => true,
            FilterExpr::Leaf(_) => false,
            FilterExpr::Must(_)
            | FilterExpr::Should(_)
            | FilterExpr::MustNot(_)
            | FilterExpr::MinShould { .. } => self.children().iter().any(Self::has_except),
        }
    }

    fn condition(&self) -> Condition {
        match self {
            FilterExpr::Leaf(leaf) => leaf.condition(),
            FilterExpr::Must(_)
            | FilterExpr::Should(_)
            | FilterExpr::MustNot(_)
            | FilterExpr::MinShould { .. } => Condition::Filter(self.filter()),
        }
    }

    fn conditions(children: &[FilterExpr]) -> Vec<Condition> {
        children.iter().map(FilterExpr::condition).collect()
    }
}

impl ModelFilter for FilterExpr {
    fn filter(&self) -> Filter {
        let empty = Filter {
            should: None,
            min_should: None,
            must: None,
            must_not: None,
        };
        match self {
            FilterExpr::Leaf(leaf) => Filter {
                must: Some(vec![leaf.condition()]),
                ..empty
            },
            FilterExpr::Must(children) => Filter {
                must: Some(Self::conditions(children)),
                ..empty
            },
            FilterExpr::Should(children) => Filter {
                should: Some(Self::conditions(children)),
                ..empty
            },
            FilterExpr::MustNot(children) => Filter {
                must_not: Some(Self::conditions(children)),
                ..empty
            },
            FilterExpr::MinShould {
                conditions,
                min_count,
            } => Filter {
                min_should: Some(MinShould {
                    conditions: Self::conditions(conditions),
                    min_count: *min_count,
                }),
                ..empty
            },
        }
    }

    fn matches(&self, payload: &Payload) -> bool {
        match self {
            FilterExpr::Leaf(leaf) => leaf.matches(payload),
            FilterExpr::Must(children) => children.iter().all(|c| c.matches(payload)),
            FilterExpr::Should(children) => children.iter().any(|c| c.matches(payload)),
            FilterExpr::MustNot(children) => !children.iter().any(|c| c.matches(payload)),
            FilterExpr::MinShould {
                conditions,
                min_count,
            } => conditions.iter().filter(|c| c.matches(payload)).count() >= *min_count,
        }
    }

    fn ambiguous(&self, payload: &Payload) -> bool {
        self.has_except()
            && values(payload, "num")
                .iter()
                .any(|value| value.as_i64().is_none())
    }
}

fn field(key: &str, r#match: Match) -> Condition {
    Condition::Field(FieldCondition::new_match(key.parse().unwrap(), r#match))
}

fn float_range(key: &str, gte: Option<f64>, lt: Option<f64>) -> Condition {
    Condition::Field(FieldCondition::new_range(
        key.parse().unwrap(),
        Range {
            lt: lt.map(OrderedFloat),
            gt: None,
            gte: gte.map(OrderedFloat),
            lte: None,
        },
    ))
}

fn datetime(value: &str) -> DateTimePayloadType {
    DateTimePayloadType::from_str(value).expect("valid datetime")
}

/// The values a field condition is checked against: the elements of an array, or the value.
fn values<'a>(payload: &'a Payload, key: &str) -> &'a [Value] {
    match payload.0.get(key) {
        None => &[],
        Some(Value::Array(values)) => values,
        Some(value) => std::slice::from_ref(value),
    }
}

fn in_range<T: PartialOrd>(value: T, gte: Option<T>, lt: Option<T>) -> bool {
    gte.is_none_or(|gte| value >= gte) && lt.is_none_or(|lt| value < lt)
}

impl Leaf {
    fn random(rng: &mut impl Rng) -> Self {
        match rng.random_range(0..13) {
            0 => Leaf::NumEq(rng.random_range(0..100)),
            1 => {
                // A window of the 0..100 domain, sometimes open on one side.
                let lo = rng.random_range(0..100);
                let hi = rng.random_range(lo..=100);
                match rng.random_range(0..4) {
                    0 => Leaf::NumRange {
                        gte: Some(lo),
                        lt: None,
                    },
                    1 => Leaf::NumRange {
                        gte: None,
                        lt: Some(hi),
                    },
                    _ => Leaf::NumRange {
                        gte: Some(lo),
                        lt: Some(hi),
                    },
                }
            }
            2 => Leaf::NumAny(Self::random_nums(rng)),
            3 => Leaf::NumExcept(Self::random_nums(rng)),
            4 => {
                let a = *FLOAT_BOUNDS.choose(rng).unwrap();
                let b = *FLOAT_BOUNDS.choose(rng).unwrap();
                Leaf::FloatRange {
                    gte: rng.random_bool(0.8).then_some(a.min(b)),
                    lt: rng.random_bool(0.8).then_some(a.max(b)),
                }
            }
            5 => Leaf::BoolEq(rng.random_bool(0.5)),
            6 => {
                let a = rng.random_range(0..DATETIMES.len());
                let b = rng.random_range(0..DATETIMES.len());
                Leaf::DatetimeRange {
                    gte: rng.random_bool(0.8).then_some(DATETIMES[a.min(b)]),
                    lt: rng.random_bool(0.8).then_some(DATETIMES[a.max(b)]),
                }
            }
            7 => Leaf::TagEq(TAGS.choose(rng).unwrap()),
            8 => {
                let count = rng.random_range(1..=2);
                Leaf::TagAny(TAGS.sample(rng, count).copied().collect())
            }
            9 => Leaf::UrlPrefix(random_url_prefix_probe(rng)),
            10 => {
                // The points live in lat 40..50, lon -10..10.
                let lat_a = f64::from(rng.random_range(38..52));
                let lat_b = f64::from(rng.random_range(38..52));
                let lon_a = f64::from(rng.random_range(-12..12));
                let lon_b = f64::from(rng.random_range(-12..12));
                Leaf::GeoBox {
                    lat_lo: lat_a.min(lat_b) + 0.05,
                    lat_hi: lat_a.max(lat_b) + 1.05,
                    lon_lo: lon_a.min(lon_b) + 0.05,
                    lon_hi: lon_a.max(lon_b) + 1.05,
                }
            }
            11 => Leaf::IsEmpty(random_emptiness_key(rng)),
            _ => Leaf::IsNull(random_emptiness_key(rng)),
        }
    }

    fn random_nums(rng: &mut impl Rng) -> Vec<i64> {
        // Up to 40 values: past the engine's threshold for switching from a scan of the list
        // to a set lookup.
        let count = *[1, 2, 5, 40].choose(rng).unwrap();
        (0..count).map(|_| rng.random_range(0..100)).collect()
    }

    fn condition(&self) -> Condition {
        match self {
            Leaf::NumEq(num) => field("num", Match::from(*num)),
            Leaf::NumRange { gte, lt } => {
                float_range("num", gte.map(|v| v as f64), lt.map(|v| v as f64))
            }
            Leaf::NumAny(nums) => field(
                "num",
                Match::Any(MatchAny {
                    any: AnyVariants::Integers(nums.iter().copied().collect()),
                }),
            ),
            Leaf::NumExcept(nums) => field(
                "num",
                Match::Except(MatchExcept {
                    except: AnyVariants::Integers(nums.iter().copied().collect()),
                }),
            ),
            Leaf::FloatRange { gte, lt } => float_range("f", *gte, *lt),
            Leaf::BoolEq(value) => field("b", Match::from(*value)),
            Leaf::DatetimeRange { gte, lt } => {
                Condition::Field(FieldCondition::new_datetime_range(
                    "d".parse().unwrap(),
                    Range {
                        lt: lt.map(datetime),
                        gt: None,
                        gte: gte.map(datetime),
                        lte: None,
                    },
                ))
            }
            Leaf::TagEq(tag) => field("tag", Match::from((*tag).to_string())),
            Leaf::TagAny(tags) => field(
                "tag",
                Match::Any(MatchAny {
                    any: AnyVariants::Strings(tags.iter().map(|tag| (*tag).to_string()).collect()),
                }),
            ),
            Leaf::UrlPrefix(prefix) => field("url", Match::new_prefix(prefix)),
            Leaf::GeoBox {
                lat_lo,
                lat_hi,
                lon_lo,
                lon_hi,
            } => Condition::Field(FieldCondition::new_geo_bounding_box(
                "g".parse().unwrap(),
                GeoBoundingBox {
                    top_left: GeoPoint::new_unchecked(*lon_lo, *lat_hi),
                    bottom_right: GeoPoint::new_unchecked(*lon_hi, *lat_lo),
                },
            )),
            Leaf::IsEmpty(key) => is_empty_filter(key).must.unwrap().remove(0),
            Leaf::IsNull(key) => is_null_filter(key).must.unwrap().remove(0),
        }
    }

    fn matches(&self, payload: &Payload) -> bool {
        match self {
            Leaf::NumEq(num) => values(payload, "num")
                .iter()
                .any(|v| v.as_i64() == Some(*num)),
            Leaf::NumRange { gte, lt } => values(payload, "num").iter().any(|v| {
                v.as_f64()
                    .is_some_and(|v| in_range(v, gte.map(|b| b as f64), lt.map(|b| b as f64)))
            }),
            Leaf::NumAny(nums) => values(payload, "num")
                .iter()
                .any(|v| v.as_i64().is_some_and(|v| nums.contains(&v))),
            // `except` holds for a value of another type, an object for example, but not for a
            // field with no value at all.
            Leaf::NumExcept(nums) => values(payload, "num").iter().any(|v| match v {
                Value::Null => false,
                Value::Number(number) => number.as_i64().is_none_or(|v| !nums.contains(&v)),
                Value::Bool(_) | Value::String(_) | Value::Array(_) | Value::Object(_) => true,
            }),
            Leaf::FloatRange { gte, lt } => values(payload, "f")
                .iter()
                .any(|v| v.as_f64().is_some_and(|v| in_range(v, *gte, *lt))),
            Leaf::BoolEq(value) => values(payload, "b")
                .iter()
                .any(|v| v.as_bool() == Some(*value)),
            Leaf::DatetimeRange { gte, lt } => values(payload, "d").iter().any(|v| {
                v.as_str()
                    .and_then(|v| DateTimePayloadType::from_str(v).ok())
                    .is_some_and(|v| {
                        in_range(
                            v.timestamp(),
                            gte.map(|b| datetime(b).timestamp()),
                            lt.map(|b| datetime(b).timestamp()),
                        )
                    })
            }),
            Leaf::TagEq(tag) => values(payload, "tag")
                .iter()
                .any(|v| v.as_str() == Some(*tag)),
            Leaf::TagAny(tags) => values(payload, "tag")
                .iter()
                .any(|v| v.as_str().is_some_and(|v| tags.contains(&v))),
            Leaf::UrlPrefix(prefix) => values(payload, "url")
                .iter()
                .any(|v| v.as_str().is_some_and(|v| v.starts_with(prefix))),
            Leaf::GeoBox {
                lat_lo,
                lat_hi,
                lon_lo,
                lon_hi,
            } => values(payload, "g").iter().any(|v| {
                let lat = v.get("lat").and_then(Value::as_f64);
                let lon = v.get("lon").and_then(Value::as_f64);
                match (lat, lon) {
                    (Some(lat), Some(lon)) => {
                        (*lat_lo..*lat_hi).contains(&lat) && (*lon_lo..*lon_hi).contains(&lon)
                    }
                    (None, _) | (_, None) => false,
                }
            }),
            Leaf::IsEmpty(key) => is_empty_matches(payload, key),
            Leaf::IsNull(key) => is_null_matches(payload, key),
        }
    }
}
