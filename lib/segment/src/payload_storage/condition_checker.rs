//! Contains functions for interpreting filter queries and defining if given points pass the conditions

use std::str::FromStr;

use ordered_float::OrderedFloat;
use serde_json::Value;

use crate::types::{
    AnyVariants, DateTimePayloadType, FieldCondition, FloatPayloadType, GeoBoundingBox, GeoPoint,
    GeoPolygon, GeoRadius, Match, MatchAny, MatchExcept, MatchPhrase, MatchText, MatchTextAny,
    MatchValue, Range, RangeInterface, ValueVariants, ValuesCount,
};

/// Threshold representing the point to which iterating through an IndexSet is more efficient than using hashing.
///
/// For sets smaller than this threshold iterating outperforms hashing.
/// For more information see <https://github.com/qdrant/qdrant/pull/3525>.
pub const INDEXSET_ITER_THRESHOLD: usize = 13;

pub trait ValueChecker {
    fn check_match(&self, payload: &Value) -> bool;

    #[inline]
    fn _check(&self, payload: &Value) -> bool {
        match payload {
            Value::Array(values) => values.iter().any(|x| self.check_match(x)),
            _ => self.check_match(payload),
        }
    }

    fn check(&self, payload: &Value) -> bool {
        self._check(payload)
    }

    /// Check condition in case of empty payload value
    fn check_empty(&self) -> bool {
        false
    }
}

fn check_is_empty(is_empty: bool, payload_value: &Value) -> bool {
    match payload_value {
        Value::Null => is_empty,
        Value::Bool(_) => !is_empty,
        Value::Number(_) => !is_empty,
        Value::String(_) => !is_empty,
        Value::Array(array) => array.is_empty() == is_empty,
        Value::Object(_) => !is_empty,
    }
}

fn check_is_null(is_null: bool, payload_value: &Value) -> bool {
    match payload_value {
        Value::Null => is_null,
        Value::Bool(_) => !is_null,
        Value::Number(_) => !is_null,
        Value::String(_) => !is_null,
        Value::Array(array) => array.iter().any(|x| x.is_null()) == is_null,
        Value::Object(_) => !is_null,
    }
}

impl ValueChecker for FieldCondition {
    fn check_match(&self, payload: &Value) -> bool {
        // Destructuring so compiler can check that we don't forget a condition
        let FieldCondition {
            r#match,
            range,
            geo_radius,
            geo_bounding_box,
            geo_polygon,
            values_count,
            key: _,
            is_empty,
            is_null,
        } = self;

        r#match
            .as_ref()
            .is_some_and(|condition| condition.check_match(payload))
            || range
                .as_ref()
                .is_some_and(|range_interface| match range_interface {
                    RangeInterface::Float(condition) => condition.check_match(payload),
                    RangeInterface::DateTime(condition) => condition.check_match(payload),
                })
            || geo_radius
                .as_ref()
                .is_some_and(|condition| condition.check_match(payload))
            || geo_bounding_box
                .as_ref()
                .is_some_and(|condition| condition.check_match(payload))
            || geo_polygon
                .as_ref()
                .is_some_and(|condition| condition.check_match(payload))
            || values_count
                .as_ref()
                .is_some_and(|condition| condition.check_match(payload))
            || is_empty.is_some_and(|is_empty| check_is_empty(is_empty, payload))
            || is_null.is_some_and(|is_null| check_is_null(is_null, payload))
    }

    fn check(&self, payload: &Value) -> bool {
        let FieldCondition {
            r#match: _,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count,
            key: _,
            is_empty,
            is_null,
        } = self;

        if values_count.is_some() {
            self.values_count
                .as_ref()
                .unwrap()
                .check_count_from(payload)
        } else if is_empty.is_some() {
            check_is_empty(is_empty.unwrap(), payload)
        } else if is_null.is_some() {
            check_is_null(is_null.unwrap(), payload)
        } else {
            self._check(payload)
        }
    }

    fn check_empty(&self) -> bool {
        let FieldCondition {
            r#match: _,
            range: _,
            geo_radius: _,
            geo_bounding_box: _,
            geo_polygon: _,
            values_count: _,
            key: _,
            is_empty,
            is_null,
        } = self;
        if let Some(is_empty) = is_empty {
            return *is_empty;
        }
        if let Some(is_null) = is_null {
            return !*is_null;
        }
        false
    }
}

impl ValueChecker for Match {
    fn check_match(&self, payload: &Value) -> bool {
        match self {
            Match::Value(MatchValue { value }) => match (payload, value) {
                (Value::Bool(stored), ValueVariants::Bool(val)) => stored == val,
                (Value::String(stored), ValueVariants::String(val)) => stored == val,
                (Value::Number(stored), ValueVariants::Integer(val)) => {
                    stored.as_i64().map(|num| num == *val).unwrap_or(false)
                }
                _ => false,
            },
            Match::Text(MatchText { text }) | Match::Phrase(MatchPhrase { phrase: text }) => {
                match payload {
                    Value::String(stored) => stored.contains(text),
                    _ => false,
                }
            }
            Match::TextAny(MatchTextAny { text_any }) => match payload {
                Value::String(stored) => text_any
                    .split_whitespace()
                    .any(|token| stored.contains(token)),
                _ => false,
            },
            Match::Any(MatchAny { any }) => match (payload, any) {
                (Value::String(stored), AnyVariants::Strings(list)) => {
                    if list.len() < INDEXSET_ITER_THRESHOLD {
                        list.iter().any(|i| i.as_str() == stored.as_str())
                    } else {
                        list.contains(stored.as_str())
                    }
                }
                (Value::Number(stored), AnyVariants::Integers(list)) => stored
                    .as_i64()
                    .map(|num| {
                        if list.len() < INDEXSET_ITER_THRESHOLD {
                            list.iter().any(|i| *i == num)
                        } else {
                            list.contains(&num)
                        }
                    })
                    .unwrap_or(false),
                _ => false,
            },
            Match::Except(MatchExcept { except }) => match (payload, except) {
                (Value::String(stored), AnyVariants::Strings(list)) => {
                    if list.len() < INDEXSET_ITER_THRESHOLD {
                        !list.iter().any(|i| i.as_str() == stored.as_str())
                    } else {
                        !list.contains(stored.as_str())
                    }
                }
                (Value::Number(stored), AnyVariants::Integers(list)) => stored
                    .as_i64()
                    .map(|num| {
                        if list.len() < INDEXSET_ITER_THRESHOLD {
                            !list.iter().any(|i| *i == num)
                        } else {
                            !list.contains(&num)
                        }
                    })
                    .unwrap_or(true),
                (Value::Null, _) => false,
                (Value::Bool(_), _) => true,
                (Value::Array(_), _) => true, // Array inside array is not flattened
                (Value::Object(_), _) => true,
                (Value::Number(_), _) => true,
                (Value::String(_), _) => true,
            },
        }
    }
}

impl ValueChecker for Range<OrderedFloat<FloatPayloadType>> {
    fn check_match(&self, payload: &Value) -> bool {
        match payload {
            Value::Number(num) => num
                .as_f64()
                .map(|number| self.check_range(OrderedFloat(number)))
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl ValueChecker for Range<DateTimePayloadType> {
    fn check_match(&self, payload: &Value) -> bool {
        payload
            .as_str()
            .and_then(|s| DateTimePayloadType::from_str(s).ok())
            .is_some_and(|x| self.check_range(x))
    }
}

impl ValueChecker for GeoBoundingBox {
    fn check_match(&self, payload: &Value) -> bool {
        match payload {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return self.check_point(&GeoPoint::new_unchecked(lon, lat));
                }
                false
            }
            _ => false,
        }
    }
}

impl ValueChecker for GeoRadius {
    fn check_match(&self, payload: &Value) -> bool {
        match payload {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return self.check_point(&GeoPoint::new_unchecked(lon, lat));
                }
                false
            }
            _ => false,
        }
    }
}

impl ValueChecker for GeoPolygon {
    fn check_match(&self, payload: &Value) -> bool {
        match payload {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return self
                        .convert()
                        .check_point(&GeoPoint::new_unchecked(lon, lat));
                }
                false
            }
            _ => false,
        }
    }
}

impl ValueChecker for ValuesCount {
    fn check_match(&self, payload: &Value) -> bool {
        self.check_count_from(payload)
    }

    fn check(&self, payload: &Value) -> bool {
        self.check_count_from(payload)
    }

    fn check_empty(&self) -> bool {
        self.check_count(0)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::json_path::JsonPath;
    use crate::types::GeoPoint;

    #[test]
    fn test_geo_matching() {
        let berlin_and_moscow = json!([
            {
                "lat": 52.52197645,
                "lon": 13.413637435864272
            },
            {
                "lat": 55.7536283,
                "lon": 37.62137960067377,
            }
        ]);

        let near_berlin_query = GeoRadius {
            center: GeoPoint::new_unchecked(13.413637, 52.521976),
            radius: OrderedFloat(2000.0),
        };
        let miss_geo_query = GeoRadius {
            center: GeoPoint::new_unchecked(20.423637, 52.511),
            radius: OrderedFloat(2000.0),
        };

        assert!(near_berlin_query.check(&berlin_and_moscow));
        assert!(!miss_geo_query.check(&berlin_and_moscow));
    }

    #[test]
    fn test_value_count() {
        let countries = json!([
            {
                "country": "Germany",
            },
            {
                "country": "France",
            }
        ]);

        let gt_one_country_query = ValuesCount {
            lt: None,
            gt: Some(1),
            gte: None,
            lte: None,
        };
        assert!(gt_one_country_query.check(&countries));

        let gt_two_countries_query = ValuesCount {
            lt: None,
            gt: Some(2),
            gte: None,
            lte: None,
        };
        assert!(!gt_two_countries_query.check(&countries));

        let gte_two_countries_query = ValuesCount {
            lt: None,
            gt: None,
            gte: Some(2),
            lte: None,
        };
        assert!(gte_two_countries_query.check(&countries));
    }

    #[test]
    fn test_value_checker_for_null_or_empty() {
        let array = json!([]);
        let array_with_null = json!([null]);
        let array_with_something = json!([true]);
        let array_with_null_and_something = json!([true, null]);
        let object = json!({});
        let string = json!("string");
        let number = json!(1);
        let bool = json!(true);

        let key = JsonPath::new("key");

        let is_empty = FieldCondition {
            r#match: None,
            range: None,
            geo_radius: None,
            geo_bounding_box: None,
            geo_polygon: None,
            values_count: None,
            key: key.clone(),
            is_empty: Some(true),
            is_null: None,
        };

        let is_not_empty = FieldCondition {
            r#match: None,
            range: None,
            geo_radius: None,
            geo_bounding_box: None,
            geo_polygon: None,
            values_count: None,
            key: key.clone(),
            is_empty: Some(false),
            is_null: None,
        };

        let is_null = FieldCondition {
            r#match: None,
            range: None,
            geo_radius: None,
            geo_bounding_box: None,
            geo_polygon: None,
            values_count: None,
            key: key.clone(),
            is_empty: None,
            is_null: Some(true),
        };

        let is_not_null = FieldCondition {
            r#match: None,
            range: None,
            geo_radius: None,
            geo_bounding_box: None,
            geo_polygon: None,
            values_count: None,
            key: key.clone(),
            is_empty: None,
            is_null: Some(false),
        };

        assert!(is_empty.check(&array));
        assert!(!is_empty.check(&array_with_null));
        assert!(!is_empty.check(&array_with_something));
        assert!(!is_empty.check(&array_with_null_and_something));
        assert!(!is_empty.check(&object));
        assert!(!is_empty.check(&string));
        assert!(!is_empty.check(&number));
        assert!(!is_empty.check(&bool));

        assert!(!is_not_empty.check(&array));
        assert!(is_not_empty.check(&array_with_null));
        assert!(is_not_empty.check(&array_with_something));
        assert!(is_not_empty.check(&array_with_null_and_something));
        assert!(is_not_empty.check(&object));
        assert!(is_not_empty.check(&string));
        assert!(is_not_empty.check(&number));
        assert!(is_not_empty.check(&bool));

        assert!(!is_null.check(&array));
        assert!(is_null.check(&array_with_null));
        assert!(!is_null.check(&array_with_something));
        assert!(is_null.check(&array_with_null_and_something));
        assert!(!is_null.check(&object));
        assert!(!is_null.check(&string));
        assert!(!is_null.check(&number));
        assert!(!is_null.check(&bool));

        assert!(is_not_null.check(&array));
        assert!(!is_not_null.check(&array_with_null));
        assert!(is_not_null.check(&array_with_something));
        assert!(!is_not_null.check(&array_with_null_and_something));
        assert!(is_not_null.check(&object));
        assert!(is_not_null.check(&string));
        assert!(is_not_null.check(&number));
        assert!(is_not_null.check(&bool));
    }
}
