//! Contains functions for interpreting filter queries and defining if given points pass the conditions

use serde_json::Value;
use smol_str::SmolStr;

use crate::types::{
    AnyVariants, DateTimePayloadType, FieldCondition, FloatPayloadType, GeoBoundingBox, GeoPoint,
    GeoPolygon, GeoRadius, Match, MatchAny, MatchExcept, MatchText, MatchValue, Range,
    RangeInterface, ValueVariants, ValuesCount,
};

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
    }

    fn check(&self, payload: &Value) -> bool {
        if self.values_count.is_some() {
            self.values_count.as_ref().unwrap().check_count(payload)
        } else {
            self._check(payload)
        }
    }
}

impl ValueChecker for Match {
    fn check_match(&self, payload: &Value) -> bool {
        match self {
            Match::Value(MatchValue { value }) => match (payload, value) {
                (Value::Bool(stored), ValueVariants::Bool(val)) => stored == val,
                (Value::String(stored), ValueVariants::Keyword(val)) => stored == val,
                (Value::Number(stored), ValueVariants::Integer(val)) => {
                    stored.as_i64().map(|num| num == *val).unwrap_or(false)
                }
                _ => false,
            },
            Match::Text(MatchText { text }) => match payload {
                Value::String(stored) => stored.contains(text),
                _ => false,
            },
            Match::Any(MatchAny { any }) => match (payload, any) {
                (Value::String(stored), AnyVariants::Keywords(list)) => {
                    let s: SmolStr = stored.into();
                    list.contains(&s)
                }
                (Value::Number(stored), AnyVariants::Integers(list)) => stored
                    .as_i64()
                    .map(|num| list.contains(&num))
                    .unwrap_or(false),
                _ => false,
            },
            Match::Except(MatchExcept { except }) => match (payload, except) {
                (Value::String(stored), AnyVariants::Keywords(list)) => {
                    let s: SmolStr = stored.into();
                    !list.contains(&s)
                }
                (Value::Number(stored), AnyVariants::Integers(list)) => stored
                    .as_i64()
                    .map(|num| !list.contains(&num))
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

impl ValueChecker for Range<FloatPayloadType> {
    fn check_match(&self, payload: &Value) -> bool {
        match payload {
            Value::Number(num) => num
                .as_f64()
                .map(|number| self.check_range(number))
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl ValueChecker for Range<DateTimePayloadType> {
    fn check_match(&self, payload: &Value) -> bool {
        payload
            .as_str()
            .and_then(|x| chrono::DateTime::parse_from_rfc3339(x).ok())
            .is_some_and(|x| self.check_range(x.into()))
    }
}

impl ValueChecker for GeoBoundingBox {
    fn check_match(&self, payload: &Value) -> bool {
        match payload {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return self.check_point(&GeoPoint { lon, lat });
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
                    return self.check_point(&GeoPoint { lon, lat });
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
                    return self.convert().check_point(&GeoPoint { lon, lat });
                }
                false
            }
            _ => false,
        }
    }
}

impl ValueChecker for ValuesCount {
    fn check_match(&self, payload: &Value) -> bool {
        self.check_count(payload)
    }

    fn check(&self, payload: &Value) -> bool {
        self.check_count(payload)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
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
            center: GeoPoint {
                lat: 52.511,
                lon: 13.423637,
            },
            radius: 2000.0,
        };
        let miss_geo_query = GeoRadius {
            center: GeoPoint {
                lat: 52.511,
                lon: 20.423637,
            },
            radius: 2000.0,
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
}
