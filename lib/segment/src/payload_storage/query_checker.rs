use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage, TheMap};
use crate::types::{Filter, PayloadKeyType, PayloadType, Condition, GeoBoundingBox, Range, Match};


fn match_payload(payload: &PayloadType, condition_match: &Match) -> bool {
    match payload {
        PayloadType::Keyword(payload_kw) => condition_match.keyword
            .as_ref().map(|x| x == payload_kw).unwrap_or(false),
        &PayloadType::Integer(payload_int) => condition_match.integer
            .map(|x| x == payload_int).unwrap_or(false),
        _ => false
    }
}

fn match_range(
    payload: &PayloadType,
    num_range: &Range,
) -> bool {
    let number: Option<f64> = match payload {
        &PayloadType::Float(num) => Some(num),
        &PayloadType::Integer(num) => Some(num as f64),
        _ => None
    };

    match number {
        Some(number) => num_range.lt.map_or(true, |x| number < x)
            && num_range.gt.map_or(true, |x| number > x)
            && num_range.lte.map_or(true, |x| number <= x)
            && num_range.gte.map_or(true, |x| number >= x),
        None => false
    }
}

fn match_geo(
    payload: &PayloadType,
    geo_bounding_box: &GeoBoundingBox,
) -> bool {
    return match payload {
        PayloadType::Geo(geo_point) => {
            // let max_lon = max(geo_bounding_box.top_left.lon, geo_bounding_box.bottom_right.lon);
            // let min_lon = min(geo_bounding_box.top_left.lon, geo_bounding_box.bottom_right.lon);
            // let max_lat = max(geo_bounding_box.top_left.lat, geo_bounding_box.bottom_right.lat);
            // let min_lat = min(geo_bounding_box.top_left.lat, geo_bounding_box.bottom_right.lat);
            (geo_bounding_box.top_left.lon < geo_point.lon) && (geo_point.lon < geo_bounding_box.bottom_right.lon)
                && (geo_bounding_box.bottom_right.lat < geo_point.lat) && (geo_point.lat < geo_bounding_box.top_left.lat)
        }
        _ => false,
    };
}


fn check_condition(payload: &TheMap<PayloadKeyType, PayloadType>, condition: &Condition) -> bool {
    match condition {
        Condition::Filter(filter) => check_filter(payload, filter),
        Condition::Match(condition_match) => {
            payload.get(&condition_match.key)
                .map(|p| match_payload(p, condition_match))
                .unwrap_or(false)
        }
        Condition::Range(range) => {
            payload.get(&range.key)
                .map(|p| match_range(p, range))
                .unwrap_or(false)
        }
        Condition::GeoBoundingBox(geo_bounding_box) => {
            payload.get(&geo_bounding_box.key)
                .map(|p| match_geo(p, geo_bounding_box))
                .unwrap_or(false)
        }
    }
}

fn check_filter(payload: &TheMap<PayloadKeyType, PayloadType>, filter: &Filter) -> bool {
    return check_should(payload, &filter.should)
        && check_must(payload, &filter.must)
        && check_must_not(payload, &filter.must_not);
}

fn check_should(payload: &TheMap<PayloadKeyType, PayloadType>, should: &Option<Vec<Condition>>) -> bool {
    let check = |x| check_condition(payload, x);
    match should {
        None => true,
        Some(conditions) => conditions.iter().any(check)
    }
}


fn check_must(payload: &TheMap<PayloadKeyType, PayloadType>, must: &Option<Vec<Condition>>) -> bool {
    let check = |x| check_condition(payload, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

fn check_must_not(payload: &TheMap<PayloadKeyType, PayloadType>, must: &Option<Vec<Condition>>) -> bool {
    let check = |x| !check_condition(payload, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

impl<T> ConditionChecker for T
    where T: PayloadStorage
{
    fn check(&self, point_id: usize, query: &Filter) -> bool {
        let payload = self.payload(point_id);
        return check_filter(&payload, query);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PayloadType;
    use crate::types::GeoPoint;

    #[test]
    fn test_condition_checker() {
        let payload: TheMap<PayloadKeyType, PayloadType> = [
            ("location".to_owned(), PayloadType::Geo(GeoPoint { lon: 13.404954, lat: 52.520008 })),
            ("price".to_owned(), PayloadType::Float(499.90)),
            ("amount".to_owned(), PayloadType::Integer(10)),
            ("rating".to_owned(), PayloadType::Integer(9)),
            ("color".to_owned(), PayloadType::Keyword("red".to_owned())),
            ("has_delivery".to_owned(), PayloadType::Integer(1)),
        ].iter().cloned().collect();

        let match_red = Condition::Match(Match {
            key: "color".to_owned(),
            keyword: Some("red".to_owned()),
            integer: None,
        });

        let match_blue = Condition::Match(Match {
            key: "color".to_owned(),
            keyword: Some("blue".to_owned()),
            integer: None,
        });

        let with_delivery = Condition::Match(Match {
            key: "has_delivery".to_owned(),
            keyword: None,
            integer: Some(1),
        });

        let in_berlin = Condition::GeoBoundingBox(GeoBoundingBox {
            key: "location".to_string(),
            top_left: GeoPoint { lon: 13.08835, lat: 52.67551 },
            bottom_right: GeoPoint { lon: 13.76116, lat: 52.33826 },
        });

        let in_moscow = Condition::GeoBoundingBox(GeoBoundingBox {
            key: "location".to_string(),
            top_left: GeoPoint { lon: 37.0366, lat: 56.1859 },
            bottom_right: GeoPoint { lon: 38.2532, lat: 55.317 },
        });

        let query = Filter {
            should: None,
            must: Some(vec![match_red.clone()]),
            must_not: None,
        };
        assert!(check_filter(&payload, &query));

        let query = Filter {
            should: None,
            must: Some(vec![match_blue.clone()]),
            must_not: None,
        };
        assert!(!check_filter(&payload, &query));

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![match_blue.clone()]),
        };
        assert!(check_filter(&payload, &query));

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![match_red.clone()]),
        };
        assert!(!check_filter(&payload, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            must: Some(vec![with_delivery.clone(), in_berlin.clone()]),
            must_not: None,
        };
        assert!(check_filter(&payload, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            must: Some(vec![with_delivery.clone(), in_moscow.clone()]),
            must_not: None,
        };
        assert!(!check_filter(&payload, &query));

        let query = Filter {
            should: Some(vec![
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![match_red.clone(), in_moscow.clone()]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![match_blue.clone(), in_berlin.clone()]),
                    must_not: None,
                }),
            ]),
            must: None,
            must_not: None,
        };
        assert!(!check_filter(&payload, &query));

        let query = Filter {
            should: Some(vec![
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![match_blue.clone(), in_moscow.clone()]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![match_red.clone(), in_berlin.clone()]),
                    must_not: None,
                }),
            ]),
            must: None,
            must_not: None,
        };
        assert!(check_filter(&payload, &query));
    }
}