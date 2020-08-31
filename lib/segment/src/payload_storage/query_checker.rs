use crate::payload_storage::payload_storage::{ConditionChecker, PayloadStorage};
use crate::types::{Filter, PayloadKeyType, PayloadType, Condition, GeoBoundingBox, Range, Match, TheMap, PointOffsetType, PointIdType};
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;


fn match_payload(payload: &PayloadType, condition_match: &Match) -> bool {
    match payload {
        PayloadType::Keyword(payload_kws) => payload_kws
            .iter()
            .any(|payload_kw| condition_match.keyword
                .as_ref()
                .map(|x| x == payload_kw).unwrap_or(false)
            ),
        PayloadType::Integer(payload_ints) => payload_ints
            .iter()
            .cloned()
            .any(|payload_int| condition_match.integer
                .map(|x| x == payload_int).unwrap_or(false)),
        _ => false
    }
}

fn match_range(
    payload: &PayloadType,
    num_range: &Range,
) -> bool {
    let condition =
        |number| num_range.lt.map_or(true, |x| number < x)
            && num_range.gt.map_or(true, |x| number > x)
            && num_range.lte.map_or(true, |x| number <= x)
            && num_range.gte.map_or(true, |x| number >= x);

    match payload {
        PayloadType::Float(num) => num.iter().cloned().any(condition),
        PayloadType::Integer(num) => num.iter().cloned().any(|x| condition(x as f64)),
        _ => false
    }
}

fn match_geo(
    payload: &PayloadType,
    geo_bounding_box: &GeoBoundingBox,
) -> bool {
    return match payload {
        PayloadType::Geo(geo_points) => geo_points
            .iter()
            .any(|geo_point| (geo_bounding_box.top_left.lon < geo_point.lon)
                && (geo_point.lon < geo_bounding_box.bottom_right.lon)
                && (geo_bounding_box.bottom_right.lat < geo_point.lat)
                && (geo_point.lat < geo_bounding_box.top_left.lat)),
        _ => false,
    };
}


fn check_condition(point_id: PointIdType, payload: &TheMap<PayloadKeyType, PayloadType>, condition: &Condition) -> bool {
    match condition {
        Condition::Filter(filter) => check_filter(point_id, payload, filter),
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
        Condition::HasId(ids) => {
            ids.contains(&point_id)
        }
    }
}

fn check_filter(point_id: PointIdType, payload: &TheMap<PayloadKeyType, PayloadType>, filter: &Filter) -> bool {
    return check_should(point_id, payload, &filter.should)
        && check_must(point_id, payload, &filter.must)
        && check_must_not(point_id, payload, &filter.must_not);
}

fn check_should(point_id: PointIdType, payload: &TheMap<PayloadKeyType, PayloadType>, should: &Option<Vec<Condition>>) -> bool {
    let check = |x| check_condition(point_id, payload, x);
    match should {
        None => true,
        Some(conditions) => conditions.iter().any(check)
    }
}


fn check_must(point_id: PointIdType, payload: &TheMap<PayloadKeyType, PayloadType>, must: &Option<Vec<Condition>>) -> bool {
    let check = |x| check_condition(point_id, payload, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

fn check_must_not(point_id: PointIdType, payload: &TheMap<PayloadKeyType, PayloadType>, must: &Option<Vec<Condition>>) -> bool {
    let check = |x| !check_condition(point_id, payload, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

impl ConditionChecker for SimplePayloadStorage
{
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let external_id = match self.point_external_id(point_id) {
            None => return false,
            Some(id) => id,
        };
        let payload = self.payload(point_id);
        return check_filter(external_id, &payload, query);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PayloadType;
    use crate::types::GeoPoint;
    use std::collections::HashSet;

    #[test]
    fn test_condition_checker() {
        let payload: TheMap<PayloadKeyType, PayloadType> = [
            ("location".to_owned(), PayloadType::Geo(vec![GeoPoint { lon: 13.404954, lat: 52.520008 }])),
            ("price".to_owned(), PayloadType::Float(vec![499.90])),
            ("amount".to_owned(), PayloadType::Integer(vec![10])),
            ("rating".to_owned(), PayloadType::Integer(vec![3, 7, 9, 9])),
            ("color".to_owned(), PayloadType::Keyword(vec!["red".to_owned()])),
            ("has_delivery".to_owned(), PayloadType::Integer(vec![1])),
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

        let with_bad_rating = Condition::Range(Range {
            key: "rating".to_string(),
            lt: None,
            gt: None,
            gte: None,
            lte: Some(5.),
        });

        let query = Filter {
            should: None,
            must: Some(vec![match_red.clone()]),
            must_not: None,
        };
        assert!(check_filter(0, &payload, &query));

        let query = Filter {
            should: None,
            must: Some(vec![match_blue.clone()]),
            must_not: None,
        };
        assert!(!check_filter(0, &payload, &query));

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![match_blue.clone()]),
        };
        assert!(check_filter(0, &payload, &query));

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![match_red.clone()]),
        };
        assert!(!check_filter(0, &payload, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            must: Some(vec![with_delivery.clone(), in_berlin.clone()]),
            must_not: None,
        };
        assert!(check_filter(0, &payload, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            must: Some(vec![with_delivery.clone(), in_moscow.clone()]),
            must_not: None,
        };
        assert!(!check_filter(0, &payload, &query));

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
        assert!(!check_filter(0, &payload, &query));

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
        assert!(check_filter(0, &payload, &query));


        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![with_bad_rating.clone()]),
        };
        assert!(!check_filter(0, &payload, &query));


        let ids: HashSet<_> = vec![1, 2, 3].into_iter().collect();


        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids)]),
        };
        assert!(!check_filter(2, &payload, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().collect();


        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids)]),
        };
        assert!(check_filter(10, &payload, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().collect();

        let query = Filter {
            should: None,
            must: Some(vec![Condition::HasId(ids)]),
            must_not: None,
        };
        assert!(check_filter(2, &payload, &query));
    }
}