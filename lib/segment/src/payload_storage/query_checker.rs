use crate::payload_storage::payload_storage::{ConditionChecker};
use crate::types::{Filter, PayloadKeyType, PayloadType, Condition, GeoBoundingBox, Range, Match, TheMap, PointOffsetType, GeoRadius};
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;
use crate::id_mapper::id_mapper::IdMapper;
use geo::Point;
use geo::algorithm::haversine_distance::HaversineDistance;


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

fn match_geo_radius(
    payload: &PayloadType,
    geo_radius_query: &GeoRadius,
) -> bool {
    return match payload {
        PayloadType::Geo(geo_points) => {
            let query_center = Point::new(
                geo_radius_query.center.lon,
                geo_radius_query.center.lat);

            geo_points
                .iter()
                .any(|geo_point|
                    query_center.haversine_distance(
                        &Point::new(geo_point.lon, geo_point.lat)
                    ) < geo_radius_query.radius
                )
        }
        _ => false,
    };
}


fn check_condition<F>(checker: &F, condition: &Condition) -> bool
    where F: Fn(&Condition) -> bool {
    match condition {
        Condition::Filter(filter) => check_filter(checker, filter),
        _ => checker(condition)
    }
}

fn check_filter<F>(checker: &F, filter: &Filter) -> bool
    where F: Fn(&Condition) -> bool {
    return check_should(checker, &filter.should)
        && check_must(checker, &filter.must)
        && check_must_not(checker, &filter.must_not);
}

fn check_should<F>(checker: &F, should: &Option<Vec<Condition>>) -> bool
    where F: Fn(&Condition) -> bool {
    let check = |x| check_condition(checker, x);
    match should {
        None => true,
        Some(conditions) => conditions.iter().any(check)
    }
}


fn check_must<F>(checker: &F, must: &Option<Vec<Condition>>) -> bool
    where F: Fn(&Condition) -> bool {
    let check = |x| check_condition(checker, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}

fn check_must_not<F>(checker: &F, must: &Option<Vec<Condition>>) -> bool
    where F: Fn(&Condition) -> bool {
    let check = |x| !check_condition(checker, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check)
    }
}


pub struct SimpleConditionChecker {
    payload_storage: Arc<AtomicRefCell<SimplePayloadStorage>>,
    id_mapper: Arc<AtomicRefCell<dyn IdMapper>>,
}

impl SimpleConditionChecker {
    pub fn new(payload_storage: Arc<AtomicRefCell<SimplePayloadStorage>>,
               id_mapper: Arc<AtomicRefCell<dyn IdMapper>>) -> Self {
        SimpleConditionChecker {
            payload_storage,
            id_mapper,
        }
    }
}

// Uncomment when stabilized
// const EMPTY_PAYLOAD: TheMap<PayloadKeyType, PayloadType> = TheMap::new();

impl ConditionChecker for SimpleConditionChecker
{
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let empty_map: TheMap<PayloadKeyType, PayloadType> = TheMap::new();

        let payload_storage_guard = self.payload_storage.borrow();
        let payload_ptr = payload_storage_guard.payload_ptr(point_id);

        let payload = match payload_ptr {
            None => &empty_map,
            Some(x) => x
        };

        let checker = |condition: &Condition| {
            match condition {
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
                Condition::GeoRadius(geo_radius) => {
                    payload.get(&geo_radius.key)
                        .map(|p| match_geo_radius(p, geo_radius))
                        .unwrap_or(false)
                }
                Condition::HasId(ids) => {
                    let external_id = match self.id_mapper.borrow().external_id(point_id) {
                        None => return false,
                        Some(id) => id,
                    };
                    ids.contains(&external_id)
                }
                Condition::Filter(_) => panic!("Unexpected branching!")
            }
        };

        check_filter(&checker, query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PayloadType;
    use crate::types::GeoPoint;
    use std::collections::HashSet;
    use tempdir::TempDir;
    use crate::payload_storage::payload_storage::PayloadStorage;
    use crate::id_mapper::simple_id_mapper::SimpleIdMapper;

    #[test]
    fn test_geo_matching() {
        let berlin_and_moscow = PayloadType::Geo(vec![
            GeoPoint { lat: 52.52197645, lon: 13.413637435864272 },
            GeoPoint { lat: 55.7536283, lon: 37.62137960067377 }
        ]);
        let near_berlin_query = GeoRadius {
            key: "test".to_string(),
            center: GeoPoint { lat: 52.511, lon: 13.423637 },
            radius: 2000.0,
        };
        let miss_geo_query = GeoRadius {
            key: "test".to_string(),
            center: GeoPoint { lat: 52.511, lon: 20.423637 },
            radius: 2000.0,
        };

        assert!(match_geo_radius(&berlin_and_moscow, &near_berlin_query));
        assert!(!match_geo_radius(&berlin_and_moscow, &miss_geo_query));
    }


    #[test]
    fn test_condition_checker() {
        let dir = TempDir::new("payload_dir").unwrap();
        let dir_id_mapper = TempDir::new("id_mapper_dir").unwrap();

        let payload: TheMap<PayloadKeyType, PayloadType> = [
            ("location".to_owned(), PayloadType::Geo(vec![GeoPoint { lon: 13.404954, lat: 52.520008 }])),
            ("price".to_owned(), PayloadType::Float(vec![499.90])),
            ("amount".to_owned(), PayloadType::Integer(vec![10])),
            ("rating".to_owned(), PayloadType::Integer(vec![3, 7, 9, 9])),
            ("color".to_owned(), PayloadType::Keyword(vec!["red".to_owned()])),
            ("has_delivery".to_owned(), PayloadType::Integer(vec![1])),
        ].iter().cloned().collect();

        let mut payload_storage = SimplePayloadStorage::open(dir.path()).unwrap();
        let mut id_mapper = SimpleIdMapper::open(dir_id_mapper.path()).unwrap();

        id_mapper.set_link(0, 0).unwrap();
        id_mapper.set_link(1, 1).unwrap();
        id_mapper.set_link(2, 2).unwrap();
        id_mapper.set_link(10, 10).unwrap();
        payload_storage.assign_all(0, payload).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(id_mapper)),
        );

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
        assert!(payload_checker.check(0, &query));

        let query = Filter {
            should: None,
            must: Some(vec![match_blue.clone()]),
            must_not: None,
        };
        assert!(!payload_checker.check(0, &query));

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![match_blue.clone()]),
        };
        assert!(payload_checker.check(0, &query));

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![match_red.clone()]),
        };
        assert!(!payload_checker.check(0, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            must: Some(vec![with_delivery.clone(), in_berlin.clone()]),
            must_not: None,
        };
        assert!(payload_checker.check(0, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            must: Some(vec![with_delivery.clone(), in_moscow.clone()]),
            must_not: None,
        };
        assert!(!payload_checker.check(0, &query));

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
        assert!(!payload_checker.check(0, &query));

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
        assert!(payload_checker.check(0, &query));


        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![with_bad_rating.clone()]),
        };
        assert!(!payload_checker.check(0, &query));


        let ids: HashSet<_> = vec![1, 2, 3].into_iter().collect();


        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids)]),
        };
        assert!(!payload_checker.check(2, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().collect();


        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids)]),
        };
        assert!(payload_checker.check(10, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().collect();

        let query = Filter {
            should: None,
            must: Some(vec![Condition::HasId(ids)]),
            must_not: None,
        };
        assert!(payload_checker.check(2, &query));
    }
}