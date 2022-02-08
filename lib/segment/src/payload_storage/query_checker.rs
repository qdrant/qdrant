use crate::id_tracker::IdTrackerSS;
use crate::payload_storage::condition_checker::{
    match_geo, match_geo_radius, match_payload, match_range,
};
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::ConditionChecker;
use crate::types::{Condition, Filter, PayloadKeyType, PayloadType, PointOffsetType, TheMap};
use atomic_refcell::AtomicRefCell;
use std::sync::Arc;

fn check_condition<F>(checker: &F, condition: &Condition) -> bool
where
    F: Fn(&Condition) -> bool,
{
    match condition {
        Condition::Filter(filter) => check_filter(checker, filter),
        _ => checker(condition),
    }
}

fn check_filter<F>(checker: &F, filter: &Filter) -> bool
where
    F: Fn(&Condition) -> bool,
{
    check_should(checker, &filter.should)
        && check_must(checker, &filter.must)
        && check_must_not(checker, &filter.must_not)
}

fn check_should<F>(checker: &F, should: &Option<Vec<Condition>>) -> bool
where
    F: Fn(&Condition) -> bool,
{
    let check = |x| check_condition(checker, x);
    match should {
        None => true,
        Some(conditions) => conditions.iter().any(check),
    }
}

fn check_must<F>(checker: &F, must: &Option<Vec<Condition>>) -> bool
where
    F: Fn(&Condition) -> bool,
{
    let check = |x| check_condition(checker, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check),
    }
}

fn check_must_not<F>(checker: &F, must: &Option<Vec<Condition>>) -> bool
where
    F: Fn(&Condition) -> bool,
{
    let check = |x| !check_condition(checker, x);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check),
    }
}

pub struct SimpleConditionChecker {
    payload_storage: Arc<AtomicRefCell<SimplePayloadStorage>>,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
}

impl SimpleConditionChecker {
    pub fn new(
        payload_storage: Arc<AtomicRefCell<SimplePayloadStorage>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    ) -> Self {
        SimpleConditionChecker {
            payload_storage,
            id_tracker,
        }
    }
}

// Uncomment when stabilized
// const EMPTY_PAYLOAD: TheMap<PayloadKeyType, PayloadType> = TheMap::new();

impl ConditionChecker for SimpleConditionChecker {
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let empty_map: TheMap<PayloadKeyType, PayloadType> = TheMap::new();

        let payload_storage_guard = self.payload_storage.borrow();
        let payload_ptr = payload_storage_guard.payload_ptr(point_id);

        let payload = match payload_ptr {
            None => &empty_map,
            Some(x) => x,
        };

        let checker = |condition: &Condition| {
            match condition {
                Condition::Field(field_condition) => {
                    payload.get(&field_condition.key).map_or(false, |p| {
                        let mut res = false;
                        // ToDo: Convert onto iterator over checkers, so it would be impossible to forget a condition
                        res = res
                            || field_condition
                                .r#match
                                .as_ref()
                                .map_or(false, |condition| match_payload(p, condition));
                        res = res
                            || field_condition
                                .range
                                .as_ref()
                                .map_or(false, |condition| match_range(p, condition));
                        res = res
                            || field_condition
                                .geo_radius
                                .as_ref()
                                .map_or(false, |condition| match_geo_radius(p, condition));
                        res = res
                            || field_condition
                                .geo_bounding_box
                                .as_ref()
                                .map_or(false, |condition| match_geo(p, condition));
                        res
                    })
                }
                Condition::HasId(has_id) => {
                    let external_id = match self.id_tracker.borrow().external_id(point_id) {
                        None => return false,
                        Some(id) => id,
                    };
                    has_id.has_id.contains(&external_id)
                }
                Condition::Filter(_) => panic!("Unexpected branching!"),
            }
        };

        check_filter(&checker, query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
    use crate::id_tracker::IdTracker;
    use crate::payload_storage::PayloadStorage;
    use crate::types::GeoPoint;
    use crate::types::{FieldCondition, GeoBoundingBox, Match, PayloadType, Range};
    use std::collections::HashSet;
    use tempdir::TempDir;

    #[test]
    fn test_condition_checker() {
        let dir = TempDir::new("payload_dir").unwrap();
        let dir_id_tracker = TempDir::new("id_tracker_dir").unwrap();

        let payload: TheMap<PayloadKeyType, PayloadType> = [
            (
                "location".to_owned(),
                PayloadType::Geo(vec![GeoPoint {
                    lon: 13.404954,
                    lat: 52.520008,
                }]),
            ),
            ("price".to_owned(), PayloadType::Float(vec![499.90])),
            ("amount".to_owned(), PayloadType::Integer(vec![10])),
            ("rating".to_owned(), PayloadType::Integer(vec![3, 7, 9, 9])),
            (
                "color".to_owned(),
                PayloadType::Keyword(vec!["red".to_owned()]),
            ),
            ("has_delivery".to_owned(), PayloadType::Integer(vec![1])),
        ]
        .iter()
        .cloned()
        .collect();

        let mut payload_storage = SimplePayloadStorage::open(dir.path()).unwrap();
        let mut id_tracker = SimpleIdTracker::open(dir_id_tracker.path()).unwrap();

        id_tracker.set_link(0.into(), 0).unwrap();
        id_tracker.set_link(1.into(), 1).unwrap();
        id_tracker.set_link(2.into(), 2).unwrap();
        id_tracker.set_link(10.into(), 10).unwrap();
        payload_storage.assign_all(0, payload).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(id_tracker)),
        );

        let match_red = Condition::Field(FieldCondition {
            key: "color".to_string(),
            r#match: Some(Match {
                keyword: Some("red".to_owned()),
                integer: None,
            }),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
        });

        let match_blue = Condition::Field(FieldCondition {
            key: "color".to_string(),
            r#match: Some(Match {
                keyword: Some("blue".to_owned()),
                integer: None,
            }),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
        });

        let with_delivery = Condition::Field(FieldCondition {
            key: "has_delivery".to_string(),
            r#match: Some(Match {
                keyword: None,
                integer: Some(1),
            }),
            range: None,
            geo_bounding_box: None,
            geo_radius: None,
        });

        let in_berlin = Condition::Field(FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            geo_bounding_box: Some(GeoBoundingBox {
                top_left: GeoPoint {
                    lon: 13.08835,
                    lat: 52.67551,
                },
                bottom_right: GeoPoint {
                    lon: 13.76116,
                    lat: 52.33826,
                },
            }),
            geo_radius: None,
        });

        let in_moscow = Condition::Field(FieldCondition {
            key: "location".to_string(),
            r#match: None,
            range: None,
            geo_bounding_box: Some(GeoBoundingBox {
                top_left: GeoPoint {
                    lon: 37.0366,
                    lat: 56.1859,
                },
                bottom_right: GeoPoint {
                    lon: 38.2532,
                    lat: 55.317,
                },
            }),
            geo_radius: None,
        });

        let with_bad_rating = Condition::Field(FieldCondition {
            key: "rating".to_string(),
            r#match: None,
            range: Some(Range {
                lt: None,
                gt: None,
                gte: None,
                lte: Some(5.),
            }),
            geo_bounding_box: None,
            geo_radius: None,
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
            must: Some(vec![with_delivery, in_moscow.clone()]),
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
                    must: Some(vec![match_blue, in_moscow]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    must: Some(vec![match_red, in_berlin]),
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
            must_not: Some(vec![with_bad_rating]),
        };
        assert!(!payload_checker.check(0, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().map(|x| x.into()).collect();

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids.into())]),
        };
        assert!(!payload_checker.check(2, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().map(|x| x.into()).collect();

        let query = Filter {
            should: None,
            must: None,
            must_not: Some(vec![Condition::HasId(ids.into())]),
        };
        assert!(payload_checker.check(10, &query));

        let ids: HashSet<_> = vec![1, 2, 3].into_iter().map(|x| x.into()).collect();

        let query = Filter {
            should: None,
            must: Some(vec![Condition::HasId(ids.into())]),
            must_not: None,
        };
        assert!(payload_checker.check(2, &query));
    }
}
