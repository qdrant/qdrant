use std::ops::Deref;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use serde_json::Value;

use crate::id_tracker::IdTrackerSS;
use crate::payload_storage::condition_checker::ValueChecker;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::ConditionChecker;
use crate::types::{Condition, Filter, IsEmptyCondition, Payload, PointOffsetType};

fn check_condition<F>(checker: &F, condition: &Condition) -> bool
where
    F: Fn(&Condition) -> bool,
{
    match condition {
        Condition::Filter(filter) => check_filter(checker, filter),
        _ => checker(condition),
    }
}

pub fn check_filter<F>(checker: &F, filter: &Filter) -> bool
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

pub fn check_payload(
    payload: &Payload,
    id_tracker: &IdTrackerSS,
    query: &Filter,
    point_id: PointOffsetType,
) -> bool {
    let checker = |condition: &Condition| {
        match condition {
            Condition::Field(field_condition) => {
                payload.get_value(&field_condition.key).map_or(false, |p| {
                    let mut res = false;
                    // ToDo: Convert onto iterator over checkers, so it would be impossible to forget a condition
                    res = res
                        || field_condition
                            .r#match
                            .as_ref()
                            .map_or(false, |condition| condition.check(p));
                    res = res
                        || field_condition
                            .range
                            .as_ref()
                            .map_or(false, |condition| condition.check(p));
                    res = res
                        || field_condition
                            .geo_radius
                            .as_ref()
                            .map_or(false, |condition| condition.check(p));
                    res = res
                        || field_condition
                            .geo_bounding_box
                            .as_ref()
                            .map_or(false, |condition| condition.check(p));
                    res = res
                        || field_condition
                            .values_count
                            .as_ref()
                            .map_or(false, |condition| condition.check(p));
                    res
                })
            }
            Condition::HasId(has_id) => {
                let external_id = match id_tracker.external_id(point_id) {
                    None => return false,
                    Some(id) => id,
                };
                has_id.has_id.contains(&external_id)
            }
            Condition::Filter(_) => unreachable!(),
            Condition::IsEmpty(IsEmptyCondition { is_empty: field }) => {
                match payload.get_value(&field.key) {
                    None => true,
                    Some(value) => match value {
                        Value::Null => true,
                        Value::Array(array) => array.is_empty(),
                        _ => false,
                    },
                }
            }
        }
    };

    check_filter(&checker, query)
}

pub struct SimpleConditionChecker {
    payload_storage: Arc<AtomicRefCell<SimplePayloadStorage>>,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    empty_payload: Payload,
}

impl SimpleConditionChecker {
    pub fn new(
        payload_storage: Arc<AtomicRefCell<SimplePayloadStorage>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    ) -> Self {
        SimpleConditionChecker {
            payload_storage,
            id_tracker,
            empty_payload: Default::default(),
        }
    }
}

impl ConditionChecker for SimpleConditionChecker {
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let payload_storage_guard = self.payload_storage.borrow();
        let payload_ptr = payload_storage_guard.payload_ptr(point_id);

        let payload = match payload_ptr {
            None => &self.empty_payload,
            Some(x) => x,
        };

        check_payload(payload, self.id_tracker.borrow().deref(), query, point_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use tempdir::TempDir;

    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
    use crate::id_tracker::IdTracker;
    use crate::payload_storage::PayloadStorage;
    use crate::types::{FieldCondition, GeoBoundingBox, Range};
    use crate::types::{GeoPoint, PayloadField};

    use super::*;

    #[test]
    fn test_condition_checker() {
        let dir = TempDir::new("payload_dir").unwrap();
        let dir_id_tracker = TempDir::new("id_tracker_dir").unwrap();

        let payload: Payload = json!(
            {
                "location":{
                    "lon": 13.404954,
                    "lat": 52.520008,
            },
            "price": 499.90,
            "amount": 10,
            "rating": vec![3, 7, 9, 9],
            "color": "red",
            "has_delivery": true,
        })
        .into();

        let mut payload_storage = SimplePayloadStorage::open(dir.path()).unwrap();
        let mut id_tracker = SimpleIdTracker::open(dir_id_tracker.path()).unwrap();

        id_tracker.set_link(0.into(), 0).unwrap();
        id_tracker.set_link(1.into(), 1).unwrap();
        id_tracker.set_link(2.into(), 2).unwrap();
        id_tracker.set_link(10.into(), 10).unwrap();
        payload_storage.assign_all(0, &payload).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(id_tracker)),
        );

        let is_empty_condition_1 = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "price".to_string(),
            },
        }));

        let is_empty_condition_2 = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "something_new".to_string(),
            },
        }));

        assert!(!payload_checker.check(0, &is_empty_condition_1));
        assert!(payload_checker.check(0, &is_empty_condition_2));

        let match_red = Condition::Field(FieldCondition::new_match(
            "color".to_string(),
            "red".to_owned().into(),
        ));
        let match_blue = Condition::Field(FieldCondition::new_match(
            "color".to_string(),
            "blue".to_owned().into(),
        ));
        let with_delivery = Condition::Field(FieldCondition::new_match(
            "has_delivery".to_string(),
            true.into(),
        ));

        let in_berlin = Condition::Field(FieldCondition::new_geo_bounding_box(
            "location".to_string(),
            GeoBoundingBox {
                top_left: GeoPoint {
                    lon: 13.08835,
                    lat: 52.67551,
                },
                bottom_right: GeoPoint {
                    lon: 13.76116,
                    lat: 52.33826,
                },
            },
        ));

        let in_moscow = Condition::Field(FieldCondition::new_geo_bounding_box(
            "location".to_string(),
            GeoBoundingBox {
                top_left: GeoPoint {
                    lon: 37.0366,
                    lat: 56.1859,
                },
                bottom_right: GeoPoint {
                    lon: 38.2532,
                    lat: 55.317,
                },
            },
        ));

        let with_bad_rating = Condition::Field(FieldCondition::new_range(
            "rating".to_string(),
            Range {
                lt: None,
                gt: None,
                gte: None,
                lte: Some(5.),
            },
        ));

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
