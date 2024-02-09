use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::types::PointOffsetType;

use crate::common::utils::IndexesMap;
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::FieldIndex;
use crate::payload_storage::condition_checker::ValueChecker;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::ConditionChecker;
use crate::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, OwnedPayloadRef, Payload,
    PayloadContainer, PayloadKeyType,
};

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

pub fn select_nested_indexes<'a, R>(
    nested_path: &str,
    field_indexes: &'a HashMap<PayloadKeyType, R>,
) -> HashMap<PayloadKeyType, &'a Vec<FieldIndex>>
where
    R: AsRef<Vec<FieldIndex>>,
{
    let nested_prefix = format!("{}.", nested_path);
    let nested_indexes: HashMap<_, _> = field_indexes
        .iter()
        .filter_map(|(key, indexes)| {
            key.strip_prefix(&nested_prefix)
                .map(|key| (key.into(), indexes.as_ref()))
        })
        .collect();
    nested_indexes
}

pub fn check_payload<'a, R>(
    get_payload: Box<dyn Fn() -> OwnedPayloadRef<'a> + 'a>,
    id_tracker: Option<&IdTrackerSS>,
    query: &Filter,
    point_id: PointOffsetType,
    field_indexes: &HashMap<PayloadKeyType, R>,
) -> bool
where
    R: AsRef<Vec<FieldIndex>>,
{
    let checker = |condition: &Condition| match condition {
        Condition::Field(field_condition) => {
            check_field_condition(field_condition, get_payload().deref(), field_indexes)
        }
        Condition::IsEmpty(is_empty) => check_is_empty_condition(is_empty, get_payload().deref()),
        Condition::IsNull(is_null) => check_is_null_condition(is_null, get_payload().deref()),
        Condition::HasId(has_id) => id_tracker
            .and_then(|id_tracker| id_tracker.external_id(point_id))
            .map_or(false, |id| has_id.has_id.contains(&id)),
        Condition::Nested(nested) => {
            let nested_path = nested.array_key();
            let nested_indexes = select_nested_indexes(&nested_path, field_indexes);
            get_payload()
                .get_value(&nested_path)
                .values()
                .iter()
                .filter_map(|value| value.as_object())
                .any(|object| {
                    check_payload(
                        Box::new(|| OwnedPayloadRef::from(object)),
                        None,
                        &nested.nested.filter,
                        point_id,
                        &nested_indexes,
                    )
                })
        }
        Condition::Filter(_) => unreachable!(),
    };

    check_filter(&checker, query)
}

pub fn check_is_empty_condition(
    is_empty: &IsEmptyCondition,
    payload: &impl PayloadContainer,
) -> bool {
    payload.get_value(&is_empty.is_empty.key).check_is_empty()
}

pub fn check_is_null_condition(is_null: &IsNullCondition, payload: &impl PayloadContainer) -> bool {
    payload.get_value(&is_null.is_null.key).check_is_null()
}

pub fn check_field_condition<R>(
    field_condition: &FieldCondition,
    payload: &impl PayloadContainer,
    field_indexes: &HashMap<PayloadKeyType, R>,
) -> bool
where
    R: AsRef<Vec<FieldIndex>>,
{
    let field_values = payload.get_value(&field_condition.key);
    let field_indexes = field_indexes.get(&field_condition.key);

    // This covers a case, when a field index affects the result of the condition.
    if let Some(field_indexes) = field_indexes {
        for p in field_values {
            let mut index_checked = false;
            for index in field_indexes.as_ref() {
                if let Some(index_check_res) = index.check_condition(field_condition, p) {
                    if index_check_res {
                        // If at least one object matches the condition, we can return true
                        return true;
                    }
                    index_checked = true;
                    // If index check of the condition returned something, we don't need to check
                    // other indexes
                    break;
                }
            }
            if !index_checked {
                // If none of the indexes returned anything, we need to check the condition
                // against the payload
                if field_condition.check(p) {
                    return true;
                }
            }
        }
        false
    } else {
        // Fallback to regular condition check if there are no indexes for the field
        field_values.into_iter().any(|p| field_condition.check(p))
    }
}

/// Only used for testing
pub struct SimpleConditionChecker {
    payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    empty_payload: Payload,
}

impl SimpleConditionChecker {
    pub fn new(
        payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
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

        let payload_ref_cell: RefCell<Option<OwnedPayloadRef>> = RefCell::new(None);
        let id_tracker = self.id_tracker.borrow();

        check_payload(
            Box::new(|| {
                if payload_ref_cell.borrow().is_none() {
                    let payload_ptr = match payload_storage_guard.deref() {
                        PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                            s.payload_ptr(point_id).map(|x| x.into())
                        }
                        PayloadStorageEnum::SimplePayloadStorage(s) => {
                            s.payload_ptr(point_id).map(|x| x.into())
                        }
                        PayloadStorageEnum::OnDiskPayloadStorage(s) => {
                            // Warn: Possible panic here
                            // Currently, it is possible that `read_payload` fails with Err,
                            // but it seems like a very rare possibility which might only happen
                            // if something is wrong with disk or storage is corrupted.
                            //
                            // In both cases it means that service can't be of use any longer.
                            // It is as good as dead. Therefore it is tolerable to just panic here.
                            // Downside is - API user won't be notified of the failure.
                            // It will just timeout.
                            //
                            // The alternative:
                            // Rewrite condition checking code to support error reporting.
                            // Which may lead to slowdown and assumes a lot of changes.
                            s.read_payload(point_id)
                                .unwrap_or_else(|err| panic!("Payload storage is corrupted: {err}"))
                                .map(|x| x.into())
                        }
                    };

                    payload_ref_cell
                        .replace(payload_ptr.or_else(|| Some((&self.empty_payload).into())));
                }
                payload_ref_cell.borrow().as_ref().cloned().unwrap()
            }),
            Some(id_tracker.deref()),
            query,
            point_id,
            &IndexesMap::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::str::FromStr;

    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::id_tracker::simple_id_tracker::SimpleIdTracker;
    use crate::id_tracker::IdTracker;
    use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
    use crate::payload_storage::PayloadStorage;
    use crate::types::{
        DateTimeWrapper, FieldCondition, GeoBoundingBox, GeoPoint, PayloadField, Range, ValuesCount,
    };

    #[test]
    fn test_condition_checker() {
        let dir = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

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
            "shipped_at": "2020-02-15T00:00:00Z",
            "parts": [],
            "packaging": null,
            "not_null": [null]
        })
        .into();

        let mut payload_storage: PayloadStorageEnum =
            SimplePayloadStorage::open(db.clone()).unwrap().into();
        let mut id_tracker = SimpleIdTracker::open(db).unwrap();

        id_tracker.set_link(0.into(), 0).unwrap();
        id_tracker.set_link(1.into(), 1).unwrap();
        id_tracker.set_link(2.into(), 2).unwrap();
        id_tracker.set_link(10.into(), 10).unwrap();
        payload_storage.assign_all(0, &payload).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(id_tracker)),
        );

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "price".to_string(),
            },
        }));
        assert!(!payload_checker.check(0, &is_empty_condition));

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "something_new".to_string(),
            },
        }));
        assert!(payload_checker.check(0, &is_empty_condition));

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "parts".to_string(),
            },
        }));
        assert!(payload_checker.check(0, &is_empty_condition));

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: "not_null".to_string(),
            },
        }));
        assert!(!payload_checker.check(0, &is_empty_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: "amount".to_string(),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: "parts".to_string(),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: "something_else".to_string(),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: "packaging".to_string(),
            },
        }));
        assert!(payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: "not_null".to_string(),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let match_red = Condition::Field(FieldCondition::new_match(
            "color".to_string(),
            "red".to_owned().into(),
        ));
        let match_blue = Condition::Field(FieldCondition::new_match(
            "color".to_string(),
            "blue".to_owned().into(),
        ));
        let shipped_in_february = Condition::Field(FieldCondition::new_datetime_range(
            "shipped_at".to_string(),
            Range {
                lt: Some(DateTimeWrapper::from_str("2020-03-01T00:00:00Z").unwrap()),
                gt: None,
                gte: Some(DateTimeWrapper::from_str("2020-02-01T00:00:00Z").unwrap()),
                lte: None,
            },
        ));
        let shipped_in_march = Condition::Field(FieldCondition::new_datetime_range(
            "shipped_at".to_string(),
            Range {
                lt: Some(DateTimeWrapper::from_str("2020-04-01T00:00:00Z").unwrap()),
                gt: None,
                gte: Some(DateTimeWrapper::from_str("2020-03-01T00:00:00Z").unwrap()),
                lte: None,
            },
        ));
        let with_delivery = Condition::Field(FieldCondition::new_match(
            "has_delivery".to_string(),
            true.into(),
        ));

        let many_value_count_condition =
            Filter::new_must(Condition::Field(FieldCondition::new_values_count(
                "rating".to_string(),
                ValuesCount {
                    lt: None,
                    gt: None,
                    gte: Some(10),
                    lte: None,
                },
            )));
        assert!(!payload_checker.check(0, &many_value_count_condition));

        let few_value_count_condition =
            Filter::new_must(Condition::Field(FieldCondition::new_values_count(
                "rating".to_string(),
                ValuesCount {
                    lt: Some(5),
                    gt: None,
                    gte: None,
                    lte: None,
                },
            )));
        assert!(payload_checker.check(0, &few_value_count_condition));

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
            must: Some(vec![shipped_in_february]),
            must_not: None,
        };
        assert!(payload_checker.check(0, &query));
        let query = Filter {
            should: None,
            must: Some(vec![shipped_in_march]),
            must_not: None,
        };
        assert!(!payload_checker.check(0, &query));

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
