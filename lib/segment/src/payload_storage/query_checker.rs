#![cfg_attr(not(feature = "testing"), allow(unused_imports))]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::utils::{IndexesMap, check_is_empty, check_is_null};
use crate::id_tracker::IdTrackerSS;
use crate::index::field_index::FieldIndex;
use crate::payload_storage::condition_checker::ValueChecker;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::payload_storage::{ConditionChecker, PayloadStorage};
use crate::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, MinShould,
    OwnedPayloadRef, Payload, PayloadContainer, PayloadKeyType, VectorNameBuf,
};
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

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
        && check_min_should(checker, &filter.min_should)
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

fn check_min_should<F>(checker: &F, min_should: &Option<MinShould>) -> bool
where
    F: Fn(&Condition) -> bool,
{
    let check = |x| check_condition(checker, x);
    match min_should {
        None => true,
        Some(MinShould {
            conditions,
            min_count,
        }) => {
            conditions
                .iter()
                .filter(|cond| check(cond))
                .take(*min_count)
                .count()
                == *min_count
        }
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
    nested_path: &PayloadKeyType,
    field_indexes: &'a HashMap<PayloadKeyType, R>,
) -> HashMap<PayloadKeyType, &'a Vec<FieldIndex>>
where
    R: AsRef<Vec<FieldIndex>>,
{
    let nested_indexes: HashMap<_, _> = field_indexes
        .iter()
        .filter_map(|(key, indexes)| {
            key.strip_prefix(nested_path)
                .map(|key| (key, indexes.as_ref()))
        })
        .collect();
    nested_indexes
}

pub fn check_payload<'a, R>(
    get_payload: Box<dyn Fn() -> OwnedPayloadRef<'a> + 'a>,
    id_tracker: Option<&IdTrackerSS>,
    vector_storages: &HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    query: &Filter,
    point_id: PointOffsetType,
    field_indexes: &HashMap<PayloadKeyType, R>,
    hw_counter: &HardwareCounterCell,
) -> bool
where
    R: AsRef<Vec<FieldIndex>>,
{
    let checker = |condition: &Condition| match condition {
        Condition::Field(field_condition) => check_field_condition(
            field_condition,
            get_payload().deref(),
            field_indexes,
            hw_counter,
        ),
        Condition::IsEmpty(is_empty) => check_is_empty_condition(is_empty, get_payload().deref()),
        Condition::IsNull(is_null) => check_is_null_condition(is_null, get_payload().deref()),
        Condition::HasId(has_id) => id_tracker
            .and_then(|id_tracker| id_tracker.external_id(point_id))
            .is_some_and(|id| has_id.has_id.contains(&id)),
        Condition::HasVector(has_vector) => {
            if let Some(vector_storage) = vector_storages.get(&has_vector.has_vector) {
                !vector_storage.borrow().is_deleted_vector(point_id)
            } else {
                false
            }
        }
        Condition::Nested(nested) => {
            let nested_path = nested.array_key();
            let nested_indexes = select_nested_indexes(&nested_path, field_indexes);
            get_payload()
                .get_value(&nested_path)
                .iter()
                .filter_map(|value| value.as_object())
                .any(|object| {
                    check_payload(
                        Box::new(|| OwnedPayloadRef::from(object)),
                        None,            // HasId check in nested fields is not supported
                        &HashMap::new(), // HasVector check in nested fields is not supported
                        &nested.nested.filter,
                        point_id,
                        &nested_indexes,
                        hw_counter,
                    )
                })
        }

        Condition::CustomIdChecker(cond) => id_tracker
            .and_then(|id_tracker| id_tracker.external_id(point_id))
            .is_some_and(|point_id| cond.0.check(point_id)),

        Condition::Filter(_) => unreachable!(),
    };

    check_filter(&checker, query)
}

pub fn check_is_empty_condition(
    is_empty: &IsEmptyCondition,
    payload: &impl PayloadContainer,
) -> bool {
    check_is_empty(payload.get_value(&is_empty.is_empty.key).iter().copied())
}

pub fn check_is_null_condition(is_null: &IsNullCondition, payload: &impl PayloadContainer) -> bool {
    check_is_null(payload.get_value(&is_null.is_null.key).iter().copied())
}

pub fn check_field_condition<R>(
    field_condition: &FieldCondition,
    payload: &impl PayloadContainer,
    field_indexes: &HashMap<PayloadKeyType, R>,
    hw_counter: &HardwareCounterCell,
) -> bool
where
    R: AsRef<Vec<FieldIndex>>,
{
    let field_values = payload.get_value(&field_condition.key);
    let field_indexes = field_indexes.get(&field_condition.key);

    if field_values.is_empty() {
        return field_condition.check_empty();
    }

    // This covers a case, when a field index affects the result of the condition.
    if let Some(field_indexes) = field_indexes {
        for p in field_values {
            let mut index_checked = false;
            for index in field_indexes.as_ref() {
                if let Some(index_check_res) =
                    index.special_check_condition(field_condition, p, hw_counter)
                {
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
#[cfg(feature = "testing")]
pub struct SimpleConditionChecker {
    payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    empty_payload: Payload,
}

#[cfg(feature = "testing")]
impl SimpleConditionChecker {
    pub fn new(
        payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
        vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    ) -> Self {
        SimpleConditionChecker {
            payload_storage,
            id_tracker,
            vector_storages,
            empty_payload: Default::default(),
        }
    }
}

#[cfg(feature = "testing")]
impl ConditionChecker for SimpleConditionChecker {
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let hw_counter = HardwareCounterCell::new(); // No measurements needed as this is only for test!

        let payload_storage_guard = self.payload_storage.borrow();

        let payload_ref_cell: RefCell<Option<OwnedPayloadRef>> = RefCell::new(None);
        let id_tracker = self.id_tracker.borrow();

        let vector_storages = &self.vector_storages;

        check_payload(
            Box::new(|| {
                if payload_ref_cell.borrow().is_none() {
                    let payload_ptr = match payload_storage_guard.deref() {
                        PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                            s.payload_ptr(point_id).map(|x| x.into())
                        }
                        #[cfg(feature = "rocksdb")]
                        PayloadStorageEnum::SimplePayloadStorage(s) => {
                            s.payload_ptr(point_id).map(|x| x.into())
                        }
                        #[cfg(feature = "rocksdb")]
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
                            s.read_payload(point_id, &hw_counter)
                                .unwrap_or_else(|err| panic!("Payload storage is corrupted: {err}"))
                                .map(|x| x.into())
                        }
                        PayloadStorageEnum::MmapPayloadStorage(s) => {
                            let payload = s.get(point_id, &hw_counter).unwrap_or_else(|err| {
                                panic!("Payload storage is corrupted: {err}")
                            });
                            Some(OwnedPayloadRef::from(payload))
                        }
                    };

                    payload_ref_cell
                        .replace(payload_ptr.or_else(|| Some((&self.empty_payload).into())));
                }
                payload_ref_cell.borrow().as_ref().cloned().unwrap()
            }),
            Some(id_tracker.deref()),
            vector_storages,
            query,
            point_id,
            &IndexesMap::new(),
            &HardwareCounterCell::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ahash::AHashSet;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::id_tracker::IdTracker;
    use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
    use crate::json_path::JsonPath;
    use crate::payload_json;
    use crate::payload_storage::PayloadStorage;
    use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
    use crate::types::{
        DateTimeWrapper, FieldCondition, GeoBoundingBox, GeoPoint, PayloadField, Range, ValuesCount,
    };

    #[test]
    fn test_condition_checker() {
        let payload = payload_json! {
            "location": {
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
            "not_null": [null],
        };

        let hw_counter = HardwareCounterCell::new();

        let mut payload_storage: PayloadStorageEnum =
            PayloadStorageEnum::InMemoryPayloadStorage(InMemoryPayloadStorage::default());
        let mut id_tracker = InMemoryIdTracker::new();

        id_tracker.set_link(0.into(), 0).unwrap();
        id_tracker.set_link(1.into(), 1).unwrap();
        id_tracker.set_link(2.into(), 2).unwrap();
        id_tracker.set_link(10.into(), 10).unwrap();
        payload_storage.overwrite(0, &payload, &hw_counter).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(id_tracker)),
            HashMap::new(),
        );

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: JsonPath::new("price"),
            },
        }));
        assert!(!payload_checker.check(0, &is_empty_condition));

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: JsonPath::new("something_new"),
            },
        }));
        assert!(payload_checker.check(0, &is_empty_condition));

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: JsonPath::new("parts"),
            },
        }));
        assert!(payload_checker.check(0, &is_empty_condition));

        let is_empty_condition = Filter::new_must(Condition::IsEmpty(IsEmptyCondition {
            is_empty: PayloadField {
                key: JsonPath::new("not_null"),
            },
        }));
        assert!(!payload_checker.check(0, &is_empty_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::new("amount"),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::new("parts"),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::new("something_else"),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::new("packaging"),
            },
        }));
        assert!(payload_checker.check(0, &is_null_condition));

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::new("not_null"),
            },
        }));
        assert!(!payload_checker.check(0, &is_null_condition));

        let match_red = Condition::Field(FieldCondition::new_match(
            JsonPath::new("color"),
            "red".to_owned().into(),
        ));
        let match_blue = Condition::Field(FieldCondition::new_match(
            JsonPath::new("color"),
            "blue".to_owned().into(),
        ));
        let shipped_in_february = Condition::Field(FieldCondition::new_datetime_range(
            JsonPath::new("shipped_at"),
            Range {
                lt: Some(DateTimeWrapper::from_str("2020-03-01T00:00:00Z").unwrap()),
                gt: None,
                gte: Some(DateTimeWrapper::from_str("2020-02-01T00:00:00Z").unwrap()),
                lte: None,
            },
        ));
        let shipped_in_march = Condition::Field(FieldCondition::new_datetime_range(
            JsonPath::new("shipped_at"),
            Range {
                lt: Some(DateTimeWrapper::from_str("2020-04-01T00:00:00Z").unwrap()),
                gt: None,
                gte: Some(DateTimeWrapper::from_str("2020-03-01T00:00:00Z").unwrap()),
                lte: None,
            },
        ));
        let with_delivery = Condition::Field(FieldCondition::new_match(
            JsonPath::new("has_delivery"),
            true.into(),
        ));

        let many_value_count_condition =
            Filter::new_must(Condition::Field(FieldCondition::new_values_count(
                JsonPath::new("rating"),
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
                JsonPath::new("rating"),
                ValuesCount {
                    lt: Some(5),
                    gt: None,
                    gte: None,
                    lte: None,
                },
            )));
        assert!(payload_checker.check(0, &few_value_count_condition));

        let in_berlin = Condition::Field(FieldCondition::new_geo_bounding_box(
            JsonPath::new("location"),
            GeoBoundingBox {
                top_left: GeoPoint::new_unchecked(13.08835, 52.67551),
                bottom_right: GeoPoint::new_unchecked(13.76116, 52.33826),
            },
        ));

        let in_moscow = Condition::Field(FieldCondition::new_geo_bounding_box(
            JsonPath::new("location"),
            GeoBoundingBox {
                top_left: GeoPoint::new_unchecked(37.0366, 56.1859),
                bottom_right: GeoPoint::new_unchecked(38.2532, 55.317),
            },
        ));

        let with_bad_rating = Condition::Field(FieldCondition::new_range(
            JsonPath::new("rating"),
            Range {
                lt: None,
                gt: None,
                gte: None,
                lte: Some(OrderedFloat(5.)),
            },
        ));

        let query = Filter::new_must(match_red.clone());
        assert!(payload_checker.check(0, &query));

        let query = Filter::new_must(match_blue.clone());
        assert!(!payload_checker.check(0, &query));

        let query = Filter::new_must_not(match_blue.clone());
        assert!(payload_checker.check(0, &query));

        let query = Filter::new_must_not(match_red.clone());
        assert!(!payload_checker.check(0, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            min_should: None,
            must: Some(vec![with_delivery.clone(), in_berlin.clone()]),
            must_not: None,
        };
        assert!(payload_checker.check(0, &query));

        let query = Filter {
            should: Some(vec![match_red.clone(), match_blue.clone()]),
            min_should: None,
            must: Some(vec![with_delivery, in_moscow.clone()]),
            must_not: None,
        };
        assert!(!payload_checker.check(0, &query));

        let query = Filter {
            should: Some(vec![
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![match_red.clone(), in_moscow.clone()]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![match_blue.clone(), in_berlin.clone()]),
                    must_not: None,
                }),
            ]),
            min_should: None,
            must: None,
            must_not: None,
        };
        assert!(!payload_checker.check(0, &query));

        let query = Filter {
            should: Some(vec![
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![match_blue.clone(), in_moscow.clone()]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![match_red.clone(), in_berlin.clone()]),
                    must_not: None,
                }),
            ]),
            min_should: None,
            must: None,
            must_not: None,
        };
        assert!(payload_checker.check(0, &query));

        let query = Filter::new_must_not(with_bad_rating);
        assert!(!payload_checker.check(0, &query));

        // min_should
        let query = Filter::new_min_should(MinShould {
            conditions: vec![match_blue.clone(), in_moscow.clone()],
            min_count: 1,
        });
        assert!(!payload_checker.check(0, &query));

        let query = Filter::new_min_should(MinShould {
            conditions: vec![match_red.clone(), in_berlin.clone(), in_moscow.clone()],
            min_count: 2,
        });
        assert!(payload_checker.check(0, &query));

        let query = Filter::new_min_should(MinShould {
            conditions: vec![
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![match_blue, in_moscow]),
                    must_not: None,
                }),
                Condition::Filter(Filter {
                    should: None,
                    min_should: None,
                    must: Some(vec![match_red, in_berlin]),
                    must_not: None,
                }),
            ],
            min_count: 1,
        });
        assert!(payload_checker.check(0, &query));

        // DateTime payload index
        let query = Filter::new_must(shipped_in_february);
        assert!(payload_checker.check(0, &query));

        let query = Filter::new_must(shipped_in_march);
        assert!(!payload_checker.check(0, &query));

        // id Filter
        let ids: AHashSet<_> = vec![1, 2, 3].into_iter().map(|x| x.into()).collect();

        let query = Filter::new_must_not(Condition::HasId(ids.into()));
        assert!(!payload_checker.check(2, &query));

        let ids: AHashSet<_> = vec![1, 2, 3].into_iter().map(|x| x.into()).collect();

        let query = Filter::new_must_not(Condition::HasId(ids.into()));
        assert!(payload_checker.check(10, &query));

        let ids: AHashSet<_> = vec![1, 2, 3].into_iter().map(|x| x.into()).collect();

        let query = Filter::new_must(Condition::HasId(ids.into()));
        assert!(payload_checker.check(2, &query));
    }
}
