#![cfg_attr(not(feature = "testing"), allow(unused_imports))]
// Deprecated storage placement params (`on_disk`, `always_ram`, `on_disk_payload`) are still
// handled here for backward compatibility with the new `memory` parameter
#![allow(deprecated)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use ahash::AHashMap;
use atomic_refcell::AtomicRefCell;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::{IndexesMap, check_is_empty, check_is_null};
use crate::id_tracker::{IdTrackerEnum, IdTrackerRead};
use crate::index::field_index::FieldIndexRead;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::condition_checker::ValueChecker;
use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{
    Condition, FieldCondition, Filter, IsEmptyCondition, IsNullCondition, MinShould,
    OwnedPayloadRef, Payload, PayloadContainer, PayloadKeyType, VectorNameBuf,
};
use crate::vector_storage::{VectorStorageEnum, VectorStorageRead};

fn try_check_condition<F>(checker: &F, condition: &Condition) -> OperationResult<bool>
where
    F: Fn(&Condition) -> OperationResult<bool>,
{
    match condition {
        Condition::Filter(filter) => try_check_filter(checker, filter),
        Condition::Field(_)
        | Condition::IsEmpty(_)
        | Condition::IsNull(_)
        | Condition::HasId(_)
        | Condition::HasVector(_)
        | Condition::Slice(_)
        | Condition::Nested(_)
        | Condition::CustomIdChecker(_) => checker(condition),
    }
}

fn try_check_filter<F>(checker: &F, filter: &Filter) -> OperationResult<bool>
where
    F: Fn(&Condition) -> OperationResult<bool>,
{
    Ok(try_check_should(checker, &filter.should)?
        && try_check_min_should(checker, &filter.min_should)?
        && try_check_must(checker, &filter.must)?
        && try_check_must_not(checker, &filter.must_not)?)
}

pub fn check_filter<F>(checker: &F, filter: &Filter) -> bool
where
    F: Fn(&Condition) -> bool,
{
    try_check_filter(
        &|condition| Ok::<bool, OperationError>(checker(condition)),
        filter,
    )
    .expect("infallible condition checker")
}

fn try_check_should<F>(checker: &F, should: &Option<Vec<Condition>>) -> OperationResult<bool>
where
    F: Fn(&Condition) -> OperationResult<bool>,
{
    match should {
        None => Ok(true),
        Some(conditions) => {
            for condition in conditions {
                if try_check_condition(checker, condition)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn try_check_min_should<F>(checker: &F, min_should: &Option<MinShould>) -> OperationResult<bool>
where
    F: Fn(&Condition) -> OperationResult<bool>,
{
    match min_should {
        None => Ok(true),
        Some(MinShould {
            conditions,
            min_count,
        }) => {
            let mut matched = 0;
            for condition in conditions {
                if try_check_condition(checker, condition)? {
                    matched += 1;
                    if matched == *min_count {
                        return Ok(true);
                    }
                }
            }
            Ok(matched == *min_count)
        }
    }
}

fn try_check_must<F>(checker: &F, must: &Option<Vec<Condition>>) -> OperationResult<bool>
where
    F: Fn(&Condition) -> OperationResult<bool>,
{
    match must {
        None => Ok(true),
        Some(conditions) => {
            for condition in conditions {
                if !try_check_condition(checker, condition)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn try_check_must_not<F>(checker: &F, must_not: &Option<Vec<Condition>>) -> OperationResult<bool>
where
    F: Fn(&Condition) -> OperationResult<bool>,
{
    match must_not {
        None => Ok(true),
        Some(conditions) => {
            for condition in conditions {
                if try_check_condition(checker, condition)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}


pub fn select_nested_indexes<'a, R, FI>(
    nested_path: &PayloadKeyType,
    field_indexes: &'a AHashMap<PayloadKeyType, R>,
) -> AHashMap<PayloadKeyType, &'a Vec<FI>>
where
    FI: FieldIndexRead,
    R: AsRef<Vec<FI>>,
{
    let nested_indexes: AHashMap<_, _> = field_indexes
        .iter()
        .filter_map(|(key, indexes)| {
            key.strip_prefix(nested_path)
                .map(|key| (key, indexes.as_ref()))
        })
        .collect();
    nested_indexes
}

pub fn check_payload<'a, R, FI>(
    get_payload: Box<dyn Fn() -> OwnedPayloadRef<'a> + 'a>,
    id_tracker: Option<&IdTrackerEnum>,
    vector_storages: &HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    query: &Filter,
    point_id: PointOffsetType,
    field_indexes: &AHashMap<PayloadKeyType, R>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<bool>
where
    FI: FieldIndexRead,
    R: AsRef<Vec<FI>>,
{
    let checker = |condition: &Condition| -> OperationResult<bool> {
        Ok(match condition {
            Condition::Field(field_condition) => {
                // Propagate index read errors instead of panicking: a failing
                // on-disk index must turn the query into an error, not crash
                // the shard thread.
                return check_field_condition(
                    field_condition,
                    get_payload().deref(),
                    field_indexes,
                    hw_counter,
                );
            }
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
                let mut matched = false;
                for value in get_payload().get_value(&nested_path) {
                    let Some(object) = value.as_object() else {
                        continue;
                    };
                    if check_payload(
                        Box::new(|| OwnedPayloadRef::from(object)),
                        None,            // HasId check in nested fields is not supported
                        &HashMap::new(), // HasVector check in nested fields is not supported
                        &nested.nested.filter,
                        point_id,
                        &nested_indexes,
                        hw_counter,
                    )? {
                        matched = true;
                        break;
                    }
                }
                matched
            }

            Condition::Slice(slice_condition) => id_tracker
                .and_then(|id_tracker| id_tracker.external_id(point_id))
                .is_some_and(|external_id| slice_condition.slice.check(external_id)),

            Condition::CustomIdChecker(cond) => id_tracker
                .and_then(|id_tracker| id_tracker.external_id(point_id))
                .is_some_and(|point_id| cond.0.check(point_id)),

            Condition::Filter(_) => unreachable!(),
        })
    };

    try_check_filter(&checker, query)
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

pub fn check_field_condition<R, FI>(
    field_condition: &FieldCondition,
    payload: &impl PayloadContainer,
    field_indexes: &AHashMap<PayloadKeyType, R>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<bool>
where
    FI: FieldIndexRead,
    R: AsRef<Vec<FI>>,
{
    let field_values = payload.get_value(&field_condition.key);
    let field_indexes = field_indexes.get(&field_condition.key);

    if field_values.is_empty() {
        return Ok(field_condition.check_empty());
    }

    // This covers a case, when a field index affects the result of the condition.
    if let Some(field_indexes) = field_indexes {
        for p in field_values {
            let mut index_checked = false;
            for index in field_indexes.as_ref() {
                if let Some(index_check_res) =
                    index.special_check_condition(field_condition, p, hw_counter)?
                {
                    if index_check_res {
                        // If at least one object matches the condition, we can return true
                        return Ok(true);
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
                    return Ok(true);
                }
            }
        }
        Ok(false)
    } else {
        // Fallback to regular condition check if there are no indexes for the field
        Ok(field_values.into_iter().any(|p| field_condition.check(p)))
    }
}

/// Only used for testing
#[cfg(feature = "testing")]
pub struct SimpleConditionChecker {
    payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
    id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
    vector_storages: HashMap<VectorNameBuf, Arc<AtomicRefCell<VectorStorageEnum>>>,
    empty_payload: Payload,
}

#[cfg(feature = "testing")]
impl SimpleConditionChecker {
    pub fn new(
        payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerEnum>>,
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
impl SimpleConditionChecker {
    pub fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let hw_counter = HardwareCounterCell::new(); // No measurements needed as this is only for test!

        let payload_storage_guard = self.payload_storage.borrow();

        let payload_ref_cell: RefCell<Option<OwnedPayloadRef>> = RefCell::new(None);
        let id_tracker = self.id_tracker.borrow();

        let vector_storages = &self.vector_storages;

        check_payload(
            Box::new(|| {
                if payload_ref_cell.borrow().is_none() {
                    let payload_ptr = match payload_storage_guard.deref() {
                        PayloadStorageEnum::InMemory(s) => s.payload_ptr(point_id).map(Into::into),
                        PayloadStorageEnum::Mmap(s) => {
                            let payload = s.get(point_id, &hw_counter).unwrap_or_else(|err| {
                                panic!("Payload storage is corrupted: {err}")
                            });
                            Some(OwnedPayloadRef::from(payload))
                        }
                        #[cfg(target_os = "linux")]
                        PayloadStorageEnum::IoUring(s) => {
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
        .expect("payload check failed in SimpleConditionChecker")
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ahash::AHashSet;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
    use crate::id_tracker::{IdTracker, IdTrackerEnum};
    use crate::index::field_index::FieldIndex;
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
            "not_null": [true],
            "null_array": [null, 1],
        };

        let hw_counter = HardwareCounterCell::new();

        let mut payload_storage: PayloadStorageEnum =
            PayloadStorageEnum::InMemory(InMemoryPayloadStorage::default());
        let mut id_tracker = InMemoryIdTracker::new();

        id_tracker.set_link(0.into(), 0).unwrap();
        id_tracker.set_link(1.into(), 1).unwrap();
        id_tracker.set_link(2.into(), 2).unwrap();
        id_tracker.set_link(10.into(), 10).unwrap();
        payload_storage.overwrite(0, &payload, &hw_counter).unwrap();

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(IdTrackerEnum::InMemoryIdTracker(
                id_tracker,
            ))),
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

        let is_null_condition = Filter::new_must(Condition::IsNull(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::new("null_array"),
            },
        }));
        assert!(payload_checker.check(0, &is_null_condition));

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
        let ids: AHashSet<_> = vec![1, 2, 3].into_iter().map(u64::into).collect();

        let query = Filter::new_must_not(Condition::HasId(ids.into()));
        assert!(!payload_checker.check(2, &query));

        let ids: AHashSet<_> = vec![1, 2, 3].into_iter().map(u64::into).collect();

        let query = Filter::new_must_not(Condition::HasId(ids.into()));
        assert!(payload_checker.check(10, &query));

        let ids: AHashSet<_> = vec![1, 2, 3].into_iter().map(u64::into).collect();

        let query = Filter::new_must(Condition::HasId(ids.into()));
        assert!(payload_checker.check(2, &query));
    }

    #[test]
    fn test_slice_condition_checker() {
        use std::num::NonZeroU32;

        use uuid::Uuid;

        use crate::types::{PointIdType, Slice, SliceCondition};

        let payload_storage: PayloadStorageEnum =
            PayloadStorageEnum::InMemory(InMemoryPayloadStorage::default());
        let mut id_tracker = InMemoryIdTracker::new();

        let external_ids: Vec<PointIdType> = (0..100_u64)
            .map(PointIdType::NumId)
            .chain((0..100_u128).map(|seed| {
                PointIdType::Uuid(Uuid::from_u128(
                    seed.wrapping_mul(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210),
                ))
            }))
            .collect();
        for (offset, external_id) in external_ids.iter().enumerate() {
            id_tracker
                .set_link(*external_id, offset as PointOffsetType)
                .unwrap();
        }

        let payload_checker = SimpleConditionChecker::new(
            Arc::new(AtomicRefCell::new(payload_storage)),
            Arc::new(AtomicRefCell::new(IdTrackerEnum::InMemoryIdTracker(
                id_tracker,
            ))),
            HashMap::new(),
        );

        let total = NonZeroU32::new(5).unwrap();
        let slice_filter = |index| {
            Filter::new_must(Condition::Slice(SliceCondition {
                slice: Slice { total, index },
            }))
        };

        for offset in 0..external_ids.len() as PointOffsetType {
            // Each point matches exactly one of the disjoint slices
            let matching: Vec<u32> = (0..total.get())
                .filter(|&index| payload_checker.check(offset, &slice_filter(index)))
                .collect();
            assert_eq!(matching.len(), 1, "point {offset} matched {matching:?}");

            // must_not inverts membership
            let inverted = Filter::new_must_not(Condition::Slice(SliceCondition {
                slice: Slice {
                    total,
                    index: matching[0],
                },
            }));
            assert!(!payload_checker.check(offset, &inverted));
        }

        // On 200 uniformly hashed ids every slice gets some points
        for index in 0..total.get() {
            assert!(
                (0..external_ids.len() as PointOffsetType)
                    .any(|offset| payload_checker.check(offset, &slice_filter(index))),
            );
        }
    }

    /// Regression test for <https://github.com/qdrant/qdrant/issues/8936>
    ///
    /// Verifies that `MatchTextAny` inside a `NestedCondition` uses the
    /// full-text index tokenizer and does NOT fall back to substring matching.
    /// Before the fix, "good" would incorrectly match "goodness" in the
    /// nested path because `special_check_condition` didn't handle
    /// `Match::TextAny`.
    #[test]
    fn test_nested_match_text_any_uses_full_text_index() {
        use tempfile::Builder;

        use crate::data_types::index::{TextIndexParams, TextIndexType, TokenizerType};
        use crate::index::field_index::ValueIndexer;
        use crate::index::field_index::full_text_index::FullTextIndex;
        use crate::types::{Condition, MatchTextAny, Nested, NestedCondition};

        let hw_counter = HardwareCounterCell::new();

        // --- build payloads with nested objects ---
        // Point 0: nested title "goodness only" (should NOT match "good cheap")
        // Point 1: nested title "cheap hardware" (SHOULD match "good cheap")
        // Point 2: nested title "neutral text"  (should NOT match)
        let payloads = [
            payload_json! {
                "items": [{"title": "goodness only"}],
            },
            payload_json! {
                "items": [{"title": "cheap hardware"}],
            },
            payload_json! {
                "items": [{"title": "neutral text"}],
            },
        ];

        // --- build a full-text index for "items.title" ---
        let temp_dir = Builder::new()
            .prefix("test_nested_text_any")
            .tempdir()
            .unwrap();
        let config = TextIndexParams {
            memory: None,
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: None,
            stemmer: None,
            ascii_folding: None,
            enable_hnsw: None,
        };

        let mut ft_index =
            FullTextIndex::new_gridstore(temp_dir.path().to_path_buf(), config, true)
                .unwrap()
                .unwrap();

        // Index each point's nested title value
        let nested_titles = ["goodness only", "cheap hardware", "neutral text"];
        for (idx, title) in nested_titles.iter().enumerate() {
            ft_index
                .add_many(idx as u32, vec![title.to_string()], &hw_counter)
                .unwrap();
        }

        // The key must include the `[]` wildcard so that
        // `select_nested_indexes` can strip the `items[]` prefix and pass the
        // index under key `title` into the nested `check_payload`.
        let field_indexes: IndexesMap = AHashMap::from([(
            JsonPath::new("items[].title"),
            vec![FieldIndex::FullTextIndex(ft_index)],
        )]);

        // --- build the nested MatchTextAny filter ---
        let nested_filter = Filter::new_must(Condition::Nested(NestedCondition::new(Nested {
            key: JsonPath::new("items"),
            filter: Filter::new_must(Condition::Field(FieldCondition::new_match(
                JsonPath::new("title"),
                crate::types::Match::TextAny(MatchTextAny {
                    text_any: "good cheap".to_string(),
                }),
            ))),
        })));

        // --- run check_payload for each point ---
        let results: Vec<bool> = (0..3)
            .map(|point_id| {
                let payload = &payloads[point_id as usize];
                check_payload(
                    Box::new(|| payload.into()),
                    None,
                    &HashMap::new(),
                    &nested_filter,
                    point_id,
                    &field_indexes,
                    &hw_counter,
                )
                .unwrap()
            })
            .collect();

        // Point 0 ("goodness only"): must NOT match — "good" is not a token in "goodness"
        assert!(
            !results[0],
            "Point 0 ('goodness only') must not match text_any('good cheap') — \
             'good' is a substring of 'goodness' but not a whole token"
        );
        // Point 1 ("cheap hardware"): must match — "cheap" is an exact token
        assert!(
            results[1],
            "Point 1 ('cheap hardware') must match text_any('good cheap')"
        );
        // Point 2 ("neutral text"): must NOT match
        assert!(
            !results[2],
            "Point 2 ('neutral text') must not match text_any('good cheap')"
        );
    }

    /// Field index whose storage read always fails, simulating an IO error in
    /// an on-disk index.
    struct FailingFieldIndex;

    impl crate::index::field_index::PayloadFieldIndexRead for FailingFieldIndex {
        fn count_indexed_points(&self) -> OperationResult<usize> {
            unimplemented!()
        }

        fn filter<'a>(
            &'a self,
            _condition: &'a FieldCondition,
            _hw_counter: &'a HardwareCounterCell,
        ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
            unimplemented!()
        }

        fn estimate_cardinality(
            &self,
            _condition: &FieldCondition,
            _hw_counter: &HardwareCounterCell,
        ) -> OperationResult<Option<crate::index::field_index::CardinalityEstimation>> {
            unimplemented!()
        }

        fn for_each_payload_block(
            &self,
            _threshold: usize,
            _key: PayloadKeyType,
            _f: &mut dyn FnMut(
                crate::index::field_index::PayloadBlockCondition,
            ) -> OperationResult<()>,
        ) -> OperationResult<()> {
            unimplemented!()
        }

        fn condition_checker<'a>(
            &'a self,
            _condition: &FieldCondition,
            _hw_acc: common::counter::hardware_accumulator::HwMeasurementAcc,
        ) -> OperationResult<Option<crate::index::ConditionCheckerEnum<'a>>>
        {
            unimplemented!()
        }

        fn special_check_condition(
            &self,
            _condition: &FieldCondition,
            _payload_value: &serde_json::Value,
            _hw_counter: &HardwareCounterCell,
        ) -> OperationResult<Option<bool>> {
            Err(crate::common::operation_error::OperationError::service_error(
                "simulated index read failure",
            ))
        }
    }

    struct NoNumeric;

    impl crate::index::field_index::numeric_index::NumericFieldIndexRead
        for NoNumeric
    {
        fn get_ordering_values(
            &self,
            _idx: PointOffsetType,
        ) -> impl Iterator<Item = crate::data_types::order_by::OrderValue> + '_ {
            std::iter::empty()
        }

        fn stream_range(
            &self,
            _range: &crate::types::RangeInterface,
        ) -> OperationResult<
            impl DoubleEndedIterator<
                Item = (crate::data_types::order_by::OrderValue, PointOffsetType),
            > + '_,
        > {
            Ok(std::iter::empty())
        }
    }

    struct NoFacet;

    impl crate::index::field_index::FacetIndex for NoFacet {
        fn unique_values_count(&self) -> usize {
            unimplemented!()
        }

        fn for_points_values(
            &self,
            _points: impl Iterator<Item = PointOffsetType>,
            _hw_counter: &HardwareCounterCell,
            _f: impl FnMut(
                PointOffsetType,
                &mut dyn Iterator<Item = crate::data_types::facets::FacetValueRef<'_>>,
            ),
        ) -> OperationResult<()> {
            unimplemented!()
        }

        fn for_values_map(
            &self,
            _values: impl Iterator<Item = crate::data_types::facets::FacetValue>,
            _hw_counter: &HardwareCounterCell,
            _f: impl FnMut(
                crate::data_types::facets::FacetValue,
                &mut dyn Iterator<Item = PointOffsetType>,
            ) -> OperationResult<()>,
        ) -> OperationResult<()> {
            unimplemented!()
        }

        fn for_each_value(
            &self,
            _f: impl FnMut(
                crate::data_types::facets::FacetValueRef<'_>,
            ) -> OperationResult<()>,
        ) -> OperationResult<()> {
            unimplemented!()
        }

        fn for_each_count_per_value(
            &self,
            _deferred_internal_id: Option<PointOffsetType>,
            _f: impl FnMut(
                crate::data_types::facets::FacetHit<crate::data_types::facets::FacetValueRef<'_>>,
            ) -> OperationResult<()>,
        ) -> OperationResult<()> {
            unimplemented!()
        }

        fn for_each_value_map(
            &self,
            _hw_acc: &HardwareCounterCell,
            _f: impl FnMut(
                crate::data_types::facets::FacetValueRef<'_>,
                &mut dyn Iterator<Item = PointOffsetType>,
            ) -> OperationResult<()>,
        ) -> OperationResult<()> {
            unimplemented!()
        }
    }

    impl crate::index::field_index::FieldIndexRead for FailingFieldIndex {
        fn get_telemetry_data(&self) -> OperationResult<crate::telemetry::PayloadIndexTelemetry> {
            unimplemented!()
        }

        fn values_count(&self, _point_id: PointOffsetType) -> OperationResult<usize> {
            unimplemented!()
        }

        fn values_is_empty(&self, _point_id: PointOffsetType) -> OperationResult<bool> {
            unimplemented!()
        }

        fn value_retriever<'a, 'q>(
            &'a self,
            _hw_counter: &'q HardwareCounterCell,
        ) -> OperationResult<
            Option<
                crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn<'q>,
            >,
        >
        where
            'a: 'q,
        {
            Ok(None)
        }

        fn as_numeric(
            &self,
        ) -> Option<
            impl crate::index::field_index::numeric_index::NumericFieldIndexRead + '_,
        > {
            None::<NoNumeric>
        }

        fn as_facet_index(&self) -> Option<impl crate::index::field_index::FacetIndex + '_> {
            None::<NoFacet>
        }
    }

    /// A storage read error inside a field condition must propagate out of
    /// `check_payload` instead of panicking the shard thread on `unwrap`.
    #[test]
    fn test_check_payload_propagates_index_read_errors() {
        let payload = payload_json! {
            "name": "John Doe",
            "age": 43,
        };
        let payload: OwnedPayloadRef = (&payload).into();

        let query = Filter::new_must(Condition::Field(FieldCondition::new_match(
            JsonPath::new("age"),
            43.into(),
        )));

        let mut field_indexes: AHashMap<PayloadKeyType, Vec<FailingFieldIndex>> =
            AHashMap::new();
        field_indexes.insert(JsonPath::new("age"), vec![FailingFieldIndex]);

        let result = check_payload(
            Box::new(|| payload.clone()),
            None,
            &HashMap::new(),
            &query,
            0,
            &field_indexes,
            &HardwareCounterCell::new(),
        );

        assert!(
            result.is_err(),
            "index read error must propagate, not panic or silently pass"
        );
    }

    /// The infallible `check_filter` wrapper keeps behaving exactly as before
    /// for checkers that cannot fail.
    #[test]
    fn test_check_filter_infallible_wrapper_semantics() {
        let is_age = |condition: &Condition| match condition {
            Condition::Field(field_condition) => field_condition.key == JsonPath::new("age"),
            _ => false,
        };

        let filter = Filter {
            should: Some(vec![Condition::Field(FieldCondition::new_match(
                JsonPath::new("age"),
                43.into(),
            ))]),
            min_should: None,
            must: None,
            must_not: None,
        };
        assert!(check_filter(&is_age, &filter));

        let filter = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
            JsonPath::new("age"),
            43.into(),
        )));
        assert!(!check_filter(&is_age, &filter));
    }
}
