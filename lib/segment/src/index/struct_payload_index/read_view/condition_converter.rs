use std::collections::HashMap;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use serde_json::Value;

use super::StructPayloadIndexReadView;
use crate::id_tracker::IdTrackerRead;
use crate::index::query_optimization::condition_converter::{
    field_condition_index, get_fallback_is_empty_checker, get_is_empty_indexes,
    get_is_null_checker, get_null_index_is_empty_checker,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::query_checker::{
    check_field_condition, check_is_empty_condition, check_is_null_condition, check_payload,
    select_nested_indexes,
};
use crate::types::{Condition, OwnedPayloadRef, PayloadContainer};
use crate::vector_storage::VectorStorageRead;

impl<'a, P, I, V> StructPayloadIndexReadView<'a, P, I, V>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
{
    pub fn condition_converter<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        condition: &'b Condition,
        payload_provider: PayloadProvider<S>,
        hw_counter: &HardwareCounterCell,
    ) -> ConditionCheckerFn<'b> {
        let id_tracker = self.id_tracker;
        let field_indexes = self.field_indexes;
        match condition {
            Condition::Field(field_condition) => field_indexes
                .get(&field_condition.key)
                .and_then(|indexes| {
                    indexes.iter().find_map(move |index| {
                        let hw_acc = hw_counter.new_accumulator();
                        field_condition_index(index, field_condition, hw_acc)
                    })
                })
                .unwrap_or_else(|| {
                    let hw = hw_counter.fork();
                    Box::new(move |point_id| {
                        payload_provider.with_payload(
                            point_id,
                            |payload| {
                                check_field_condition(field_condition, &payload, field_indexes, &hw)
                                    .unwrap(/* TODO(uio): handle errors */)
                            },
                            &hw,
                        )
                    })
                }),
            // Use dedicated null index for `is_empty` check if it is available
            // Otherwise we might use another index just to check if a field is not empty, if we
            // don't have an indexed value we must still check the payload to see if its empty
            Condition::IsEmpty(is_empty) => {
                let field_indexes = field_indexes.get(&is_empty.is_empty.key);

                let (primary_null_index, fallback_index) = field_indexes
                    .map(|field_indexes| get_is_empty_indexes(field_indexes))
                    .unwrap_or((None, None));

                if let Some(null_index) = primary_null_index {
                    get_null_index_is_empty_checker(null_index, true)
                } else {
                    // Fallback to reading payload, in case we don't yet have null-index
                    let hw = hw_counter.fork();
                    let fallback = Box::new(move |point_id| {
                        payload_provider.with_payload(
                            point_id,
                            |payload| check_is_empty_condition(is_empty, &payload),
                            &hw,
                        )
                    });

                    if let Some(fallback_index) = fallback_index {
                        get_fallback_is_empty_checker(fallback_index, true, fallback)
                    } else {
                        fallback
                    }
                }
            }

            Condition::IsNull(is_null) => {
                let field_indexes = field_indexes.get(&is_null.is_null.key);

                let is_null_checker = field_indexes.and_then(|field_indexes| {
                    field_indexes
                        .iter()
                        .find_map(|index| get_is_null_checker(index, true))
                });

                if let Some(checker) = is_null_checker {
                    checker
                } else {
                    // Fallback to reading payload
                    let hw = hw_counter.fork();
                    Box::new(move |point_id| {
                        payload_provider.with_payload(
                            point_id,
                            |payload| check_is_null_condition(is_null, &payload),
                            &hw,
                        )
                    })
                }
            }
            // ToDo: It might be possible to make this condition faster by using `VisitedPool` instead of HashSet
            Condition::HasId(has_id) => {
                let segment_ids: AHashSet<_> = has_id
                    .has_id
                    .iter()
                    .filter_map(|external_id| id_tracker.internal_id(*external_id))
                    .collect();
                Box::new(move |point_id| segment_ids.contains(&point_id))
            }
            Condition::HasVector(has_vector) => {
                if let Some(vector_storage) =
                    self.vector_storages.get(&has_vector.has_vector).cloned()
                {
                    Box::new(move |point_id| !vector_storage.borrow().is_deleted_vector(point_id))
                } else {
                    Box::new(|_point_id| false)
                }
            }
            Condition::Nested(nested) => {
                // Select indexes for nested fields. Trim nested part from key, so
                // that nested condition can address fields without nested part.

                // Example:
                // Index for field `nested.field` will be stored under key `nested.field`
                // And we have a query:
                // {
                //   "nested": {
                //     "path": "nested",
                //     "filter": {
                //         ...
                //         "match": {"key": "field", "value": "value"}
                //     }
                //   }

                // In this case we want to use `nested.field`, but we only have `field` in query.
                // Therefore we need to trim `nested` part from key. So that query executor
                // can address proper index for nested field.
                let nested_path = nested.array_key();

                let nested_indexes = select_nested_indexes(&nested_path, field_indexes);

                let hw = hw_counter.fork();
                Box::new(move |point_id| {
                    payload_provider.with_payload(
                        point_id,
                        |payload| {
                            let field_values = payload.get_value(&nested_path);

                            for value in field_values {
                                if let Value::Object(object) = value {
                                    let get_payload = || OwnedPayloadRef::from(object);
                                    if check_payload(
                                        Box::new(get_payload),
                                        // None because has_id in nested is not supported. So retrieving
                                        // IDs through the tracker would always return None.
                                        None,
                                        // Same as above, nested conditions don't support has_vector.
                                        &HashMap::new(),
                                        &nested.nested.filter,
                                        point_id,
                                        &nested_indexes,
                                        &hw,
                                    ) {
                                        // If at least one nested object matches, return true
                                        return true;
                                    }
                                }
                            }
                            false
                        },
                        &hw,
                    )
                })
            }
            Condition::CustomIdChecker(cond) => {
                let segment_ids: AHashSet<_> = id_tracker
                    .point_mappings()
                    .iter_external()
                    .filter(|&point_id| cond.0.check(point_id))
                    .filter_map(|external_id| id_tracker.internal_id(external_id))
                    .collect();

                Box::new(move |internal_id| segment_ids.contains(&internal_id))
            }
            Condition::Filter(_) => unreachable!(),
        }
    }
}
