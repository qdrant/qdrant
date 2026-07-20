use std::collections::HashMap;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use atomic_refcell::AtomicRefCell;
use common::condition_checker::{ConditionChecker, ConstantConditionChecker};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{DeferredBehavior, PointOffsetType};
use serde_json::Value;

use super::StructPayloadIndexReadView;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::id_tracker::IdTrackerRead;
use crate::index::condition_checker::ConditionCheckerEnum;
use crate::index::field_index::FieldIndexRead;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::payload_storage::query_checker::{
    check_field_condition, check_is_empty_condition, check_is_null_condition, check_payload,
    select_nested_indexes,
};
use crate::types::{Condition, FieldCondition, OwnedPayloadRef, PayloadContainer};
use crate::vector_storage::VectorStorageRead;

impl<'a, P, I, V, F> StructPayloadIndexReadView<'a, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    pub fn condition_converter<'b, S: PayloadStorageRead + 'b>(
        &'b self,
        condition: &'b Condition,
        payload_provider: PayloadProvider<S>,
        deferred_behavior: DeferredBehavior,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<ConditionCheckerEnum<'b>> {
        let id_tracker = self.id_tracker;
        let field_indexes = self.field_indexes;
        Ok(match condition {
            Condition::Field(field_condition) => field_condition_checker(
                field_indexes,
                &field_condition.key,
                hw_counter,
                field_condition,
                payload_provider,
                |payload, hw| check_field_condition(field_condition, &payload, field_indexes, hw),
            )?,
            // is_empty / is_null are served by NullIndex via
            // `condition_checker`. NullIndex is built alongside every
            // index from #6088 (released in v1.13.5) onwards, so the
            // direct path covers every collection created since. For
            // segments older than that the payload-fallback below
            // handles it — without the historic `values_is_empty`
            // fast-path, on the assumption that ~5 minor releases of
            // upgrades have effectively migrated those segments.
            Condition::IsEmpty(is_empty) => {
                let key = &is_empty.is_empty.key;
                field_condition_checker(
                    field_indexes,
                    key,
                    hw_counter,
                    &FieldCondition::new_is_empty(key.clone(), true),
                    payload_provider,
                    |payload, _| Ok(check_is_empty_condition(is_empty, &payload)),
                )?
            }

            Condition::IsNull(is_null) => {
                let key = &is_null.is_null.key;
                field_condition_checker(
                    field_indexes,
                    key,
                    hw_counter,
                    &FieldCondition::new_is_null(key.clone(), true),
                    payload_provider,
                    |payload, _| Ok(check_is_null_condition(is_null, &payload)),
                )?
            }
            // ToDo: It might be possible to make this condition faster by using `VisitedPool` instead of HashSet
            Condition::HasId(has_id) => {
                let mut segment_ids = AHashSet::with_capacity(has_id.has_id.len());
                id_tracker.resolve_external_ids(
                    has_id.has_id.iter().copied(),
                    deferred_behavior,
                    |_, offset| {
                        segment_ids.insert(offset);
                    },
                )?;
                ConditionCheckerEnum::Ids(IdsConditionChecker(segment_ids))
            }
            Condition::HasVector(has_vector) => {
                if let Some(vector_storage) =
                    self.vector_storages.get(&has_vector.has_vector).cloned()
                {
                    ConditionCheckerEnum::Dyn(Box::new(HasVectorConditionChecker(vector_storage)))
                } else {
                    ConditionCheckerEnum::Constant(ConstantConditionChecker::MATCH_NONE)
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

                ConditionCheckerEnum::Dyn(Box::new(PayloadConditionChecker {
                    payload_provider,
                    hw_counter: hw_counter.fork(),
                    check: move |payload, point_id, hw| {
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
                                    hw,
                                ) {
                                    // If at least one nested object matches, return true
                                    return Ok(true);
                                }
                            }
                        }
                        Ok(false)
                    },
                }))
            }
            Condition::CustomIdChecker(cond) => {
                let segment_ids: AHashSet<_> = id_tracker
                    .point_mappings()
                    .iter_external()
                    .filter(|&point_id| cond.0.check(point_id))
                    .filter_map(|external_id| {
                        id_tracker.internal_id_with_behavior(external_id, deferred_behavior)
                    })
                    .collect();

                ConditionCheckerEnum::Ids(IdsConditionChecker(segment_ids))
            }
            Condition::Filter(_) => unreachable!(),
        })
    }
}

/// For [`Condition::Field`], [`Condition::IsEmpty`] and [`Condition::IsNull`].
fn field_condition_checker<'a>(
    field_indexes: &'a AHashMap<JsonPath, Vec<impl FieldIndexRead>>,
    key: &JsonPath,
    hw_counter: &HardwareCounterCell,
    field_condition: &FieldCondition,
    payload_provider: PayloadProvider<impl PayloadStorageRead + 'a>,
    check: impl Fn(OwnedPayloadRef, &HardwareCounterCell) -> OperationResult<bool> + 'a,
) -> OperationResult<ConditionCheckerEnum<'a>> {
    // 1. Find first index that can check condition.
    if let Some(indexes) = field_indexes.get(key) {
        for index in indexes {
            let hw_acc = hw_counter.new_accumulator();
            if let Some(checker) = index.condition_checker(field_condition, hw_acc)? {
                return Ok(checker);
            }
        }
    }

    // 2. None found => fallback to payload check.
    Ok(ConditionCheckerEnum::Dyn(Box::new(
        PayloadConditionChecker {
            payload_provider,
            hw_counter: hw_counter.fork(),
            check: move |payload, _, hw| check(payload, hw),
        },
    )))
}

/// For [`field_condition_checker`] and [`Condition::Nested`].
struct PayloadConditionChecker<S, F>
where
    S: PayloadStorageRead,
    F: Fn(OwnedPayloadRef, PointOffsetType, &HardwareCounterCell) -> OperationResult<bool>,
{
    payload_provider: PayloadProvider<S>,
    hw_counter: HardwareCounterCell,
    check: F,
}

impl<S, F> ConditionChecker for PayloadConditionChecker<S, F>
where
    S: PayloadStorageRead,
    F: Fn(OwnedPayloadRef, PointOffsetType, &HardwareCounterCell) -> OperationResult<bool>,
{
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        self.payload_provider.with_payload(
            point_id,
            |payload| (self.check)(payload, point_id, &self.hw_counter),
            &self.hw_counter,
        )
    }
}

/// For [`Condition::HasId`] and [`Condition::CustomIdChecker`].
pub struct IdsConditionChecker(AHashSet<PointOffsetType>);

impl ConditionChecker for IdsConditionChecker {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(self.0.contains(&point_id))
    }
}

/// For [`Condition::HasVector`].
struct HasVectorConditionChecker<V: VectorStorageRead>(Arc<AtomicRefCell<V>>);

impl<V: VectorStorageRead> ConditionChecker for HasVectorConditionChecker<V> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.0.borrow().is_deleted_vector(point_id))
    }
}
