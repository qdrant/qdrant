use bitvec::prelude::*;

use crate::common::utils::{IndexesMap, JsonPathPayload};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::payload_storage::nested_query_checker::{
    check_nested_is_empty_condition, check_nested_is_null_condition, nested_check_field_condition,
};
use crate::types::{Condition, NestedCondition, PointOffsetType};

/// Given a point_id, returns the list of nested indices matching the condition and the total number of nested elements in the payload
type NestedMatchingIndicesFn<'a> = Box<dyn Fn(PointOffsetType) -> BitVec + 'a>;

/// Apply `point_id` to `nested_checkers` and return the list of indices in the payload matching all conditions
pub fn find_indices_matching_all_conditions(
    point_id: PointOffsetType,
    nested_checkers: &[NestedMatchingIndicesFn],
) -> BitVec {
    nested_checkers
        .iter()
        .map(|checker| checker(point_id))
        .reduce(|acc: BitVec, x: BitVec| acc & x)
        .unwrap_or_default()
}

/// Apply `point_id` to `nested_checkers` and return the list of indices in the payload matching none of the conditions
pub fn find_indices_matching_none_conditions(
    point_id: PointOffsetType,
    nested_checkers: &[NestedMatchingIndicesFn],
) -> BitVec {
    let combined_mask = find_indices_matching_any_conditions(point_id, nested_checkers);

    debug_assert!(combined_mask.is_some(), "combined_mask should be Some");

    combined_mask.map(|mask| !mask).unwrap_or_default()
}

/// Apply `point_id` to `nested_checkers` and return the list of indices in the payload matching any of the conditions
pub fn find_indices_matching_any_conditions(
    point_id: PointOffsetType,
    nested_checkers: &[NestedMatchingIndicesFn],
) -> Option<BitVec> {
    nested_checkers
        .iter()
        .map(|checker| checker(point_id))
        .reduce(|acc: BitVec, x: BitVec| acc | x)
}

pub fn nested_conditions_converter<'a>(
    conditions: &'a [Condition],
    payload_provider: PayloadProvider,
    field_indexes: &'a IndexesMap,
    nested_path: JsonPathPayload,
) -> Vec<NestedMatchingIndicesFn<'a>> {
    conditions
        .iter()
        .map(|condition| {
            nested_condition_converter(
                condition,
                payload_provider.clone(),
                field_indexes,
                nested_path.clone(),
            )
        })
        .collect()
}

pub fn nested_condition_converter<'a>(
    condition: &'a Condition,
    payload_provider: PayloadProvider,
    field_indexes: &'a IndexesMap,
    nested_path: JsonPathPayload,
) -> NestedMatchingIndicesFn<'a> {
    match condition {
        Condition::Field(field_condition) => {
            // Do not rely on existing indexes for nested fields because
            // they are not retaining the structure of the nested fields (flatten vs unflatten)
            // We would need specialized nested indexes.
            Box::new(move |point_id| {
                payload_provider.with_payload(point_id, |payload| {
                    nested_check_field_condition(
                        field_condition,
                        &payload,
                        &nested_path,
                        field_indexes,
                    )
                })
            })
        }
        Condition::IsEmpty(is_empty) => Box::new(move |point_id| {
            payload_provider.with_payload(point_id, |payload| {
                check_nested_is_empty_condition(&nested_path, is_empty, &payload)
            })
        }),
        Condition::IsNull(is_null) => Box::new(move |point_id| {
            payload_provider.with_payload(point_id, |payload| {
                check_nested_is_null_condition(&nested_path, is_null, &payload)
            })
        }),
        Condition::HasId(_) => {
            // No support for has_id in nested queries
            Box::new(move |_| BitVec::default())
        }
        Condition::Nested(nested) => {
            Box::new(move |point_id| {
                let mut bitvecs = Vec::with_capacity(3);

                // must
                let must_matching = check_nested_must(
                    point_id,
                    nested,
                    field_indexes,
                    payload_provider.clone(),
                    nested_path.clone(),
                );
                if let Some(must_matching) = must_matching {
                    bitvecs.push(must_matching);
                }

                // must_not
                let must_not_matching = check_nested_must_not(
                    point_id,
                    nested,
                    field_indexes,
                    payload_provider.clone(),
                    &nested_path,
                );
                if let Some(must_not_matching) = must_not_matching {
                    bitvecs.push(must_not_matching);
                }

                // should
                let should_matching = check_nested_should(
                    point_id,
                    nested,
                    field_indexes,
                    payload_provider.clone(),
                    nested_path.clone(),
                );
                if let Some(should_matching) = should_matching {
                    bitvecs.push(should_matching);
                }

                // combine all bitvecs
                bitvecs
                    .into_iter()
                    .reduce(|acc, x| {
                        debug_assert_eq!(acc.len(), x.len());
                        acc & x
                    })
                    .unwrap_or_default()
            })
        }
        Condition::Filter(_) => unreachable!(),
    }
}

fn check_nested_must(
    point_id: PointOffsetType,
    nested: &NestedCondition,
    field_indexes: &IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: JsonPathPayload,
) -> Option<BitVec> {
    match &nested.filter().must {
        None => None,
        Some(musts_conditions) => {
            let full_path = nested_path.extend(&nested.array_key());
            let nested_checkers = nested_conditions_converter(
                musts_conditions,
                payload_provider,
                field_indexes,
                full_path,
            );
            let matches = find_indices_matching_all_conditions(point_id, &nested_checkers);
            Some(matches)
        }
    }
}

fn check_nested_must_not(
    point_id: PointOffsetType,
    nested: &NestedCondition,
    field_indexes: &IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: &JsonPathPayload,
) -> Option<BitVec> {
    match &nested.filter().must_not {
        None => None,
        Some(musts_not_conditions) => {
            let full_path = nested_path.extend(&nested.array_key());
            let matching_indices = nested_conditions_converter(
                musts_not_conditions,
                payload_provider,
                field_indexes,
                full_path,
            );
            let matches = find_indices_matching_none_conditions(point_id, &matching_indices);
            Some(matches)
        }
    }
}

fn check_nested_should(
    point_id: PointOffsetType,
    nested: &NestedCondition,
    field_indexes: &IndexesMap,
    payload_provider: PayloadProvider,
    nested_path: JsonPathPayload,
) -> Option<BitVec> {
    match &nested.filter().should {
        None => None,
        Some(musts_not_conditions) => {
            let full_path = nested_path.extend(&nested.array_key());
            let matching_indices = nested_conditions_converter(
                musts_not_conditions,
                payload_provider,
                field_indexes,
                full_path,
            );
            find_indices_matching_any_conditions(point_id, &matching_indices)
        }
    }
}
