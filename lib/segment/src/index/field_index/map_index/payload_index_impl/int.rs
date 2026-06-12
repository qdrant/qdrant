use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use gridstore::Blob;
use itertools::Itertools;

use super::super::MapIndex;
use super::super::key::MapIndexKey;
use super::super::read_only::ReadOnlyMapIndex;
use super::super::read_ops::MapIndexRead;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    PrimaryCondition,
};
use crate::index::query_estimator::combine_should_estimations;
use crate::index::query_optimization::optimized_filter::DynConditionChecker;
use crate::types::{
    AnyVariants, FieldCondition, IntPayloadType, Match, MatchAny, MatchExcept, MatchValue,
    PayloadKeyType, ValueVariants,
};

impl PayloadFieldIndex for MapIndex<IntPayloadType> {
    fn wipe(self) -> OperationResult<()> {
        self.wipe()
    }

    fn flusher(&self) -> Flusher {
        MapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        self.files()
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.immutable_files()
    }
}

impl PayloadFieldIndexRead for MapIndex<IntPayloadType> {
    fn count_indexed_points(&self) -> usize {
        MapIndexRead::get_indexed_points(self)
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        filter_impl(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(estimate_cardinality_impl(self, condition, hw_counter))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_each_payload_block_impl(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<DynConditionChecker<'a>>> {
        Ok(condition_checker_impl(self, condition, hw_acc))
    }
}

impl<S: UniversalRead> PayloadFieldIndexRead for ReadOnlyMapIndex<IntPayloadType, S>
where
    Vec<<IntPayloadType as MapIndexKey>::Owned>: Blob + Send + Sync,
{
    fn count_indexed_points(&self) -> usize {
        MapIndexRead::get_indexed_points(self)
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        filter_impl(self, condition, hw_counter)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(estimate_cardinality_impl(self, condition, hw_counter))
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        for_each_payload_block_impl(self, threshold, key, f)
    }

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> OperationResult<Option<DynConditionChecker<'a>>> {
        Ok(condition_checker_impl(self, condition, hw_acc))
    }
}

// Shared bodies for `MapIndex<IntPayloadType>` and
// `ReadOnlyMapIndex<IntPayloadType, S>`.

fn filter_impl<'a, T: MapIndexRead<'a, IntPayloadType>>(
    index: &'a T,
    condition: &'a FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
    let result: Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> = match &condition.r#match {
        Some(Match::Value(MatchValue { value })) => match value {
            ValueVariants::String(_) => None,
            ValueVariants::Integer(integer) => {
                Some(Box::new(index.get_iterator(integer, hw_counter)))
            }
            ValueVariants::Bool(_) => None,
        },
        Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
            AnyVariants::Strings(keywords) => {
                if keywords.is_empty() {
                    Some(Box::new(std::iter::empty()))
                } else {
                    None
                }
            }
            AnyVariants::Integers(integers) => Some(Box::new(
                integers
                    .iter()
                    .flat_map(move |integer| index.get_iterator(integer, hw_counter))
                    .unique(),
            )),
        },
        Some(Match::Except(MatchExcept { except })) => match except {
            AnyVariants::Strings(_) => None,
            AnyVariants::Integers(integers) => Some(index.except_set(integers, hw_counter)?),
        },
        _ => None,
    };

    Ok(result)
}

fn estimate_cardinality_impl<'a, T: MapIndexRead<'a, IntPayloadType>>(
    index: &'a T,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> Option<CardinalityEstimation> {
    match &condition.r#match {
        Some(Match::Value(MatchValue { value })) => match value {
            ValueVariants::String(_) => None,
            ValueVariants::Integer(integer) => {
                let mut estimation = index.match_cardinality(integer, hw_counter);
                estimation
                    .primary_clauses
                    .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                Some(estimation)
            }
            ValueVariants::Bool(_) => None,
        },
        Some(Match::Any(MatchAny { any: any_variants })) => match any_variants {
            AnyVariants::Strings(keywords) => {
                if keywords.is_empty() {
                    Some(CardinalityEstimation::exact(0).with_primary_clause(
                        PrimaryCondition::Condition(Box::new(condition.clone())),
                    ))
                } else {
                    None
                }
            }
            AnyVariants::Integers(integers) => {
                let estimations = integers
                    .iter()
                    .map(|integer| index.match_cardinality(integer, hw_counter))
                    .collect::<Vec<_>>();
                let estimation = if estimations.is_empty() {
                    CardinalityEstimation::exact(0)
                } else {
                    combine_should_estimations(&estimations, index.get_indexed_points())
                };
                Some(
                    estimation.with_primary_clause(PrimaryCondition::Condition(Box::new(
                        condition.clone(),
                    ))),
                )
            }
        },
        Some(Match::Except(MatchExcept { except })) => match except {
            AnyVariants::Strings(_) => None,
            AnyVariants::Integers(integers) => {
                Some(index.except_cardinality(integers.iter(), hw_counter))
            }
        },
        _ => None,
    }
}

fn for_each_payload_block_impl<'a, T: MapIndexRead<'a, IntPayloadType>>(
    index: &'a T,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    index.for_each_value(|value| {
        let count = index
            // Only used in HNSW building so no measurement needed here.
            .get_count_for_value(value, &HardwareCounterCell::disposable())
            .unwrap_or(0);
        if count >= threshold {
            f(PayloadBlockCondition {
                condition: FieldCondition::new_match(key.clone(), (*value).into()),
                cardinality: count,
            })?;
        }
        Ok(())
    })
}

fn condition_checker_impl<'a, T: MapIndexRead<'a, IntPayloadType> + 'a>(
    index: &'a T,
    condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<DynConditionChecker<'a>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let cond_match = r#match.as_ref()?;
    let hw_counter = hw_acc.get_counter_cell();
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::Integer(value),
        }) => Some(index.match_value_checker(hw_counter, *value)),
        Match::Any(MatchAny {
            any: AnyVariants::Integers(list),
        }) => Some(index.match_any_checker(hw_counter, list.clone(), false)),
        Match::Except(MatchExcept {
            except: AnyVariants::Integers(list),
        }) => Some(index.match_any_checker(hw_counter, list.clone(), true)),
        // Conditions this index can't serve.
        Match::Value(MatchValue {
            value: ValueVariants::String(_) | ValueVariants::Bool(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Strings(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(_),
        })
        | Match::Text(_)
        | Match::TextAny(_)
        | Match::Phrase(_) => None,
    }
}
