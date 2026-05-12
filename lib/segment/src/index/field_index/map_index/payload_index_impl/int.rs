use std::iter;
use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;

use super::super::MapIndex;
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    PrimaryCondition,
};
use crate::index::query_estimator::combine_should_estimations;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::payload_storage::condition_checker::INDEXSET_ITER_THRESHOLD;
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
        self.get_indexed_points()
    }

    fn filter<'a>(
        &'a self,
        condition: &'a FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        let result: Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> =
            match &condition.r#match {
                Some(Match::Value(MatchValue { value })) => match value {
                    ValueVariants::String(_) => None,
                    ValueVariants::Integer(integer) => {
                        Some(Box::new(self.get_iterator(integer, hw_counter)))
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
                            .flat_map(move |integer| self.get_iterator(integer, hw_counter))
                            .unique(),
                    )),
                },
                Some(Match::Except(MatchExcept { except })) => match except {
                    AnyVariants::Strings(other) => {
                        if other.is_empty() {
                            Some(Box::new(iter::empty()))
                        } else {
                            None
                        }
                    }
                    AnyVariants::Integers(integers) => Some(self.except_set(integers, hw_counter)?),
                },
                _ => None,
            };

        Ok(result)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        Ok(match &condition.r#match {
            Some(Match::Value(MatchValue { value })) => match value {
                ValueVariants::String(_) => None,
                ValueVariants::Integer(integer) => {
                    let mut estimation = self.match_cardinality(integer, hw_counter);
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
                        .map(|integer| self.match_cardinality(integer, hw_counter))
                        .collect::<Vec<_>>();
                    let estimation = if estimations.is_empty() {
                        CardinalityEstimation::exact(0)
                    } else {
                        combine_should_estimations(&estimations, self.get_indexed_points())
                    };
                    Some(
                        estimation.with_primary_clause(PrimaryCondition::Condition(Box::new(
                            condition.clone(),
                        ))),
                    )
                }
            },
            Some(Match::Except(MatchExcept { except })) => match except {
                AnyVariants::Strings(others) => {
                    if others.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
                AnyVariants::Integers(integers) => {
                    Some(self.except_cardinality(integers.iter(), hw_counter))
                }
            },
            _ => None,
        })
    }

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.for_each_value(|value| {
            let count = self
                .get_count_for_value(value, &HardwareCounterCell::disposable()) // Only used in HNSW building so no measurement needed here.
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

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
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
            }) => {
                let value = *value;
                Some(Box::new(move |point_id: PointOffsetType| {
                    self.check_values_any(point_id, &hw_counter, |i| *i == value)
                }))
            }
            Match::Any(MatchAny {
                any: AnyVariants::Integers(list),
            }) => {
                let list = list.clone();
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            list.iter().any(|i| i == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| list.contains(value))
                    }))
                }
            }
            Match::Except(MatchExcept {
                except: AnyVariants::Integers(list),
            }) => {
                let list = list.clone();
                if list.len() < INDEXSET_ITER_THRESHOLD {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| {
                            !list.iter().any(|i| i == value)
                        })
                    }))
                } else {
                    Some(Box::new(move |point_id: PointOffsetType| {
                        self.check_values_any(point_id, &hw_counter, |value| !list.contains(value))
                    }))
                }
            }
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
}
