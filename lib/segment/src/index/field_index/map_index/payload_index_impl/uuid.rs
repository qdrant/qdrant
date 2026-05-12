use std::iter;
use std::path::PathBuf;
use std::str::FromStr;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use indexmap::IndexSet;
use itertools::Itertools;
use uuid::Uuid;

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
    AnyVariants, FieldCondition, Match, MatchAny, MatchExcept, MatchValue, PayloadKeyType,
    UuidIntType, ValueVariants,
};

impl PayloadFieldIndex for MapIndex<UuidIntType> {
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

impl PayloadFieldIndexRead for MapIndex<UuidIntType> {
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
                    ValueVariants::String(uuid_string) => {
                        let Ok(uuid) = Uuid::from_str(uuid_string) else {
                            return Ok(None);
                        };
                        Some(Box::new(self.get_iterator(&uuid.as_u128(), hw_counter)))
                    }
                    ValueVariants::Integer(_) => None,
                    ValueVariants::Bool(_) => None,
                },
                Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                    AnyVariants::Strings(uuids_string) => {
                        let Ok(uuids) = uuids_string
                            .iter()
                            .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                            .collect::<Result<IndexSet<u128>, _>>()
                        else {
                            return Ok(None);
                        };

                        Some(Box::new(
                            uuids
                                .into_iter()
                                .flat_map(move |uuid| self.get_iterator(&uuid, hw_counter))
                                .unique(),
                        ))
                    }
                    AnyVariants::Integers(integers) => {
                        if integers.is_empty() {
                            Some(Box::new(iter::empty()))
                        } else {
                            None
                        }
                    }
                },
                Some(Match::Except(MatchExcept { except })) => match except {
                    AnyVariants::Strings(uuids_string) => {
                        let Ok(excluded_uuids) = uuids_string
                            .iter()
                            .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                            .collect::<Result<IndexSet<u128>, _>>()
                        else {
                            return Ok(None);
                        };
                        let mut points = IndexSet::new();
                        self.for_each_value(|key| {
                            if !excluded_uuids.contains(key) {
                                self.get_iterator(key, hw_counter).for_each(|p| {
                                    points.insert(p);
                                });
                            }
                            Ok(())
                        })?;
                        Some(Box::new(points.into_iter()))
                    }
                    AnyVariants::Integers(other) => {
                        if other.is_empty() {
                            Some(Box::new(iter::empty()))
                        } else {
                            None
                        }
                    }
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
                ValueVariants::String(uuid_string) => {
                    let Some(uuid) = Uuid::from_str(uuid_string).ok() else {
                        return Ok(None);
                    };
                    let mut estimation = self.match_cardinality(&uuid.as_u128(), hw_counter);
                    estimation
                        .primary_clauses
                        .push(PrimaryCondition::Condition(Box::new(condition.clone())));
                    Some(estimation)
                }
                ValueVariants::Integer(_) => None,
                ValueVariants::Bool(_) => None,
            },
            Some(Match::Any(MatchAny { any: any_variant })) => match any_variant {
                AnyVariants::Strings(uuids_string) => {
                    let uuids: Result<IndexSet<u128>, _> = uuids_string
                        .iter()
                        .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                        .collect();

                    let Some(uuids) = uuids.ok() else {
                        return Ok(None);
                    };

                    let estimations = uuids
                        .into_iter()
                        .map(|uuid| self.match_cardinality(&uuid, hw_counter))
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
                AnyVariants::Integers(integers) => {
                    if integers.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
                }
            },
            Some(Match::Except(MatchExcept { except })) => match except {
                AnyVariants::Strings(uuids_string) => {
                    let uuids: Result<IndexSet<u128>, _> = uuids_string
                        .iter()
                        .map(|uuid_string| Uuid::from_str(uuid_string).map(|x| x.as_u128()))
                        .collect();

                    let Some(excluded_uuids) = uuids.ok() else {
                        return Ok(None);
                    };

                    Some(self.except_cardinality(excluded_uuids.iter(), hw_counter))
                }
                AnyVariants::Integers(other) => {
                    if other.is_empty() {
                        Some(CardinalityEstimation::exact(0).with_primary_clause(
                            PrimaryCondition::Condition(Box::new(condition.clone())),
                        ))
                    } else {
                        None
                    }
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
                .get_count_for_value(value, &HardwareCounterCell::disposable()) // payload_blocks only used in HNSW building, which is unmeasured.
                .unwrap_or(0);
            if count >= threshold {
                f(PayloadBlockCondition {
                    condition: FieldCondition::new_match(
                        key.clone(),
                        Uuid::from_u128(*value).to_string().into(),
                    ),
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
                value: ValueVariants::String(keyword),
            }) => {
                let uuid = Uuid::parse_str(keyword).map(|u| u.as_u128()).ok()?;
                Some(Box::new(move |point_id: PointOffsetType| {
                    self.check_values_any(point_id, &hw_counter, |value| value == &uuid)
                }))
            }
            Match::Any(MatchAny {
                any: AnyVariants::Strings(list),
            }) => {
                let list = list
                    .iter()
                    .map(|s| Uuid::parse_str(s).map(|u| u.as_u128()).ok())
                    .collect::<Option<IndexSet<_>>>()?;
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
                except: AnyVariants::Strings(list),
            }) => {
                let list = list
                    .iter()
                    .map(|s| Uuid::parse_str(s).map(|u| u.as_u128()).ok())
                    .collect::<Option<IndexSet<_>>>()?;
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
                value: ValueVariants::Integer(_) | ValueVariants::Bool(_),
            })
            | Match::Any(MatchAny {
                any: AnyVariants::Integers(_),
            })
            | Match::Except(MatchExcept {
                except: AnyVariants::Integers(_),
            })
            | Match::Text(_)
            | Match::TextAny(_)
            | Match::Phrase(_) => None,
        }
    }
}
