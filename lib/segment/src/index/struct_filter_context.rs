use crate::index::field_index::{CardinalityEstimation, FieldIndex, PrimaryCondition};
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::types::{
    Filter, FloatPayloadType, GeoBoundingBox, GeoRadius, Match, MatchValue, PayloadKeyType,
    PointOffsetType, Range, ValueVariants,
};
use std::collections::HashMap;
use std::sync::Arc;

pub type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;
pub type ConditionChecker<'a> = Box<dyn Fn(PointOffsetType) -> bool + 'a>;

pub struct StructFilterContext<'a> {
    condition_checker: Arc<ConditionCheckerSS>,
    filter: &'a Filter,
    checkers: Vec<ConditionChecker<'a>>,
}

impl<'a> StructFilterContext<'a> {
    pub fn new(
        condition_checker: Arc<ConditionCheckerSS>,
        filter: &'a Filter,
        field_indexes: &'a IndexesMap,
        cardinality_estimation: CardinalityEstimation,
    ) -> Self {
        let mut checkers: Vec<ConditionChecker<'a>> = vec![];

        for clause in cardinality_estimation.primary_clauses {
            match clause {
                PrimaryCondition::Condition(field_condition) => {
                    let indexes_opt = field_indexes.get(&field_condition.key);
                    let indexes = if let Some(indexes) = indexes_opt {
                        indexes
                    } else {
                        continue;
                    };
                    for index in indexes {
                        checkers.extend(
                            field_condition
                                .r#match
                                .clone()
                                .and_then(|cond| get_match_checkers(index, cond)),
                        );
                        checkers.extend(
                            field_condition
                                .range
                                .clone()
                                .and_then(|cond| get_range_checkers(index, cond)),
                        );
                        checkers.extend(
                            field_condition
                                .geo_radius
                                .clone()
                                .and_then(|cond| get_geo_radius_checkers(index, cond)),
                        );
                        checkers.extend(
                            field_condition
                                .geo_bounding_box
                                .clone()
                                .and_then(|cond| get_geo_bounding_box_checkers(index, cond)),
                        );
                    }
                }
                PrimaryCondition::IsEmpty(_) => {}
                PrimaryCondition::Ids(_) => {}
            }
        }

        Self {
            condition_checker,
            filter,
            checkers,
        }
    }
}

fn get_geo_radius_checkers(index: &FieldIndex, geo_radius: GeoRadius) -> Option<ConditionChecker> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match geo_index.get_values(point_id) {
                None => false,
                Some(values) => values
                    .iter()
                    .any(|geo_point| geo_radius.check_point(geo_point.lon, geo_point.lat)),
            }
        })),
        _ => None,
    }
}

fn get_geo_bounding_box_checkers(
    index: &FieldIndex,
    geo_bounding_box: GeoBoundingBox,
) -> Option<ConditionChecker> {
    match index {
        FieldIndex::GeoIndex(geo_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match geo_index.get_values(point_id) {
                None => false,
                Some(values) => values
                    .iter()
                    .any(|geo_point| geo_bounding_box.check_point(geo_point.lon, geo_point.lat)),
            }
        })),
        _ => None,
    }
}

fn get_range_checkers(index: &FieldIndex, range: Range) -> Option<ConditionChecker> {
    match index {
        FieldIndex::IntIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => false,
                Some(values) => values
                    .iter()
                    .copied()
                    .any(|i| range.check_range(i as FloatPayloadType)),
            }
        })),
        FieldIndex::FloatIndex(num_index) => Some(Box::new(move |point_id: PointOffsetType| {
            match num_index.get_values(point_id) {
                None => false,
                Some(values) => values.iter().copied().any(|i| range.check_range(i)),
            }
        })),
        _ => None,
    }
}

fn get_match_checkers(index: &FieldIndex, cond_match: Match) -> Option<ConditionChecker> {
    if let Match::Value(MatchValue {
        value: value_variant,
    }) = cond_match
    {
        match (value_variant, index) {
            (ValueVariants::Keyword(keyword), FieldIndex::KeywordIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => false,
                        Some(values) => values.iter().any(|k| k == &keyword),
                    }
                }))
            }
            (ValueVariants::Integer(value), FieldIndex::IntMapIndex(index)) => {
                Some(Box::new(move |point_id: PointOffsetType| {
                    match index.get_values(point_id) {
                        None => false,
                        Some(values) => values.iter().any(|i| i == &value),
                    }
                }))
            }
            (_, _) => None,
        }
    } else {
        None
    }
}

impl<'a> FilterContext for StructFilterContext<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        // At least one primary condition should be satisfied - that is necessary, but not sufficient condition
        if !self.checkers.is_empty() && !self.checkers.iter().any(|foo| foo(point_id)) {
            false
        } else {
            self.condition_checker.check(point_id, self.filter)
        }
    }
}
