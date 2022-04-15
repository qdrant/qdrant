use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::index::query_optimization::condition_converter::{
    get_geo_bounding_box_checkers, get_geo_radius_checkers, get_match_checkers, get_range_checkers,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::optimizer::IndexesMap;
use crate::payload_storage::query_checker::check_filter;
use crate::payload_storage::{ConditionCheckerSS, FilterContext};
use crate::types::{Condition, Filter, PointOffsetType};
use std::collections::HashMap;
use std::sync::Arc;

pub struct StructFilterContext<'a> {
    condition_checker: Arc<ConditionCheckerSS>,
    filter: &'a Filter,
    primary_checkers: Vec<ConditionCheckerFn<'a>>,
}

pub fn fast_check_payload(
    query: &Filter,
    point_id: PointOffsetType,
    checkers: &HashMap<&str, Box<dyn Fn(PointOffsetType) -> bool>>,
) -> bool {
    let checker = |condition: &Condition| match condition {
        Condition::Field(field_condition) => {
            checkers.get(field_condition.key.as_str()).unwrap()(point_id)
        }
        _ => panic!("unexpected condition"),
    };

    check_filter(&checker, query)
}

impl<'a> StructFilterContext<'a> {
    pub fn new(
        condition_checker: Arc<ConditionCheckerSS>,
        filter: &'a Filter,
        field_indexes: &'a IndexesMap,
        cardinality_estimation: CardinalityEstimation,
    ) -> Self {
        let mut checkers: Vec<ConditionCheckerFn<'a>> = vec![];

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
            primary_checkers: checkers,
        }
    }
}

impl<'a> FilterContext for StructFilterContext<'a> {
    fn check(&self, point_id: PointOffsetType) -> bool {
        // At least one primary condition should be satisfied - that is necessary, but not sufficient condition
        if !self.primary_checkers.is_empty()
            && !self.primary_checkers.iter().any(|check| check(point_id))
        {
            false
        } else {
            self.condition_checker.check(point_id, self.filter)
        }
    }
}
