use common::types::PointOffsetType;

use crate::index::field_index::CardinalityEstimation;
use crate::types::PayloadKeyType;

pub type ConditionCheckerFn<'a> = Box<dyn Fn(PointOffsetType) -> bool + 'a>;

#[derive(Debug, Clone, Default)]
pub struct ConditionMeta {
    /// List of un-indexed fields, used in this search condition
    pub missing_index: Vec<PayloadKeyType>,
}

pub struct OptimizedConditionChecker<'a> {
    pub checker: ConditionCheckerFn<'a>,
    pub estimated_cardinality: CardinalityEstimation,
    pub meta: ConditionMeta,
}

pub struct OptimizedConditionFilter<'a> {
    pub filter: OptimizedFilter<'a>,
    pub estimated_cardinality: CardinalityEstimation,
}

pub enum OptimizedCondition<'a> {
    Checker(OptimizedConditionChecker<'a>),
    /// Nested filter
    Filter(OptimizedConditionFilter<'a>),
}

impl OptimizedCondition<'_> {
    pub fn estimated_cardinality(&self) -> &CardinalityEstimation {
        match self {
            OptimizedCondition::Checker(checker) => &checker.estimated_cardinality,
            OptimizedCondition::Filter(filter) => &filter.estimated_cardinality,
        }
    }
}

pub struct OptimizedFilter<'a> {
    /// At least one of those conditions should match
    pub should: Option<Vec<OptimizedCondition<'a>>>,
    /// All conditions must match
    pub must: Option<Vec<OptimizedCondition<'a>>>,
    /// All conditions must NOT match
    pub must_not: Option<Vec<OptimizedCondition<'a>>>,
}

pub fn check_optimized_filter(filter: &OptimizedFilter, point_id: PointOffsetType) -> bool {
    check_should(&filter.should, point_id)
        && check_must(&filter.must, point_id)
        && check_must_not(&filter.must_not, point_id)
}

fn check_condition(condition: &OptimizedCondition, point_id: PointOffsetType) -> bool {
    match condition {
        OptimizedCondition::Filter(filter) => check_optimized_filter(&filter.filter, point_id),
        OptimizedCondition::Checker(checker) => (checker.checker)(point_id),
    }
}

fn check_should(should: &Option<Vec<OptimizedCondition>>, point_id: PointOffsetType) -> bool {
    let check = |condition| check_condition(condition, point_id);
    match should {
        None => true,
        Some(conditions) => conditions.iter().any(check),
    }
}

fn check_must(must: &Option<Vec<OptimizedCondition>>, point_id: PointOffsetType) -> bool {
    let check = |condition| check_condition(condition, point_id);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check),
    }
}

fn check_must_not(must: &Option<Vec<OptimizedCondition>>, point_id: PointOffsetType) -> bool {
    let check = |condition| !check_condition(condition, point_id);
    match must {
        None => true,
        Some(conditions) => conditions.iter().all(check),
    }
}
