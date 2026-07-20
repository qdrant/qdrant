use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;

pub type ConditionCheckerFn<'a> = Box<dyn Fn(PointOffsetType) -> OperationResult<bool> + 'a>;

pub enum OptimizedCondition<'a> {
    Checker(ConditionCheckerFn<'a>),
    /// Nested filter
    Filter(OptimizedFilter<'a>),
}

pub struct OptimizedMinShould<'a> {
    pub conditions: Vec<OptimizedCondition<'a>>,
    pub min_count: usize,
}

pub struct OptimizedFilter<'a> {
    /// At least one of those conditions should match
    pub should: Option<Vec<OptimizedCondition<'a>>>,
    /// At least minimum amount of given conditions should match
    pub min_should: Option<OptimizedMinShould<'a>>,
    /// All conditions must match
    pub must: Option<Vec<OptimizedCondition<'a>>>,
    /// All conditions must NOT match
    pub must_not: Option<Vec<OptimizedCondition<'a>>>,
}

pub fn check_optimized_filter(
    filter: &OptimizedFilter,
    point_id: PointOffsetType,
) -> OperationResult<bool> {
    Ok(check_should(&filter.should, point_id)?
        && check_min_should(&filter.min_should, point_id)?
        && check_must(&filter.must, point_id)?
        && check_must_not(&filter.must_not, point_id)?)
}

pub fn check_condition(
    condition: &OptimizedCondition,
    point_id: PointOffsetType,
) -> OperationResult<bool> {
    match condition {
        OptimizedCondition::Filter(filter) => check_optimized_filter(filter, point_id),
        OptimizedCondition::Checker(checker) => checker(point_id),
    }
}

fn check_should(
    should: &Option<Vec<OptimizedCondition>>,
    point_id: PointOffsetType,
) -> OperationResult<bool> {
    match should {
        None => Ok(true),
        Some(conditions) => {
            for condition in conditions {
                if check_condition(condition, point_id)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn check_min_should(
    min_should: &Option<OptimizedMinShould>,
    point_id: PointOffsetType,
) -> OperationResult<bool> {
    match min_should {
        None => Ok(true),
        Some(OptimizedMinShould {
            conditions,
            min_count,
        }) => {
            let mut count = 0;
            for condition in conditions {
                if check_condition(condition, point_id)? {
                    count += 1;
                    if count == *min_count {
                        return Ok(true);
                    }
                }
            }
            Ok(count == *min_count)
        }
    }
}

fn check_must(
    must: &Option<Vec<OptimizedCondition>>,
    point_id: PointOffsetType,
) -> OperationResult<bool> {
    match must {
        None => Ok(true),
        Some(conditions) => {
            for condition in conditions {
                if !check_condition(condition, point_id)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn check_must_not(
    must_not: &Option<Vec<OptimizedCondition>>,
    point_id: PointOffsetType,
) -> OperationResult<bool> {
    match must_not {
        None => Ok(true),
        Some(conditions) => {
            for condition in conditions {
                if check_condition(condition, point_id)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}
