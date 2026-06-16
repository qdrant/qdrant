use common::condition_checker::ConditionChecker;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};

pub type DynConditionChecker<'a> = Box<dyn ConditionChecker<Error = OperationError> + 'a>;

pub enum OptimizedCondition<'a> {
    Checker(DynConditionChecker<'a>),
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

impl ConditionChecker for OptimizedFilter<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        let OptimizedFilter {
            should,
            min_should,
            must,
            must_not,
        } = self;

        // `should`: at least one matches.
        if let Some(conditions) = should
            && !conditions
                .iter()
                .try_any(|condition| condition.check(point_id))?
        {
            return Ok(false);
        }

        // `min_should`: at least `min_count` match.
        if let Some(min_should) = min_should {
            let OptimizedMinShould {
                conditions,
                min_count,
            } = min_should;
            let mut matched = 0;

            for condition in conditions {
                if condition.check(point_id)? {
                    matched += 1;
                    if matched == *min_count {
                        break;
                    }
                }
            }
            if matched < *min_count {
                return Ok(false);
            }
        }

        // `must`: all match.
        if let Some(conditions) = must {
            for condition in conditions {
                if !condition.check(point_id)? {
                    return Ok(false);
                }
            }
        }

        // `must_not`: none match.
        if let Some(conditions) = must_not {
            for condition in conditions {
                if condition.check(point_id)? {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

impl ConditionChecker for OptimizedCondition<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        match self {
            OptimizedCondition::Filter(filter) => filter.check(point_id),
            OptimizedCondition::Checker(checker) => checker.check(point_id),
        }
    }

    fn check_infallible(&self, point_id: PointOffsetType) -> bool {
        match self {
            OptimizedCondition::Filter(filter) => filter.check_infallible(point_id),
            OptimizedCondition::Checker(checker) => checker.check_infallible(point_id),
        }
    }
}
