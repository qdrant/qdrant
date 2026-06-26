use common::condition_checker::ConditionChecker;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::condition_checker::ConditionCheckerEnum;

pub struct OptimizedFilter<'a> {
    /// At least one of those conditions should match, if not empty.
    pub should: Vec<ConditionCheckerEnum<'a>>,
    /// At least minimum amount of given conditions should match
    pub min_should: Vec<ConditionCheckerEnum<'a>>,
    pub min_should_count: usize,
    /// All conditions must match
    pub must: Vec<ConditionCheckerEnum<'a>>,
    /// All conditions must NOT match
    pub must_not: Vec<ConditionCheckerEnum<'a>>,
}

impl<'a> OptimizedFilter<'a> {
    /// A filter that matches a point iff the single given checker matches it.
    pub fn from_checker(checker: ConditionCheckerEnum<'a>) -> Self {
        OptimizedFilter {
            should: Vec::new(),
            min_should: Vec::new(),
            min_should_count: 0,
            must: vec![checker],
            must_not: Vec::new(),
        }
    }
}

impl ConditionChecker for OptimizedFilter<'_> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        let OptimizedFilter {
            should,
            min_should,
            min_should_count,
            must,
            must_not,
        } = self;

        // `should`: at least one matches, if not empty.
        if !should.is_empty()
            && !should
                .iter()
                .try_any(|condition| condition.check(point_id))?
        {
            return Ok(false);
        }

        // `min_should`: at least `min_count` match.
        let mut remaining = *min_should_count;
        let mut min_should_iter = min_should.iter();
        while remaining > 0 {
            let Some(condition) = min_should_iter.next() else {
                // Not enough conditions to match `min_count`
                return Ok(false);
            };
            if condition.check(point_id)? {
                remaining -= 1;
            }
        }

        // `must`: all match.
        for condition in must {
            if !condition.check(point_id)? {
                return Ok(false);
            }
        }

        // `must_not`: none match.
        for condition in must_not {
            if condition.check(point_id)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
