use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select};
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::condition_checker::ConditionCheckerEnum;

pub struct OptimizedFilter<'a> {
    /// At least one of those conditions should match, if not empty.
    should: Vec<ConditionCheckerEnum<'a>>,
    /// At least minimum amount of given conditions should match
    min_should: Vec<ConditionCheckerEnum<'a>>,
    min_should_count: usize,
    /// All conditions must match
    must: Vec<ConditionCheckerEnum<'a>>,
    /// All conditions must NOT match
    must_not: Vec<ConditionCheckerEnum<'a>>,

    scratch: Vec<usize>,
}

impl<'a> OptimizedFilter<'a> {
    pub fn new(
        should: Vec<ConditionCheckerEnum<'a>>,
        min_should: Vec<ConditionCheckerEnum<'a>>,
        min_should_count: usize,
        must: Vec<ConditionCheckerEnum<'a>>,
        must_not: Vec<ConditionCheckerEnum<'a>>,
    ) -> Self {
        OptimizedFilter {
            should,
            min_should,
            min_should_count,
            must,
            must_not,
            scratch: Vec::new(),
        }
    }

    /// A filter that matches a point iff the single given checker matches it.
    pub fn from_checker(checker: ConditionCheckerEnum<'a>) -> Self {
        Self::new(Vec::new(), Vec::new(), 0, vec![checker], Vec::new())
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
            scratch: _,
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

    fn check_batched<K: CheckItem>(
        &mut self,
        ids: &mut [K],
        select: Select,
        rest: Rest,
    ) -> OperationResult<usize> {
        let OptimizedFilter {
            should,
            min_should,
            min_should_count,
            must,
            must_not,
            scratch,
        } = self;
        match select {
            Select::Matches => {
                // must ∩ must_not ∩ should ∩ min_should

                let mut n = all_of(must, ids, Select::Matches, rest)?;
                n = all_of(must_not, &mut ids[..n], Select::NonMatches, rest)?;
                if !should.is_empty() {
                    // Empty `should` means "no constraint", not an empty disjunction.
                    n = any_of(should, &mut ids[..n], Select::Matches, rest)?;
                }
                n = at_least(
                    min_should,
                    &mut ids[..n],
                    *min_should_count,
                    Select::Matches,
                    rest,
                    scratch,
                )?;
                Ok(n)
            }
            Select::NonMatches => {
                // ¬must ∪ ¬must_not ∪ ¬should ∪ ¬min_should

                let min_should_rest = rest;
                let should_rest = min_should_rest.keep_if(*min_should_count > 0);
                let must_not_rest = should_rest.keep_if(!should.is_empty());
                let must_rest = must_not_rest.keep_if(!must_not.is_empty());

                let mut n = any_of(must, ids, Select::NonMatches, must_rest)?;
                n += any_of(must_not, &mut ids[n..], Select::Matches, must_not_rest)?;
                if !should.is_empty() {
                    n += all_of(should, &mut ids[n..], Select::NonMatches, should_rest)?;
                }
                let threshold = (min_should.len() + 1).saturating_sub(*min_should_count);
                n += at_least(
                    min_should,
                    &mut ids[n..],
                    threshold,
                    Select::NonMatches,
                    min_should_rest,
                    scratch,
                )?;
                Ok(n)
            }
        }
    }
}

/// Select ids that satisfy all of the conditions.
fn all_of<C: ConditionChecker, K: CheckItem>(
    conditions: &mut [C],
    ids: &mut [K],
    select: Select,
    rest: Rest,
) -> Result<usize, C::Error> {
    // Each step narrows the matching (left) zone.
    //
    // Input  │                                                                │
    //        └────────────────────────────────────────────────────────────────┘
    // Step A │ A                                                    │ ¬A      │
    //        └──────────────────────────────────────────────────────┴─────────┘
    // Step B │ A ∩ B                                       │ A ∩ ¬B │
    //        └─────────────────────────────────────────────┴────────┘
    // Step C │ A ∩ B ∩ C                      │ A ∩ B ∩ ¬C │
    //        └────────────────────────────────┴────────────┘
    // Step D │ A ∩ B ∩ C ∩ D │ A ∩ B ∩ C ∩ ¬D │
    //        └───────────────┴────────────────┘
    // Result │ A ∩ B ∩ C ∩ D │
    //        └───────────────┘
    let mut n = ids.len();
    for condition in conditions {
        n = condition.check_batched(&mut ids[..n], select, rest)?;
    }
    Ok(n)
}

/// Select ids that satisfy any of the conditions.
fn any_of<C: ConditionChecker, K: CheckItem>(
    conditions: &mut [C],
    ids: &mut [K],
    select: Select,
    rest: Rest,
) -> Result<usize, C::Error> {
    // Each step scans only the ids rejected by the previous step.
    //
    // Input  │                                                                │
    //        └────────────────────────────────────────────────────────────────┘
    // Step A │ A │ ¬A                                                         │
    //        └───┴────────────────────────────────────────────────────────────┘
    // Step B     │ ¬A ∩ B │ ¬A ∩ ¬B                                           │
    //            └────────┴───────────────────────────────────────────────────┘
    // Step C              │ ¬A ∩ ¬B ∩ C │ ¬A ∩ ¬B ∩ ¬C                        │
    //                     └─────────────┴─────────────────────────────────────┘
    // Step D                            │¬A ∩ ¬B ∩ ¬C ∩ D │ ¬A ∩ ¬B ∩ ¬C ∩ ¬D │
    //                                   └─────────────────┴───────────────────┘
    // Output │ A ∪ B ∪ C ∪ D                              │
    //        └────────────────────────────────────────────┘
    let mut n = 0;
    let last = conditions.len().wrapping_sub(1);
    for (i, condition) in conditions.iter_mut().enumerate() {
        // Only the last conditions's rejects are final.
        let condition_rest = rest.keep_if(i != last);
        n += condition.check_batched(&mut ids[n..], select, condition_rest)?;
    }
    Ok(n)
}

/// Selects ids that satisfy at least `threshold` conditions.
fn at_least<C: ConditionChecker, K: CheckItem>(
    conditions: &mut [C],
    ids: &mut [K],
    threshold: usize,
    select: Select,
    rest: Rest,
    scratch: &mut Vec<usize>,
) -> Result<usize, C::Error> {
    if threshold == 0 {
        return Ok(ids.len());
    }
    if threshold > conditions.len() {
        return Ok(0);
    }

    // Counting sort: ids are kept in blocks by descending number of satisfied
    // children; `ids[..scratch[0]]` satisfy at least `threshold` of them,
    // `ids[scratch[i - 1]..scratch[i]]` satisfy exactly `threshold - i`.
    // A satisfied id moves to the front of its block, joining the block above.
    let m = ids.len();
    scratch.clear();
    scratch.resize(threshold, 0);
    let last = conditions.len() - 1;
    for (c, condition) in conditions.iter_mut().enumerate() {
        // Only the last condition's rejects are final (see `any_of`).
        let child_rest = rest.keep_if(c != last);
        for i in 0..threshold {
            let start = scratch[i];
            let end = if i + 1 < threshold { scratch[i + 1] } else { m };
            scratch[i] += condition.check_batched(&mut ids[start..end], select, child_rest)?;
        }
    }
    Ok(scratch[0])
}
