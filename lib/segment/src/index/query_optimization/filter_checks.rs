//! Cost model for sharing one filter between several searches of a batch (see
//! [`worth_sharing`]).
//!
//! All costs are counted in filter checks: one check is one point tested against the filter.
//! A check costs about the same whether it happens while materializing the filter or during a
//! search, so the price of the filter itself (a `nested` clause or a single indexed `match`)
//! cancels out of the comparison.

use std::cell::OnceCell;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::CardinalityEstimation;

/// The cardinality estimation of a filter, computed the first time an index asks for it.
///
/// Whether a search evaluates its filter on every candidate can be known without the
/// estimation: a plain index, an exact search or a search that doesn't run never need it. An
/// index that does need it calls [`Self::get`]; when no index does, the estimation is never
/// computed.
pub struct LazyCardinality<'a> {
    compute: Box<dyn Fn() -> OperationResult<CardinalityEstimation> + 'a>,
    value: OnceCell<CardinalityEstimation>,
}

impl<'a> LazyCardinality<'a> {
    pub fn new(compute: impl Fn() -> OperationResult<CardinalityEstimation> + 'a) -> Self {
        Self {
            compute: Box::new(compute),
            value: OnceCell::new(),
        }
    }

    /// An estimation that is already known.
    pub fn known(estimation: CardinalityEstimation) -> LazyCardinality<'static> {
        LazyCardinality::new(move || Ok(estimation.clone()))
    }

    /// The estimation, computed on the first call.
    pub fn get(&self) -> OperationResult<&CardinalityEstimation> {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }
        let value = (self.compute)()?;
        Ok(self.value.get_or_init(|| value))
    }
}

/// How a search uses its filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterChecks {
    /// The search evaluates the filter on every candidate before scoring, e.g. a plain or
    /// exact search, or a filter that matches fewer points than the full-scan threshold.
    ///
    /// This costs the same as materializing the filter. It is paid once per search call, no
    /// matter how many query vectors the call has.
    Full,
    /// The search checks points against the filter one at a time, as it visits them, e.g. a
    /// graph search or an inverted index.
    Lazy {
        /// Expected number of checks, over all query vectors of the search call. Zero if the
        /// search doesn't run, e.g. `top == 0`.
        expected_checks: usize,
    },
}

impl FilterChecks {
    /// Number of filter checks this search makes, given the cost of materializing its filter.
    pub fn cost(self, materialization_cost: usize) -> usize {
        match self {
            FilterChecks::Full => materialization_cost,
            FilterChecks::Lazy { expected_checks } => expected_checks,
        }
    }
}

/// Whether materializing a filter once is cheaper than letting every search in `uses` evaluate
/// it on its own.
///
/// One [`FilterChecks::Full`] search already costs as much as materializing, so two of them
/// always share, and so does one of them with any lazy search. Lazy searches alone share only
/// when their checks add up to more than `materialization_cost`.
pub fn worth_sharing(
    materialization_cost: usize,
    uses: impl IntoIterator<Item = FilterChecks>,
) -> bool {
    let unshared = uses
        .into_iter()
        .map(|checks| checks.cost(materialization_cost))
        .fold(0usize, usize::saturating_add);
    materialization_cost < unshared
}

/// Whether [`worth_sharing`] is false for `uses` whatever the materialization cost is, so the
/// cost needn't be computed.
///
/// A [`FilterChecks::Full`] search costs as much as materializing, so fewer than two of them
/// never beat it, and searches without lazy checks add nothing.
pub fn never_worth_sharing(uses: &[FilterChecks]) -> bool {
    let full = uses
        .iter()
        .filter(|checks| matches!(checks, FilterChecks::Full))
        .count();
    let lazy_checks = uses
        .iter()
        .map(|checks| match checks {
            FilterChecks::Full => 0,
            FilterChecks::Lazy { expected_checks } => *expected_checks,
        })
        .fold(0usize, usize::saturating_add);
    full < 2 && lazy_checks == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn lazy(expected_checks: usize) -> FilterChecks {
        FilterChecks::Lazy { expected_checks }
    }

    #[test]
    fn test_two_full_searches_share() {
        assert!(worth_sharing(1_000_000, [FilterChecks::Full; 2]));
    }

    #[test]
    fn test_one_full_search_does_not_share() {
        assert!(!worth_sharing(100, [FilterChecks::Full]));
    }

    #[test]
    fn test_full_search_shares_with_lazy_search() {
        assert!(worth_sharing(1_000_000, [FilterChecks::Full, lazy(1)]));
    }

    #[test]
    fn test_lazy_searches_share_below_their_checks() {
        let uses = [lazy(3_200); 6];
        assert!(worth_sharing(19_000, uses));
        assert!(!worth_sharing(19_200, uses));
        assert!(!worth_sharing(30_000, uses));
    }

    #[test]
    fn test_searches_without_checks_do_not_share() {
        assert!(!worth_sharing(0, [lazy(0); 3]));
        assert!(!worth_sharing(10, [lazy(0); 3]));
    }
}
