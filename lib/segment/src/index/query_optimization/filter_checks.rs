//! How much filter evaluation a search would do on its own, to decide whether resolving a
//! filter once for several searches is cheaper (see [`worth_sharing`]).
//!
//! Costs are counted in filter checks of one point. Checking a point costs about the same
//! whether it happens up front or during the search, so the cost of the filter itself (a
//! `nested` clause or a single indexed `match`) cancels out of the comparison.

use std::ops::Add;

/// Filter checks one search call makes when it evaluates its filter itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterChecks {
    /// Whether the search checks every candidate of the filter's primary clauses, as a
    /// plain search does. Counted once per search call: it is shared by its query vectors.
    pub all_candidates: bool,
    /// Points checked one at a time, besides: those a graph search visits, the samples of a
    /// cardinality estimation, the points an inverted index scores.
    pub lazy: usize,
}

impl FilterChecks {
    pub const NONE: Self = Self {
        all_candidates: false,
        lazy: 0,
    };

    pub const ALL_CANDIDATES: Self = Self {
        all_candidates: true,
        lazy: 0,
    };

    pub const fn lazy(checks: usize) -> Self {
        Self {
            all_candidates: false,
            lazy: checks,
        }
    }

    /// Number of checks, given the `materialization_cost` of the filter: checking every
    /// candidate costs as much as materializing it.
    pub fn cost(self, materialization_cost: usize) -> usize {
        let Self {
            all_candidates,
            lazy,
        } = self;
        let full = if all_candidates {
            materialization_cost
        } else {
            0
        };
        full.saturating_add(lazy)
    }
}

impl Add for FilterChecks {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            all_candidates: self.all_candidates || other.all_candidates,
            lazy: self.lazy.saturating_add(other.lazy),
        }
    }
}

/// Whether materializing a filter once, at `materialization_cost` filter checks, costs less
/// than every search of `uses` evaluating it on its own.
///
/// A search that checks every candidate anyway makes materializing free for the others, so
/// two such searches always share. Searches that only check the points they visit share
/// only when they would check more points together than materializing does.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_two_plain_searches_share() {
        let uses = [FilterChecks::ALL_CANDIDATES; 2];
        assert!(worth_sharing(1_000_000, uses));
    }

    #[test]
    fn test_one_plain_search_does_not_share() {
        assert!(!worth_sharing(100, [FilterChecks::ALL_CANDIDATES]));
    }

    #[test]
    fn test_plain_search_shares_with_graph_search() {
        let uses = [FilterChecks::ALL_CANDIDATES, FilterChecks::lazy(1)];
        assert!(worth_sharing(1_000_000, uses));
    }

    #[test]
    fn test_graph_searches_share_below_their_checks() {
        let uses = [FilterChecks::lazy(3_200); 6];
        assert!(worth_sharing(19_000, uses));
        assert!(!worth_sharing(19_200, uses));
        assert!(!worth_sharing(30_000, uses));
    }

    #[test]
    fn test_searches_without_checks_do_not_share() {
        assert!(!worth_sharing(0, [FilterChecks::NONE; 3]));
        assert!(!worth_sharing(10, [FilterChecks::NONE; 3]));
    }

    #[test]
    fn test_add() {
        let sum = FilterChecks::ALL_CANDIDATES + FilterChecks::lazy(5) + FilterChecks::lazy(7);
        assert_eq!(
            sum,
            FilterChecks {
                all_candidates: true,
                lazy: 12,
            }
        );
        assert_eq!(sum.cost(100), 112);
    }
}
