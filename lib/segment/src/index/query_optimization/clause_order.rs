//! Which side of an [`OptimizedFilter`](super::optimized_filter::OptimizedFilter) is
//! checked first.

/// Which side of a filter is checked first: `must` and `must_not`, or `should` and
/// `min_should`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClauseOrder {
    MustFirst,
    ShouldFirst,
}

/// One side of a filter.
#[derive(Clone, Copy, Debug)]
pub struct Side {
    /// Number of conditions the side checks, as its cost.
    pub conditions: usize,
    /// Estimated share of points that pass the side, from 0 to 1.
    pub passing: f64,
}

impl ClauseOrder {
    /// Check first the side that rejects the most points for its cost: the lowest
    /// number of conditions over the share of points rejected. For independent
    /// sides, this order checks the fewest conditions per point.
    pub fn by_rejection(must: Side, should: Side) -> Self {
        let rank = |side: Side| {
            if side.conditions == 0 {
                return 0.0;
            }
            // Infinite if the side rejects no point
            side.conditions as f64 / (1.0 - side.passing.clamp(0.0, 1.0))
        };
        if rank(should) < rank(must) {
            ClauseOrder::ShouldFirst
        } else {
            ClauseOrder::MustFirst
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side(conditions: usize, passing: f64) -> Side {
        Side {
            conditions,
            passing,
        }
    }

    #[test]
    fn selective_side_first() {
        // A `must` passing 1% before a `should` of two conditions passing 75%.
        let order = ClauseOrder::by_rejection(side(1, 0.01), side(2, 0.75));
        assert_eq!(order, ClauseOrder::MustFirst);

        // A `should` of two conditions passing 2% before a `must` passing 90%.
        let order = ClauseOrder::by_rejection(side(1, 0.9), side(2, 0.02));
        assert_eq!(order, ClauseOrder::ShouldFirst);

        // One condition rejecting 30% before ten rejecting 95% together.
        let order = ClauseOrder::by_rejection(side(1, 0.7), side(10, 0.05));
        assert_eq!(order, ClauseOrder::MustFirst);
    }
}
