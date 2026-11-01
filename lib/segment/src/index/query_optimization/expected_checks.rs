/// How many points the consumer of a filter is expected to check against it, and how
/// it checks them. Decides whether a condition's posting list is read into a bitset
/// up front instead of checking each point through the field index.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedChecks {
    pub count: usize,
    pub consumer: CheckConsumer,
}

#[derive(Clone, Copy, Debug)]
pub enum CheckConsumer {
    /// Plain search: every candidate of the primary clauses is checked, in large batches.
    Plain,
    /// Graph search: a few neighbours are checked at a time while the graph is walked.
    Graph,
}

/// Posting-list entries worth reading per avoided check when a check is a disk read:
/// an entry is 4 sequential bytes, a check on a cold page cache is a random read.
const DISK_POSTING_TO_CHECKS_RATIO: usize = 64;

/// Same, when a check is a memory lookup on the plain path.
const PLAIN_POSTING_TO_CHECKS_RATIO: usize = 4;

/// Same, when a check is a memory lookup in graph search. Each check there is cheap
/// relative to the vector scoring around it, so only lists no longer than the checks pay.
const GRAPH_POSTING_TO_CHECKS_RATIO: usize = 1;

impl ExpectedChecks {
    pub fn plain(count: usize) -> Self {
        Self {
            count,
            consumer: CheckConsumer::Plain,
        }
    }

    /// Checks of a graph search with beam `ef` over a graph of degree `m0` when the
    /// filter keeps `cardinality` of `available` points: every visited point checks its
    /// `m0` neighbours, and about `available / cardinality` points are visited per
    /// accepted one.
    pub fn graph(ef: usize, m0: usize, cardinality: usize, available: usize) -> Self {
        let count = ef
            .saturating_mul(m0)
            .saturating_mul(available)
            .checked_div(cardinality)
            .map_or(available, |visited| visited.min(available));
        Self {
            count,
            consumer: CheckConsumer::Graph,
        }
    }

    /// Largest posting list worth reading into a bitset instead of `count` per-point checks.
    pub fn max_posting_len(self, checks_read_disk: bool) -> usize {
        let ratio = match (checks_read_disk, self.consumer) {
            (true, CheckConsumer::Plain | CheckConsumer::Graph) => DISK_POSTING_TO_CHECKS_RATIO,
            (false, CheckConsumer::Plain) => PLAIN_POSTING_TO_CHECKS_RATIO,
            (false, CheckConsumer::Graph) => GRAPH_POSTING_TO_CHECKS_RATIO,
        };
        self.count.saturating_mul(ratio)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_checks() {
        // 1% selectivity: ef=100, m0=32 visits 100 * 32 * 100 points, capped at the total.
        assert_eq!(
            ExpectedChecks::graph(100, 32, 10_000, 1_000_000).count,
            320_000
        );
        assert_eq!(
            ExpectedChecks::graph(100, 32, 100, 1_000_000).count,
            1_000_000
        );
        assert_eq!(ExpectedChecks::graph(100, 32, 0, 1_000).count, 1_000);
    }

    #[test]
    fn test_ratios() {
        let plain = ExpectedChecks::plain(1000);
        assert_eq!(plain.max_posting_len(true), 64_000);
        assert_eq!(plain.max_posting_len(false), 4_000);
        let graph = ExpectedChecks::graph(10, 10, 100, 1000);
        assert_eq!(graph.max_posting_len(false), 1000);
    }
}
