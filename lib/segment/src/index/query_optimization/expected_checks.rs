/// How the consumer of a filter checks points against it. Decides whether a condition's
/// posting list is read into a point set up front instead of checking each point through
/// the field index.
#[derive(Clone, Copy, Debug)]
pub enum ExpectedChecks {
    /// About `count` checks made in batches (plain path): on a cold index, the disk reads
    /// of one batch overlap, so a check costs a share of the read throughput.
    Batched { count: usize },
    /// Checks made one at a time, each blocking the caller until it returns (graph walk,
    /// cardinality sampling): on a cold index, every check is a full disk round trip.
    Serial,
}

/// Posting-list entries worth reading per avoided batched check on a cold index
/// (`memory: cold`): an entry is 4 sequential bytes, a check is a random disk read.
const COLD_POSTING_TO_BATCHED_CHECKS_RATIO: usize = 64;

/// Same, for an index held in RAM (cached or pinned).
const RAM_POSTING_TO_BATCHED_CHECKS_RATIO: usize = 4;

impl ExpectedChecks {
    /// Whether a posting list of `posting_len` points is worth reading up front instead
    /// of checking points through an index that is cold (`memory: cold`) or not.
    ///
    /// A serial check on a cold index is a disk round trip of ~100 µs, worth tens of
    /// thousands of sequentially read posting entries, more than any posting list in a
    /// segment: always read. On an index held in RAM a serial check is a memory lookup,
    /// cheap next to the vector scoring around it: never read.
    pub fn reads_posting(self, posting_len: usize, cold_index: bool) -> bool {
        match self {
            ExpectedChecks::Batched { count } => {
                let ratio = if cold_index {
                    COLD_POSTING_TO_BATCHED_CHECKS_RATIO
                } else {
                    RAM_POSTING_TO_BATCHED_CHECKS_RATIO
                };
                posting_len <= count.saturating_mul(ratio)
            }
            ExpectedChecks::Serial => cold_index,
        }
    }

    /// Number of checks to size a point set for, in a segment of `total_points` points.
    /// Serial consumers have no count; assume many checks, so sets prefer bitsets.
    pub fn point_set_checks(self, total_points: usize) -> usize {
        match self {
            ExpectedChecks::Batched { count } => count,
            ExpectedChecks::Serial => total_points,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batched_ratios() {
        let batched = ExpectedChecks::Batched { count: 1000 };
        assert!(batched.reads_posting(64_000, true));
        assert!(!batched.reads_posting(64_001, true));
        assert!(batched.reads_posting(4_000, false));
        assert!(!batched.reads_posting(4_001, false));
    }

    #[test]
    fn test_serial_reads_only_cold() {
        assert!(ExpectedChecks::Serial.reads_posting(usize::MAX, true));
        assert!(!ExpectedChecks::Serial.reads_posting(0, false));
        assert_eq!(ExpectedChecks::Serial.point_set_checks(1_000), 1_000);
    }
}
