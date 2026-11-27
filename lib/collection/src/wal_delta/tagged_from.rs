/// The first WAL record from which every record has a clock tag
///
/// A WAL delta can't include records without a clock tag, so the delta scan stops at the first
/// untagged record it meets. A WAL delta can start no lower than this record.
///
/// It can be higher than the real boundary, never lower:
/// - writing an untagged record moves it past that record
/// - if the end of the WAL is dropped, it can stay past the end, until new records reach it
#[derive(Debug)]
pub struct TaggedFrom {
    record: u64,
}

impl TaggedFrom {
    pub fn new(record: u64) -> Self {
        Self { record }
    }

    /// The record `op_num` has no clock tag
    pub fn mark_untagged(&mut self, op_num: u64) {
        let next = op_num + 1;

        if next > self.record {
            self.record = next;
        }
    }

    /// How many records a WAL delta scan can read backwards from the end of the WAL
    ///
    /// The scan stops at the start of the WAL or before this record, whichever comes first.
    /// `first_index` is the first record still in the WAL, including acknowledged records, and
    /// `num_entries` is how many records it has.
    pub fn tagged_records(&self, first_index: u64, num_entries: u64) -> u64 {
        let end = first_index + num_entries;
        end.saturating_sub(self.record.max(first_index))
    }
}

impl Default for TaggedFrom {
    /// Every record of a new, empty WAL starting at record 0 is tagged
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_untagged_only_moves_forward() {
        let mut tagged_from = TaggedFrom::new(10);

        tagged_from.mark_untagged(15);
        assert_eq!(tagged_from.record, 16);

        tagged_from.mark_untagged(12);
        assert_eq!(tagged_from.record, 16);
    }

    #[test]
    fn test_tagged_records() {
        // WAL has records 100..=199
        let first_index = 100;
        let num_entries = 100;

        // Untagged record in the WAL, at 130
        let tagged_from = TaggedFrom::new(131);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 69);

        // Untagged record already truncated from the WAL
        let tagged_from = TaggedFrom::new(50);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 100);

        // Untagged record is the last one
        let tagged_from = TaggedFrom::new(200);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 0);

        // The end of the WAL was dropped after an untagged record
        let tagged_from = TaggedFrom::new(250);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 0);

        // Empty WAL
        let tagged_from = TaggedFrom::new(0);
        assert_eq!(tagged_from.tagged_records(100, 0), 0);
    }
}
