use common::types::PointOffsetType;

/// Represents how values for a single point are stored.
///
/// - `Single(N)`: The point has exactly one value, stored inline to avoid
///   container indirection and save memory.
/// - `Slice { start, count }`: The point has zero or multiple (2+) values,
///   stored as a contiguous slice in the shared `values_container`.
///   When `count == 0`, the point has no values (also used after removal).
#[derive(Debug, Clone)]
enum PointValueEntry<N> {
    /// Exactly one value, stored inline for efficiency.
    Single(N),
    /// Zero or multiple values stored in the external `values_container`.
    /// `start` is the index of the first value, `count` is the number of values.
    /// `u32` is used instead of `usize` because it's more RAM efficient —
    /// we can expect that we will never have more than 4 billion values per segment.
    Slice { start: u32, count: u32 },
}

impl<N> Default for PointValueEntry<N> {
    /// Default represents "no values" — an empty slice.
    fn default() -> Self {
        PointValueEntry::Slice { start: 0, count: 0 }
    }
}

/// Flattened points-to-values map.
///
/// An analogue of `Vec<Vec<N>>` but more RAM efficient:
/// - Points with exactly **one** value store it inline in the entry (no container overhead).
/// - Points with **zero** or **multiple** values store them in a shared, contiguous `Vec<N>`.
///
/// This structure doesn't support adding new values, only removing.
/// It's used in immutable field indices like `ImmutableMapIndex`, `ImmutableNumericIndex`, etc.
#[derive(Debug, Clone, Default)]
pub struct ImmutablePointToValues<N: Default> {
    /// Per-point entry describing where (or how) its values are stored.
    /// Indexed by `PointOffsetType`.
    point_entries: Vec<PointValueEntry<N>>,
    /// Shared container holding values for points that have multiple values.
    /// Single-value points are **not** stored here — they live inline in `point_entries`.
    values_container: Vec<N>,
}

impl<N: Default> ImmutablePointToValues<N> {
    pub fn new(src: Vec<Vec<N>>) -> Self {
        let mut point_entries = Vec::with_capacity(src.len());

        // Only values from multi-value points go into the container.
        let container_capacity = src
            .iter()
            .map(|values| values.len())
            .filter(|&size| size > 1)
            .sum();
        let mut values_container = Vec::with_capacity(container_capacity);

        for values in src {
            match values.len() {
                // Zero values — store inline
                0 => {
                    point_entries.push(PointValueEntry::default());
                }
                // Single value — store inline, skip container entirely
                1 => {
                    let value = values.into_iter().next().expect("length checked above");
                    point_entries.push(PointValueEntry::Single(value));
                }
                // Multiple values — store in container, record slice location
                2.. => {
                    let start = values_container.len() as u32;
                    let count = values.len() as u32;
                    point_entries.push(PointValueEntry::Slice { start, count });
                    values_container.extend(values);
                }
            }
        }

        Self {
            point_entries,
            values_container,
        }
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        mut check_fn: impl FnMut(&N) -> bool,
    ) -> bool {
        let Some(entry) = self.point_entries.get(idx as usize) else {
            return false;
        };

        match entry {
            PointValueEntry::Single(v) => check_fn(v),
            PointValueEntry::Slice { start, count } => {
                let range = *start as usize..(*start + *count) as usize;
                if let Some(values) = self.values_container.get(range) {
                    values.iter().any(check_fn)
                } else {
                    false
                }
            }
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<impl Iterator<Item = &N> + '_> {
        let entry = self.point_entries.get(idx as usize)?;
        match entry {
            PointValueEntry::Single(v) => Some(std::slice::from_ref(v).iter()),
            PointValueEntry::Slice { start, count } => {
                let range = *start as usize..(*start + *count) as usize;
                Some(self.values_container[range].iter())
            }
        }
    }

    pub fn get_values_count(&self, idx: PointOffsetType) -> Option<usize> {
        let entry = self.point_entries.get(idx as usize)?;
        match entry {
            PointValueEntry::Single(_) => Some(1),
            PointValueEntry::Slice { start: _, count } => Some(*count as usize),
        }
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> Vec<N> {
        if self.point_entries.len() <= idx as usize {
            return Default::default();
        }

        // Replace the entry with default (empty slice) and take ownership of the old one.
        let removed_entry = std::mem::take(&mut self.point_entries[idx as usize]);

        match removed_entry {
            PointValueEntry::Single(v) => vec![v],
            PointValueEntry::Slice { start, count } => {
                let mut result = Vec::with_capacity(count as usize);
                for i in start..(start + count) {
                    // Deleted values still occupy RAM in the container, but optimizers
                    // will rebuild the index to actually reclaim memory.
                    let value = std::mem::take(&mut self.values_container[i as usize]);
                    result.push(value);
                }
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: assert that `get_values` for every index matches the expected `values` slice.
    fn check_values(point_to_values: &ImmutablePointToValues<i32>, values: &[Vec<i32>]) {
        for (idx, expected) in values.iter().enumerate() {
            let actual: Option<Vec<_>> = point_to_values
                .get_values(idx as PointOffsetType)
                .map(|i| i.copied().collect());
            assert_eq!(actual, Some(expected.clone()));
        }
    }

    #[test]
    fn test_immutable_point_to_values_remove() {
        let mut values = vec![
            vec![0, 1, 2, 3, 4],
            vec![5, 6, 7, 8, 9],
            vec![0, 1, 2, 3, 4],
            vec![5, 6, 7, 8, 9],
            vec![10, 11, 12],
            vec![],
            vec![13],
            vec![14, 15],
        ];

        let mut point_to_values = ImmutablePointToValues::new(values.clone());

        check_values(&point_to_values, values.as_slice());

        point_to_values.remove_point(0);
        values[0].clear();

        check_values(&point_to_values, values.as_slice());

        point_to_values.remove_point(3);
        values[3].clear();

        check_values(&point_to_values, values.as_slice());
    }

    #[test]
    fn test_single_value_stored_inline() {
        // Points with exactly one value should be stored as Single entries,
        // and should NOT appear in the values_container.
        let src = vec![
            vec![42],   // single — inline
            vec![1, 2], // multi — in container
            vec![99],   // single — inline
            vec![],     // empty — inline (count=0)
        ];

        let ptv = ImmutablePointToValues::new(src);

        // Only values from multi-value points go into the container: [1, 2]
        assert_eq!(ptv.values_container, [1, 2]);

        // Verify all values are correctly retrievable.
        assert_eq!(
            ptv.get_values(0).map(|i| i.copied().collect::<Vec<_>>()),
            Some(vec![42]),
        );
        assert_eq!(
            ptv.get_values(1).map(|i| i.copied().collect::<Vec<_>>()),
            Some(vec![1, 2]),
        );
        assert_eq!(
            ptv.get_values(2).map(|i| i.copied().collect::<Vec<_>>()),
            Some(vec![99]),
        );
        assert_eq!(
            ptv.get_values(3).map(|i| i.copied().collect::<Vec<_>>()),
            Some(vec![]),
        );
    }

    #[test]
    fn test_get_values_count() {
        let src = vec![vec![1, 2, 3], vec![10], vec![], vec![4, 5]];

        let ptv = ImmutablePointToValues::new(src);

        assert_eq!(ptv.get_values_count(0), Some(3));
        assert_eq!(ptv.get_values_count(1), Some(1));
        assert_eq!(ptv.get_values_count(2), Some(0));
        assert_eq!(ptv.get_values_count(3), Some(2));
        // Out-of-bounds index returns None.
        assert_eq!(ptv.get_values_count(100), None);
    }

    #[test]
    fn test_check_values_any() {
        let src = vec![vec![1, 2, 3], vec![10], vec![], vec![4, 5]];

        let ptv = ImmutablePointToValues::new(src);

        // Multi-value point: check existing and missing values.
        assert!(ptv.check_values_any(0, |v| *v == 2));
        assert!(!ptv.check_values_any(0, |v| *v == 99));

        // Single-value point.
        assert!(ptv.check_values_any(1, |v| *v == 10));
        assert!(!ptv.check_values_any(1, |v| *v == 0));

        // Empty point always returns false.
        assert!(!ptv.check_values_any(2, |_| true));

        // Out-of-bounds index returns false.
        assert!(!ptv.check_values_any(100, |_| true));
    }

    /// Helper: assert that the point entry at `idx` has been reset to the default
    /// value (i.e., zero values, as if it were an empty slice).
    fn assert_point_entry_is_default(ptv: &ImmutablePointToValues<i32>, idx: PointOffsetType) {
        // Default entry should report a count of 0.
        assert_eq!(
            ptv.get_values_count(idx),
            Some(0),
            "Expected values count to be Some(0) for removed point {idx}"
        );
        // get_values should return Some(empty iterator).
        let vals: Vec<i32> = ptv
            .get_values(idx)
            .expect("Entry should still exist after removal")
            .copied()
            .collect();
        assert!(
            vals.is_empty(),
            "Expected no values for removed point {idx}, got {vals:?}"
        );
        // check_values_any should always return false for a default (empty) entry.
        assert!(
            !ptv.check_values_any(idx, |_| true),
            "Expected check_values_any to return false for removed point {idx}"
        );
    }

    #[test]
    fn test_remove_single_value_point() {
        let mut values = vec![
            vec![10],     // single — inline
            vec![20, 30], // multi — in container
            vec![40],     // single — inline
        ];

        let mut ptv = ImmutablePointToValues::new(values.clone());

        // Remove the inline single-value point.
        let removed = ptv.remove_point(0);
        assert_eq!(removed, vec![10]);
        values[0].clear();

        check_values(&ptv, &values);
        assert_point_entry_is_default(&ptv, 0);

        // Remove the second inline single-value point.
        let removed = ptv.remove_point(2);
        assert_eq!(removed, vec![40]);
        values[2].clear();

        check_values(&ptv, &values);
        assert_point_entry_is_default(&ptv, 2);

        // Multi-value point should still be intact.
        let removed = ptv.remove_point(1);
        assert_eq!(removed, vec![20, 30]);
        values[1].clear();

        check_values(&ptv, &values);
        assert_point_entry_is_default(&ptv, 1);
    }

    #[test]
    fn test_remove_out_of_bounds() {
        let ptv_src = vec![vec![1]];
        let mut ptv = ImmutablePointToValues::new(ptv_src);

        // Removing an out-of-bounds index should return an empty vec.
        let removed = ptv.remove_point(999);
        assert!(removed.is_empty());
    }

    #[test]
    fn test_get_values_out_of_bounds() {
        let ptv = ImmutablePointToValues::<i32>::new(vec![vec![1]]);
        assert!(ptv.get_values(10).is_none());
    }

    #[test]
    fn test_all_single_values() {
        // When every point has exactly one value, the container should be empty.
        let src = vec![vec![1], vec![2], vec![3], vec![4], vec![5]];

        let ptv = ImmutablePointToValues::new(src);

        assert!(ptv.values_container.is_empty());

        for i in 0..5 {
            assert_eq!(ptv.get_values_count(i), Some(1));
            let vals: Vec<_> = ptv.get_values(i).unwrap().copied().collect();
            assert_eq!(vals, vec![(i + 1) as i32]);
        }
    }

    #[test]
    fn test_empty_source() {
        let mut ptv = ImmutablePointToValues::<i32>::new(vec![]);
        assert!(ptv.get_values(0).is_none());
        assert_eq!(ptv.get_values_count(0), None);
        assert!(!ptv.check_values_any(0, |_| true));
        assert!(ptv.remove_point(0).is_empty());
    }
}
