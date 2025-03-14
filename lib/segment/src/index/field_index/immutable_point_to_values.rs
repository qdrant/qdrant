use std::ops::Range;

use common::types::PointOffsetType;

// Flatten points-to-values map
// It's an analogue of `Vec<Vec<N>>` but more RAM efficient because it stores values in a single Vec.
// This structure doesn't support adding new values, only removing.
// It's used in immutable field indices like `ImmutableMapIndex`, `ImmutableNumericIndex`, etc to store points-to-values map.
#[derive(Debug, Clone, Default)]
pub struct ImmutablePointToValues<N: Default> {
    // ranges in `point_to_values_container` which contains values for each point
    // `u32` is used instead of `usize` because it's more RAM efficient
    // We can expect that we will never have more than 4 billion values per segment
    point_to_values: Vec<Range<u32>>,
    // flattened values
    point_to_values_container: Vec<N>,
}

impl<N: Default> ImmutablePointToValues<N> {
    pub fn new(src: Vec<Vec<N>>) -> Self {
        let mut point_to_values = Vec::with_capacity(src.len());
        let all_values_count = src.iter().fold(0, |acc, values| acc + values.len());
        let mut point_to_values_container = Vec::with_capacity(all_values_count);
        for values in src {
            let container_len = point_to_values_container.len() as u32;
            let range = container_len..container_len + values.len() as u32;
            point_to_values.push(range.clone());
            point_to_values_container.extend(values);
        }
        Self {
            point_to_values,
            point_to_values_container,
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl FnMut(&N) -> bool) -> bool {
        let Some(range) = self.point_to_values.get(idx as usize).cloned() else {
            return false;
        };

        let range = range.start as usize..range.end as usize;
        if let Some(values) = self.point_to_values_container.get(range) {
            values.iter().any(check_fn)
        } else {
            false
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<impl Iterator<Item = &N> + '_> {
        let range = self.point_to_values.get(idx as usize)?.clone();
        let range = range.start as usize..range.end as usize;
        Some(self.point_to_values_container[range].iter())
    }

    pub fn get_values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.point_to_values
            .get(idx as usize)
            .map(|range| (range.end - range.start) as usize)
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> Vec<N> {
        if self.point_to_values.len() <= idx as usize {
            return Default::default();
        }

        let removed_values_range = self.point_to_values[idx as usize].clone();
        self.point_to_values[idx as usize] = Default::default();

        let mut result = Vec::with_capacity(removed_values_range.len());
        for value_index in removed_values_range {
            // deleted values still use RAM, but it's not a problem because optimizers will actually reduce RAM usage
            let value = std::mem::take(&mut self.point_to_values_container[value_index as usize]);
            result.push(value);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let check = |point_to_values: &ImmutablePointToValues<_>, values: &[Vec<_>]| {
            for (idx, values) in values.iter().enumerate() {
                let values_vec: Option<Vec<_>> = point_to_values
                    .get_values(idx as PointOffsetType)
                    .map(|i| i.copied().collect());
                assert_eq!(values_vec, Some(values.clone()),);
            }
        };

        check(&point_to_values, values.as_slice());

        point_to_values.remove_point(0);
        values[0].clear();

        check(&point_to_values, values.as_slice());

        point_to_values.remove_point(3);
        values[3].clear();

        check(&point_to_values, values.as_slice());
    }
}
