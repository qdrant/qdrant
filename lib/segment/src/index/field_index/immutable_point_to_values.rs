use std::ops::Range;

use common::types::PointOffsetType;

// Flatten points-to-values map
// It's an analogue of `Vec<Vec<N>>` but more RAM efficient because it stores values in a single Vec.
// This structure doesn't support adding new values, only removing.
// It's used in immutable field indices like ImmutableMapIndex, ImmutableNumericIndex, etc to store points-to-values map.
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
        let mut point_to_values: Vec<_> = Default::default();
        let mut point_to_values_container: Vec<_> = Default::default();
        for values in src {
            let values = values.into_iter().collect::<Vec<_>>();
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

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[N]> {
        let range = self.point_to_values.get(idx as usize)?.clone();
        let range = range.start as usize..range.end as usize;
        Some(&self.point_to_values_container[range])
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
