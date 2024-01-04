use std::ops::Range;

use common::types::PointOffsetType;

// Flatten points-to-values map
#[derive(Debug, Clone, Default)]
pub struct ImmutablePointToValues<N: Default> {
    point_to_values: Vec<Range<u32>>,
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

        // Point removing has to remove `idx` from both maps: points-to-values and values-to-points.
        // The first one is easy: we just remove the entry from the map.
        // The second one is more complicated: we have to remove all mentions of `idx` in values-to-points map.
        // To deal with it, take old values from points-to-values map, witch contains all values with `idx` in values-to-points map.
        let removed_values_range = self.point_to_values[idx as usize].clone();
        self.point_to_values[idx as usize] = Default::default();

        // Iterate over all values which were removed from points-to-values map
        let mut result = Vec::with_capacity(removed_values_range.len());
        for value_index in removed_values_range {
            let value = std::mem::take(&mut self.point_to_values_container[value_index as usize]);
            result.push(value);
        }

        result
    }
}
