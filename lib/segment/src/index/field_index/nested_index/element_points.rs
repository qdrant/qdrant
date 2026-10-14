//! The points of the elements that have one value, for facets of a per-element index.

use common::types::PointOffsetType;

use super::{ElementMap, ElementOffsetType};

/// Maps the elements of one value at a time to the points that own them, reusing its
/// buffer from one value to the next.
///
/// The points of each value are sorted. In an immutable map the elements are laid out in
/// point order, so the points of elements in element order come already sorted, which
/// the sort handles in close to linear time.
pub(super) struct ElementPoints<'a> {
    element_map: &'a ElementMap,
    /// Points of the current value. Cleared for every value, but keeps its capacity, so a
    /// facet over many distinct values doesn't allocate a vector for each of them.
    points: Vec<PointOffsetType>,
}

impl<'a> ElementPoints<'a> {
    pub(super) fn new(element_map: &'a ElementMap) -> Self {
        Self {
            element_map,
            points: Vec::new(),
        }
    }

    /// The points owning `elements`, sorted and each once.
    pub(super) fn sorted(
        &mut self,
        elements: &mut dyn Iterator<Item = ElementOffsetType>,
    ) -> &[PointOffsetType] {
        self.points.clear();
        self.points
            .extend(elements.filter_map(|element| self.element_map.element_point(element)));
        self.points.sort_unstable();
        self.points.dedup();
        &self.points
    }

    /// The number of points below `max_point` owning `elements`, each counted once.
    pub(super) fn count_below(
        &mut self,
        elements: &mut dyn Iterator<Item = ElementOffsetType>,
        max_point: PointOffsetType,
    ) -> usize {
        self.sorted(elements)
            .partition_point(|&point| point < max_point)
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::index::field_index::nested_index::ElementMapBuilder;
    use crate::index::payload_config::IndexMutability;

    /// Consecutive values give the same points as mapping each value on its own.
    #[test]
    fn element_points_match_reference_across_values() {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(9);

        let mut builder = ElementMapBuilder::default();
        for point in 0..100 {
            builder.add(point, rng.random_range(0..4)).unwrap();
        }
        let map = ElementMap::create(
            dir.path().join("map"),
            IndexMutability::Immutable,
            &builder,
            0,
            true,
        )
        .unwrap();

        let mut element_points = ElementPoints::new(&map);
        for _ in 0..100 {
            let elements: Vec<ElementOffsetType> = (0..map.elements_count())
                .filter_map(|element| rng.random_bool(0.3).then_some(element as ElementOffsetType))
                .collect();
            let max_point = rng.random_range(0..120);

            let mut expected: Vec<PointOffsetType> = elements
                .iter()
                .filter_map(|&element| map.element_point(element))
                .collect();
            expected.sort_unstable();
            expected.dedup();
            let expected_count = expected.iter().filter(|&&point| point < max_point).count();

            assert_eq!(
                element_points.sorted(&mut elements.iter().copied()),
                expected.as_slice(),
            );
            assert_eq!(
                element_points.count_below(&mut elements.iter().copied(), max_point),
                expected_count,
            );
        }
    }
}
