use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

/*
Links data for whole graph layers.

                                    sorted
                     points:        points:
points to lvl        012345         142350
     0 -> 0
     1 -> 4    lvl4:  7       lvl4: 7
     2 -> 2    lvl3:  Z  Y    lvl3: ZY
     3 -> 2    lvl2:  abcd    lvl2: adbc
     4 -> 3    lvl1:  ABCDE   lvl1: ADBCE
     5 -> 1    lvl0: 123456   lvl0: 123456  <- lvl 0 is not sorted


lvl offset:        6       11     15     17
                   │       │      │      │
                   │       │      │      │
                   ▼       ▼      ▼      ▼
indexes:  012345   6789A   BCDE   FG     H

flatten:  123456   ADBCE   adbc   ZY     7
                   ▲ ▲ ▲   ▲ ▲    ▲      ▲
                   │ │ │   │ │    │      │
                   │ │ │   │ │    │      │
                   │ │ │   │ │    │      │
reindex:           142350  142350 142350 142350  (same for each level)


for lvl > 0:
links offset = level_offsets[level] + offsets[reindex[point_id]]
*/
#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct GraphLinks {
    // all flattened links of all levels
    links: Vec<PointOffsetType>,
    // all ranges in `links`. each range is `links[offsets[i]..offsets[i+1]]`
    // ranges are sorted by level
    offsets: Vec<usize>,
    // start offet of each level in `offsets`
    level_offsets: Vec<usize>,
    // for level 1 and above: reindex[point_id] = index of point_id in offsets
    reindex: Vec<PointOffsetType>,
}

impl GraphLinks {
    // Convert from graph layers builder links
    // `Vec<Vec<Vec<_>>>` means:
    // vector of points -> vector of layers for specific point -> vector of links for specific point and layer
    pub fn from_vec(edges: &Vec<Vec<Vec<PointOffsetType>>>) -> Self {
        if edges.is_empty() {
            return Self::default();
        }

        // create map from index in `offsets` to point_id
        let mut back_index: Vec<usize> = (0..edges.len()).collect();
        // sort by max layer and use this map to build `Self.reindex`
        back_index.sort_unstable_by_key(|&i| edges[i].len());
        back_index.reverse();

        // `reindex` is map from point id to index in `Self.offsets`
        let mut reindex = vec![0; back_index.len()];
        for i in 0..back_index.len() {
            reindex[back_index[i]] = i as PointOffsetType;
        }

        let mut graph_links = GraphLinks {
            links: Vec::new(),
            offsets: vec![0],
            level_offsets: Vec::new(),
            reindex,
        };

        // because back_index is sorted by point`s max layer, we can retrieve max level from `point_id = back_index[0]`
        let levels_count = edges[back_index[0]].len();

        // fill level 0 links. level 0 is required
        debug_assert!(levels_count > 0);
        graph_links.fill_level_links(0, 0..edges.len(), edges);

        // fill other levels links
        for level in 1..levels_count {
            let point_id_iter = back_index
                .iter()
                .cloned()
                .take_while(|&point_id| level < edges[point_id].len());
            graph_links.fill_level_links(level, point_id_iter, edges);
        }

        graph_links
    }

    // Convert into graph builder format
    pub fn to_vec(&self) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut result = Vec::new();
        let num_points = self.num_points();
        for i in 0..num_points {
            let mut layers = Vec::new();
            let num_levels = self.point_level(i as PointOffsetType) + 1;
            for level in 0..num_levels {
                let links = self.links(i as PointOffsetType, level).to_vec();
                layers.push(links);
            }
            result.push(layers);
        }
        result
    }

    pub fn links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        if level == 0 {
            self.get_links_at_zero_level(point_id)
        } else {
            self.get_links_at_nonzero_level(point_id, level)
        }
    }

    pub fn num_points(&self) -> usize {
        self.reindex.len()
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed_point_id = self.reindex[point_id as usize] as usize;
        // level 0 is always present, start checking from level 1. Stop checking when level is incorrect
        for level in 1.. {
            if let Some(offsets_range) = self.get_level_offsets_range(level) {
                if offsets_range.start + reindexed_point_id >= offsets_range.end {
                    // incorrect level because point_id is out of range
                    return level - 1;
                }
            } else {
                // incorrect level because this level is larger that avaliable levels
                return level - 1;
            }
        }
        unreachable!()
    }

    fn get_links_at_zero_level(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        let start = self.offsets[point_id as usize];
        let end = self.offsets[point_id as usize + 1];
        &self.links[start..end]
    }

    fn get_links_at_nonzero_level(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> &[PointOffsetType] {
        debug_assert!(level > 0);
        let reindexed_point_id = self.reindex[point_id as usize] as usize;
        let layer_offsets_start = self.level_offsets[level];
        let start = self.offsets[layer_offsets_start + reindexed_point_id];
        let end = self.offsets[layer_offsets_start + reindexed_point_id + 1];
        &self.links[start..end]
    }

    fn get_level_offsets_range(&self, level: usize) -> Option<Range<usize>> {
        if level < self.level_offsets.len() {
            let layer_offsets_start = self.level_offsets[level];
            let layer_offsets_end = if level + 1 < self.level_offsets.len() {
                // `level` is not last, next level_offsets is end of range
                self.level_offsets[level + 1]
            } else {
                // `level` is last, next `offsets.len()` is end of range
                self.offsets.len() - 1
            };
            Some(layer_offsets_start..layer_offsets_end)
        } else {
            None
        }
    }

    fn fill_level_links<I>(
        &mut self,
        level: usize,
        level_points_iter: I,
        edges: &[Vec<Vec<PointOffsetType>>],
    ) where
        I: Iterator<Item = usize>,
    {
        self.level_offsets.push(self.offsets.len() - 1);

        for point_id in level_points_iter {
            let links = &edges[point_id][level];
            self.links.extend_from_slice(links);
            self.offsets.push(self.links.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::types::PointOffsetType;

    #[test]
    fn test_graph_links_construction() {
        // no points
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);

        // 2 points without any links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![]], vec![vec![]]];
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);

        // one link at level 0
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![1]], vec![vec![0]]];
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);

        // 3 levels with no links at second level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2], vec![], vec![2]],
            vec![vec![0, 1], vec![], vec![1]],
        ];
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);

        // 3 levels with no links at last level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2], vec![2], vec![]],
            vec![vec![0, 2], vec![1], vec![]],
            vec![vec![0, 1]],
        ];
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);

        // 4 levels with random unexists links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2, 5, 6]],
            vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
            vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
            vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
            vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
        ];
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);

        // fully random links
        let mut rng = rand::thread_rng();
        let points_count = 100;
        let max_levels_count = 10;
        let links: Vec<Vec<Vec<PointOffsetType>>> = (0..points_count)
            .map(|_| {
                let levels_count = rng.gen_range(1..max_levels_count);
                (0..levels_count)
                    .map(|_| {
                        let links_count = rng.gen_range(0..max_levels_count);
                        (0..links_count)
                            .map(|_| rng.gen_range(0..points_count) as PointOffsetType)
                            .collect()
                    })
                    .collect()
            })
            .collect();
        let cmp_links = GraphLinks::from_vec(&links).to_vec();
        assert_eq!(links, cmp_links);
    }
}
