use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::{types::PointOffsetType, entry::entry_point::OperationResult};

pub trait GraphLinks: Default {
    fn allocate(&mut self, points_count: usize, levels_count: usize, offsets_len: usize, links_len: usize) -> OperationResult<()>;

    fn set_reindex(&mut self, rendex: &[PointOffsetType]) -> OperationResult<()>;

    fn offsets_len(&self) -> usize;

    fn total_links_len(&self) -> usize;

    fn levels_count(&self) -> usize;

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType];

    fn get_links_range(&self, idx: usize) -> Range<usize>;

    fn get_level_offset(&self, level: usize) -> usize;

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType;

    fn push_offset(&mut self, offset: usize) -> OperationResult<()>;

    fn push_links(&mut self, links: &[PointOffsetType]) -> OperationResult<()>;

    fn push_level_offset(&mut self, level_offset: usize);

    fn num_points(&self) -> usize;

    // Convert from graph layers builder links
    // `Vec<Vec<Vec<_>>>` means:
    // vector of points -> vector of layers for specific point -> vector of links for specific point and layer
    fn from_vec(&mut self, edges: &Vec<Vec<Vec<PointOffsetType>>>) -> OperationResult<()> {
        if edges.is_empty() {
            return Ok(());
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

        // estimate size of `links` and `offsets`
        let mut links_len = 0;
        let mut offsets_len = 1;
        for point in edges.iter() {
            for layer in point.iter() {
                links_len += layer.len();
                offsets_len += 1;
            }
        }

        // because back_index is sorted by point`s max layer, we can retrieve max level from `point_id = back_index[0]`
        let levels_count = edges[back_index[0]].len();

        self.allocate(reindex.len(), levels_count, offsets_len as usize, links_len as usize)?;
        self.set_reindex(&reindex)?;
        self.push_offset(0)?;

        // fill level 0 links. level 0 is required
        debug_assert!(levels_count > 0);
        self.fill_level_links(0, 0..edges.len(), edges)?;

        // fill other levels links
        for level in 1..levels_count {
            let point_id_iter = back_index
                .iter()
                .cloned()
                .take_while(|&point_id| level < edges[point_id].len());
            self.fill_level_links(level, point_id_iter, edges)?;
        }

        Ok(())
    }

    // Convert into graph builder format
    fn to_vec(&self) -> Vec<Vec<Vec<PointOffsetType>>> {
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

    fn links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        if level == 0 {
            self.get_links_at_zero_level(point_id)
        } else {
            self.get_links_at_nonzero_level(point_id, level)
        }
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed_point_id = self.reindex(point_id) as usize;
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
        let links_range = self.get_links_range(point_id as usize);
        &self.get_links(links_range)
    }

    fn get_links_at_nonzero_level(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> &[PointOffsetType] {
        debug_assert!(level > 0);
        let reindexed_point_id = self.reindex(point_id) as usize;
        let layer_offsets_start = self.get_level_offset(level);
        let links_range = self.get_links_range(layer_offsets_start + reindexed_point_id);
        &self.get_links(links_range)
    }

    fn get_level_offsets_range(&self, level: usize) -> Option<Range<usize>> {
        if level < self.levels_count() {
            let layer_offsets_start = self.get_level_offset(level);
            let layer_offsets_end = if level + 1 < self.levels_count() {
                // `level` is not last, next level_offsets is end of range
                self.get_level_offset(level + 1)
            } else {
                // `level` is last, next `offsets.len()` is end of range
                self.offsets_len() - 1
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
    ) -> OperationResult<()> where
        I: Iterator<Item = usize>,
    {
        self.push_level_offset(self.offsets_len() - 1);

        for point_id in level_points_iter {
            let links = &edges[point_id][level];
            self.push_links(links)?;
            self.push_offset(self.total_links_len())?;
        }
        Ok(())
    }
}

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
pub struct GraphLinksRam {
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

impl GraphLinks for GraphLinksRam {
    fn allocate(&mut self, points_count: usize, levels_count: usize, offsets_len: usize, links_len: usize) -> OperationResult<()> {
        self.links.reserve(links_len);
        self.offsets.reserve(offsets_len);
        self.level_offsets.reserve(levels_count);
        self.reindex.reserve(points_count);
        Ok(())
    }

    fn set_reindex(&mut self, rendex: &[PointOffsetType]) -> OperationResult<()> {
        self.reindex = rendex.to_vec();
        Ok(())
    }

    fn offsets_len(&self) -> usize {
        self.offsets.len()
    }

    fn total_links_len(&self) -> usize {
        self.links.len()
    }

    fn levels_count(&self) -> usize {
        self.level_offsets.len()
    }

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType] {
        &self.links[range]
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        start..end
    }

    fn get_level_offset(&self, level: usize) -> usize {
        self.level_offsets[level]
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        self.reindex[point_id as usize]
    }

    fn push_offset(&mut self, offset: usize) -> OperationResult<()> {
        self.offsets.push(offset);
        Ok(())
    }

    fn push_links(&mut self, links: &[PointOffsetType]) -> OperationResult<()> {
        self.links.extend_from_slice(links);
        Ok(())
    }

    fn push_level_offset(&mut self, level_offset: usize) {
        self.level_offsets.push(level_offset);
    }

    fn num_points(&self) -> usize {
        self.reindex.len()
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct GraphLinksMmap {
    
    // start offet of each level in `offsets`
    level_offsets: Vec<usize>,
}

impl GraphLinks for GraphLinksMmap {
    fn allocate(&mut self, points_count: usize, levels_count: usize, offsets_len: usize, links_len: usize) -> OperationResult<()> {
        todo!()
    }

    fn set_reindex(&mut self, rendex: &[PointOffsetType]) -> OperationResult<()> {
        todo!()
    }

    fn offsets_len(&self) -> usize {
        todo!()
    }

    fn total_links_len(&self) -> usize {
        todo!()
    }

    fn levels_count(&self) -> usize {
        todo!()
    }

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType] {
        todo!()
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        todo!()
    }

    fn get_level_offset(&self, level: usize) -> usize {
        todo!()
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        todo!()
    }

    fn push_offset(&mut self, offset: usize) -> OperationResult<()> {
        todo!()
    }

    fn push_links(&mut self, links: &[PointOffsetType]) -> OperationResult<()> {
        todo!()
    }

    fn push_level_offset(&mut self, level_offset: usize) {
        todo!()
    }

    fn num_points(&self) -> usize {
        todo!()
    }
}
