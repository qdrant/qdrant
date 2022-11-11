use std::fs::OpenOptions;
use std::mem::size_of;
use std::ops::Range;
use std::path::Path;

use memmap2::{Mmap, MmapMut};

use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::PointOffsetType;

pub const MMAP_PANIC_MESSAGE: &str = "Mmap links are not loaded";

fn transmute_from_u8<T>(data: &[u8]) -> &[T] {
    let len = data.len() / size_of::<T>();
    let ptr = data.as_ptr() as *const T;
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

fn transmute_from_u8_mut<T>(data: &mut [u8]) -> &mut [T] {
    let len = data.len() / size_of::<T>();
    let ptr = data.as_mut_ptr() as *mut T;
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
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

#[derive(Default)]
struct GraphLinksFileHeader {
    pub point_count: u64,
    pub levels_count: u64,
    pub total_links_len: u64,
    pub total_offsets_len: u64,
}

impl GraphLinksFileHeader {
    pub fn raw_size() -> usize {
        size_of::<u64>() * 4
    }

    pub fn save(&self, mmap: &mut MmapMut) {
        let byte_slice = &mut mmap[0..Self::raw_size()];
        let arr: &mut [u64] = transmute_from_u8_mut(byte_slice);
        arr[0] = self.point_count;
        arr[1] = self.levels_count;
        arr[2] = self.total_links_len;
        arr[3] = self.total_offsets_len;
    }

    pub fn load(mmap: &Mmap) -> GraphLinksFileHeader {
        let byte_slice = &mmap[0..Self::raw_size()];
        let arr: &[u64] = transmute_from_u8(byte_slice);
        GraphLinksFileHeader {
            point_count: arr[0],
            levels_count: arr[1],
            total_links_len: arr[2],
            total_offsets_len: arr[3],
        }
    }

    pub fn get_file_size(&self) -> u64 {
        self.get_offsets_range().end as u64
    }

    pub fn get_level_offsets_range(&self) -> Range<usize> {
        let start = 64;
        start..start + self.levels_count as usize * size_of::<u64>()
    }

    pub fn get_reindex_range(&self) -> Range<usize> {
        let start = self.get_level_offsets_range().end;
        start..start + self.point_count as usize * size_of::<PointOffsetType>()
    }

    pub fn get_links_range(&self) -> Range<usize> {
        let start = self.get_reindex_range().end;
        start..start + self.total_links_len as usize * size_of::<PointOffsetType>()
    }

    pub fn get_offsets_range(&self) -> Range<usize> {
        let start = self.get_links_range().end;
        start..start + self.total_offsets_len as usize * size_of::<u64>()
    }
}

struct GraphLinksConverter {
    pub edges: Vec<Vec<Vec<PointOffsetType>>>,
    pub reindex: Vec<PointOffsetType>,
    pub back_index: Vec<usize>,
    pub total_links_len: usize,
    pub total_offsets_len: usize,
}

impl GraphLinksConverter {
    pub fn new(edges: Vec<Vec<Vec<PointOffsetType>>>) -> Self {
        if edges.is_empty() {
            return Self {
                edges,
                reindex: Vec::new(),
                back_index: Vec::new(),
                total_links_len: 0,
                total_offsets_len: 1,
            };
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
        let mut total_links_len = 0;
        let mut total_offsets_len = 1;
        for point in edges.iter() {
            for layer in point.iter() {
                total_links_len += layer.len();
                total_offsets_len += 1;
            }
        }

        Self {
            edges,
            reindex,
            back_index,
            total_links_len,
            total_offsets_len,
        }
    }

    pub fn save_to_file(&mut self, path: &Path) -> OperationResult<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let header = GraphLinksFileHeader {
            point_count: self.reindex.len() as u64,
            levels_count: self.get_levels_count() as u64,
            total_links_len: self.total_links_len as u64,
            total_offsets_len: self.total_offsets_len as u64,
        };

        file.set_len(header.get_file_size())?;

        let m = unsafe { MmapMut::map_mut(&file) };
        let mut mmap = m?;
        header.save(&mut mmap);

        {
            let reindex_range = header.get_reindex_range();
            let reindex_byte_slice = &mut mmap[reindex_range];
            let reindex_slice: &mut [PointOffsetType] = transmute_from_u8_mut(reindex_byte_slice);
            reindex_slice.copy_from_slice(&self.reindex);
        }

        let mut level_offsets = Vec::new();
        {
            let links_range = header.get_links_range();
            let offsets_range = header.get_offsets_range();
            let union_range = links_range.start..offsets_range.end;
            let (links_mmap, offsets_mmap) =
                mmap[union_range].as_mut().split_at_mut(links_range.len());
            let links_mmap: &mut [PointOffsetType] = transmute_from_u8_mut(links_mmap);
            let offsets_mmap: &mut [u64] = transmute_from_u8_mut(offsets_mmap);
            offsets_mmap[0] = 0;

            let mut links_pos = 0;
            let mut offsets_pos = 1;
            for level in 0..header.levels_count as usize {
                level_offsets.push(offsets_pos as u64 - 1);
                self.iterate_level_points(level, |_, links| {
                    links_mmap[links_pos..links_pos + links.len()].copy_from_slice(&links);
                    links_pos += links.len();

                    offsets_mmap[offsets_pos] = links_pos as u64;
                    offsets_pos += 1;
                });
            }
        }

        {
            let level_offsets_range = header.get_level_offsets_range();
            let level_offsets_byte_slice = &mut mmap[level_offsets_range];
            let level_offsets_slice: &mut [u64] = transmute_from_u8_mut(level_offsets_byte_slice);
            level_offsets_slice.copy_from_slice(&level_offsets);
        }

        mmap.flush()?;
        Ok(())
    }

    pub fn get_levels_count(&self) -> usize {
        // because back_index is sorted by point`s max layer, we can retrieve max level from `point_id = back_index[0]`
        self.edges[self.back_index[0]].len()
    }

    pub fn iterate_level_points<F>(&mut self, level: usize, mut f: F)
    where
        F: FnMut(usize, &mut Vec<PointOffsetType>),
    {
        let edges_len = self.edges.len();
        if level == 0 {
            (0..edges_len).for_each(|point_id| f(point_id, &mut self.edges[point_id][0]));
        } else {
            for i in 0..edges_len {
                let point_id = self.back_index[i];
                if level >= self.edges[point_id].len() {
                    break;
                }
                f(point_id, &mut self.edges[point_id][level]);
            }
        }
    }
}

pub trait GraphLinks: Default {
    fn load_from_file(path: &Path) -> OperationResult<Self>;

    fn offsets_len(&self) -> usize;

    fn levels_count(&self) -> usize;

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType];

    fn get_links_range(&self, idx: usize) -> Range<usize>;

    fn get_level_offset(&self, level: usize) -> usize;

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType;

    fn num_points(&self) -> usize;

    // Convert from graph layers builder links
    // `Vec<Vec<Vec<_>>>` means:
    // vector of points -> vector of layers for specific point -> vector of links for specific point and layer
    fn from_vec(
        edges: Vec<Vec<Vec<PointOffsetType>>>,
        path: Option<&Path>,
    ) -> OperationResult<Self>;

    fn links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        if level == 0 {
            let links_range = self.get_links_range(point_id as usize);
            &self.get_links(links_range)
        } else {
            let reindexed_point_id = self.reindex(point_id) as usize;
            let layer_offsets_start = self.get_level_offset(level);
            let links_range = self.get_links_range(layer_offsets_start + reindexed_point_id);
            &self.get_links(links_range)
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
}

#[derive(Default)]
pub struct GraphLinksRam {
    // all flattened links of all levels
    links: Vec<PointOffsetType>,
    // all ranges in `links`. each range is `links[offsets[i]..offsets[i+1]]`
    // ranges are sorted by level
    offsets: Vec<u64>,
    // start offet of each level in `offsets`
    level_offsets: Vec<u64>,
    // for level 1 and above: reindex[point_id] = index of point_id in offsets
    reindex: Vec<PointOffsetType>,
}

impl GraphLinks for GraphLinksRam {
    fn load_from_file(path: &Path) -> OperationResult<Self> {
        let mmap = GraphLinksMmap::load_from_file(path)?;

        Ok(Self {
            links: mmap.get_links_slice().to_vec(),
            offsets: mmap.get_offsets_slice().to_vec(),
            level_offsets: mmap.level_offsets.clone(),
            reindex: mmap.get_reindex_slice().to_vec(),
        })
    }

    fn from_vec(
        edges: Vec<Vec<Vec<PointOffsetType>>>,
        path: Option<&Path>,
    ) -> OperationResult<Self> {
        let mut graph_links = GraphLinksRam {
            links: Vec::new(),
            offsets: vec![0],
            level_offsets: Vec::new(),
            reindex: Vec::new(),
        };

        let mut converter = GraphLinksConverter::new(edges);
        if let Some(path) = path {
            converter.save_to_file(path)?;
        }

        if !converter.edges.is_empty() {
            let levels_count = converter.get_levels_count();
            for level in 0..levels_count {
                graph_links
                    .level_offsets
                    .push(graph_links.offsets.len() as u64 - 1);
                converter.iterate_level_points(level, |_, links| {
                    graph_links.links.extend_from_slice(links);
                    graph_links.offsets.push(graph_links.links.len() as u64);
                    links.clear();
                });
            }
            graph_links.reindex = converter.reindex;
        }
        Ok(graph_links)
    }

    fn offsets_len(&self) -> usize {
        self.offsets.len()
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
        start as usize..end as usize
    }

    fn get_level_offset(&self, level: usize) -> usize {
        self.level_offsets[level] as usize
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        self.reindex[point_id as usize]
    }

    fn num_points(&self) -> usize {
        self.reindex.len()
    }
}

#[derive(Default)]
pub struct GraphLinksMmap {
    mmap: Option<Mmap>,
    header: GraphLinksFileHeader,
    level_offsets: Vec<u64>,
}

impl GraphLinksMmap {
    fn get_reindex_slice(&self) -> &[PointOffsetType] {
        if let Some(mmap) = &self.mmap {
            let reindex_range = self.header.get_reindex_range();
            let reindex_byte_slice = &mmap[reindex_range];
            transmute_from_u8(reindex_byte_slice)
        } else {
            panic!("{}", MMAP_PANIC_MESSAGE);
        }
    }

    fn get_links_slice(&self) -> &[PointOffsetType] {
        if let Some(mmap) = &self.mmap {
            let links_range = self.header.get_links_range();
            let links_byte_slice = &mmap[links_range];
            transmute_from_u8(links_byte_slice)
        } else {
            panic!("{}", "Mmap links are not loaded");
        }
    }

    fn get_offsets_slice(&self) -> &[u64] {
        if let Some(mmap) = &self.mmap {
            let offsets_range = self.header.get_offsets_range();
            let offsets_byte_slice = &mmap[offsets_range];
            transmute_from_u8(offsets_byte_slice)
        } else {
            panic!("{}", MMAP_PANIC_MESSAGE);
        }
    }
}

impl GraphLinks for GraphLinksMmap {
    fn load_from_file(path: &Path) -> OperationResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path)?;

        let mmap = unsafe { Mmap::map(&file)? };
        let header = GraphLinksFileHeader::load(&mmap);
        let level_offsets_range = header.get_level_offsets_range();
        let level_offsets_byte_slice = &mmap[level_offsets_range];
        let level_offsets: &[u64] = transmute_from_u8(level_offsets_byte_slice);
        let level_offsets = level_offsets.to_vec();

        Ok(Self {
            mmap: Some(mmap),
            header,
            level_offsets,
        })
    }

    fn from_vec(
        edges: Vec<Vec<Vec<PointOffsetType>>>,
        path: Option<&Path>,
    ) -> OperationResult<Self> {
        let mut converter = GraphLinksConverter::new(edges);
        if let Some(path) = path {
            converter.save_to_file(path)?;
            Self::load_from_file(path)
        } else {
            Err(OperationError::service_error(
                "path is required for GraphLinksMmap",
            ))
        }
    }

    fn offsets_len(&self) -> usize {
        self.header.get_offsets_range().len() / size_of::<u64>()
    }

    fn levels_count(&self) -> usize {
        self.level_offsets.len()
    }

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType] {
        &self.get_links_slice()[range]
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        let offsets_slice = self.get_offsets_slice();
        offsets_slice[idx as usize] as usize..offsets_slice[idx as usize + 1] as usize
    }

    fn get_level_offset(&self, level: usize) -> usize {
        self.level_offsets[level] as usize
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        self.get_reindex_slice()[point_id as usize]
    }

    fn num_points(&self) -> usize {
        self.header.point_count as usize
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;
    use crate::types::PointOffsetType;

    fn to_vec(links: &GraphLinksRam) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut result = Vec::new();
        let num_points = links.num_points();
        for i in 0..num_points {
            let mut layers = Vec::new();
            let num_levels = links.point_level(i as PointOffsetType) + 1;
            for level in 0..num_levels {
                let links = links.links(i as PointOffsetType, level).to_vec();
                layers.push(links);
            }
            result.push(layers);
        }
        result
    }

    #[test]
    fn test_graph_links_construction() {
        // no points
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
        assert_eq!(links, cmp_links);

        // 2 points without any links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![]], vec![vec![]]];
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
        assert_eq!(links, cmp_links);

        // one link at level 0
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![1]], vec![vec![0]]];
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
        assert_eq!(links, cmp_links);

        // 3 levels with no links at second level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2], vec![], vec![2]],
            vec![vec![0, 1], vec![], vec![1]],
        ];
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
        assert_eq!(links, cmp_links);

        // 3 levels with no links at last level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2], vec![2], vec![]],
            vec![vec![0, 2], vec![1], vec![]],
            vec![vec![0, 1]],
        ];
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
        assert_eq!(links, cmp_links);

        // 4 levels with random unexists links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2, 5, 6]],
            vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
            vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
            vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
            vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
        ];
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
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
        let cmp_links = to_vec(&GraphLinksRam::from_vec(links.clone(), None).unwrap());
        assert_eq!(links, cmp_links);
    }
}
