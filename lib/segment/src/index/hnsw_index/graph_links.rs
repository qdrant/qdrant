use std::cmp::max;
use std::fs::OpenOptions;
use std::io::Read as _;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use memmap2::{Mmap, MmapMut};
use memory::{madvise, mmap_ops};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::vector_utils::TrySetCapacityExact;

pub const MMAP_PANIC_MESSAGE: &str = "Mmap links are not loaded";

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

const HEADER_SIZE: usize = 64;

#[derive(Clone, Debug, Default)]
struct GraphLinksFileHeader {
    pub point_count: u64,
    pub levels_count: u64,
    pub total_links_len: u64,
    pub total_offsets_len: u64,
    pub offsets_padding: u64,
}

fn get_level_offsets<'a>(data: &'a [u8], header: &GraphLinksFileHeader) -> &'a [u64] {
    let level_offsets_range = header.get_level_offsets_range();
    let level_offsets_byte_slice = &data[level_offsets_range];
    mmap_ops::transmute_from_u8_to_slice(level_offsets_byte_slice)
}

impl GraphLinksFileHeader {
    pub fn new(
        point_count: usize,
        levels_count: usize,
        total_links_len: usize,
        total_offsets_len: usize,
    ) -> GraphLinksFileHeader {
        let offsets_padding = if (point_count + total_links_len) % 2 == 0 {
            0
        } else {
            4
        };
        GraphLinksFileHeader {
            point_count: point_count as u64,
            levels_count: levels_count as u64,
            total_links_len: total_links_len as u64,
            total_offsets_len: total_offsets_len as u64,
            offsets_padding,
        }
    }

    pub fn raw_size() -> usize {
        size_of::<u64>() * 5
    }

    pub fn serialize_bytes_to(&self, raw_data: &mut [u8]) {
        let byte_slice = &mut raw_data[0..Self::raw_size()];
        let arr: &mut [u64] = mmap_ops::transmute_from_u8_to_mut_slice(byte_slice);
        arr[0] = self.point_count;
        arr[1] = self.levels_count;
        arr[2] = self.total_links_len;
        arr[3] = self.total_offsets_len;
        arr[4] = self.offsets_padding;
    }

    pub fn deserialize_bytes_from(raw_data: &[u8]) -> GraphLinksFileHeader {
        let byte_slice = &raw_data[0..Self::raw_size()];
        let arr: &[u64] = mmap_ops::transmute_from_u8_to_slice(byte_slice);
        GraphLinksFileHeader {
            point_count: arr[0],
            levels_count: arr[1],
            total_links_len: arr[2],
            total_offsets_len: arr[3],
            offsets_padding: arr[4],
        }
    }

    pub fn get_data_size(&self) -> u64 {
        self.get_offsets_range().end as u64
    }

    pub fn get_level_offsets_range(&self) -> Range<usize> {
        // level offsets are stored after header
        // but we might want to have some extra space for future changes
        let start = max(HEADER_SIZE, Self::raw_size());
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
        let start = self.get_links_range().end + self.offsets_padding as usize;
        start..start + self.total_offsets_len as usize * size_of::<u64>()
    }
}

pub struct GraphLinksConverter {
    edges: Vec<Vec<Vec<PointOffsetType>>>,
    reindex: Vec<PointOffsetType>,
    back_index: Vec<usize>,
    total_links_len: usize,
    total_offsets_len: usize,
    path: Option<PathBuf>,
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
                path: None,
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
            path: None,
        }
    }

    fn get_header(&self) -> GraphLinksFileHeader {
        GraphLinksFileHeader::new(
            self.reindex.len(),
            self.get_levels_count(),
            self.total_links_len,
            self.total_offsets_len,
        )
    }

    /// Size of compacted graph in bytes.
    pub fn data_size(&self) -> u64 {
        self.get_header().get_data_size()
    }

    pub fn serialize_to(&self, bytes_data: &mut [u8]) {
        let header = self.get_header();

        header.serialize_bytes_to(bytes_data);

        {
            let reindex_range = header.get_reindex_range();
            let reindex_byte_slice = &mut bytes_data[reindex_range];
            let reindex_slice: &mut [PointOffsetType] =
                mmap_ops::transmute_from_u8_to_mut_slice(reindex_byte_slice);
            reindex_slice.copy_from_slice(&self.reindex);
        }

        let header_levels_count = header.levels_count as usize;
        let mut level_offsets = Vec::with_capacity(header_levels_count);
        {
            let links_range = header.get_links_range();
            let offsets_range = header.get_offsets_range();
            let union_range = links_range.start..offsets_range.end;
            let (links_mmap, offsets_with_padding_mmap) = bytes_data[union_range]
                .as_mut()
                .split_at_mut(links_range.len());
            let offsets_mmap = &mut offsets_with_padding_mmap[header.offsets_padding as _..];
            let links_mmap: &mut [PointOffsetType] =
                mmap_ops::transmute_from_u8_to_mut_slice(links_mmap);
            let offsets_mmap: &mut [u64] = mmap_ops::transmute_from_u8_to_mut_slice(offsets_mmap);
            offsets_mmap[0] = 0;

            let mut links_pos = 0;
            let mut offsets_pos = 1;
            for level in 0..header_levels_count {
                level_offsets.push(offsets_pos as u64 - 1);
                self.iterate_level_points(level, |_, links| {
                    links_mmap[links_pos..links_pos + links.len()].copy_from_slice(links);
                    links_pos += links.len();

                    offsets_mmap[offsets_pos] = links_pos as u64;
                    offsets_pos += 1;
                });
            }
        }

        {
            let level_offsets_range = header.get_level_offsets_range();
            let level_offsets_byte_slice = &mut bytes_data[level_offsets_range];
            let level_offsets_slice: &mut [u64] =
                mmap_ops::transmute_from_u8_to_mut_slice(level_offsets_byte_slice);
            level_offsets_slice.copy_from_slice(&level_offsets);
        }
    }

    pub fn save_as(&mut self, path: &Path) -> OperationResult<()> {
        self.path = Some(path.to_path_buf());
        let temp_path = path.with_extension("tmp");
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                // Don't truncate because we explicitly set the length later
                .truncate(false)
                .open(temp_path.as_path())?;
            file.set_len(self.data_size())?;

            let m = unsafe { MmapMut::map_mut(&file) };
            let mut mmap = m?;

            self.serialize_to(&mut mmap);

            mmap.flush()?;
        }
        std::fs::rename(temp_path, path)?;

        Ok(())
    }

    pub fn get_levels_count(&self) -> usize {
        if self.back_index.is_empty() {
            return 0;
        }
        // because back_index is sorted by point`s max layer, we can retrieve max level from `point_id = back_index[0]`
        self.edges[self.back_index[0]].len()
    }

    pub fn iterate_level_points<F>(&self, level: usize, mut f: F)
    where
        F: FnMut(usize, &Vec<PointOffsetType>),
    {
        let edges_len = self.edges.len();
        if level == 0 {
            (0..edges_len).for_each(|point_id| f(point_id, &self.edges[point_id][0]));
        } else {
            for i in 0..edges_len {
                let point_id = self.back_index[i];
                if level >= self.edges[point_id].len() {
                    break;
                }
                f(point_id, &self.edges[point_id][level]);
            }
        }
    }
}

pub trait GraphLinks: Sized {
    fn load_from_file(path: &Path) -> OperationResult<Self>;

    fn from_converter(converter: GraphLinksConverter) -> OperationResult<Self>;

    fn num_points(&self) -> usize;

    fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    );

    fn point_level(&self, point_id: PointOffsetType) -> usize;

    #[cfg(test)]
    fn links_vec(&self, point_id: PointOffsetType, level: usize) -> Vec<PointOffsetType> {
        let mut links = Vec::new();
        self.for_each_link(point_id, level, |link| links.push(link));
        links
    }
}

#[derive(Debug)]
pub struct GraphLinksRam {
    data: Vec<u8>,
    header: GraphLinksFileHeader,
    level_offsets: Vec<u64>,
}

impl GraphLinksRam {
    fn from_bytes(data: Vec<u8>) -> Self {
        let header = GraphLinksFileHeader::deserialize_bytes_from(&data);
        let level_offsets = get_level_offsets(&data, &header).to_vec();
        Self {
            data,
            header,
            level_offsets,
        }
    }

    fn view(&self) -> GraphLinksView {
        GraphLinksView {
            data: &self.data,
            header: &self.header,
            level_offsets: &self.level_offsets,
        }
    }
}

impl GraphLinks for GraphLinksRam {
    fn load_from_file(path: &Path) -> OperationResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let len = file.metadata()?.len();

        let mut data = Vec::new();
        data.try_set_capacity_exact(len as usize)?;
        file.take(len).read_to_end(&mut data)?;

        Ok(Self::from_bytes(data))
    }

    fn from_converter(converter: GraphLinksConverter) -> OperationResult<Self> {
        let mut data = vec![0; converter.data_size() as usize];
        converter.serialize_to(&mut data);
        drop(converter);

        Ok(Self::from_bytes(data))
    }

    fn num_points(&self) -> usize {
        self.header.point_count as usize
    }

    fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        self.view().for_each_link(point_id, level, f)
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.view().point_level(point_id)
    }
}

#[derive(Debug)]
pub struct GraphLinksMmap {
    mmap: Arc<Mmap>,
    header: GraphLinksFileHeader,
    level_offsets: Vec<u64>,
}

impl GraphLinksMmap {
    pub fn prefault_mmap_pages(&self, path: &Path) -> mmap_ops::PrefaultMmapPages {
        mmap_ops::PrefaultMmapPages::new(Arc::clone(&self.mmap), Some(path))
    }

    fn view(&self) -> GraphLinksView {
        GraphLinksView {
            data: &self.mmap,
            header: &self.header,
            level_offsets: &self.level_offsets,
        }
    }
}

impl GraphLinks for GraphLinksMmap {
    fn load_from_file(path: &Path) -> OperationResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;

        let mmap = unsafe { Mmap::map(&file)? };
        madvise::madvise(&mmap, madvise::get_global())?;

        let header = GraphLinksFileHeader::deserialize_bytes_from(&mmap);
        let level_offsets = get_level_offsets(&mmap, &header).to_vec();

        Ok(Self {
            mmap: Arc::new(mmap),
            header,
            level_offsets,
        })
    }

    fn from_converter(converter: GraphLinksConverter) -> OperationResult<Self> {
        if let Some(path) = converter.path {
            Self::load_from_file(&path)
        } else {
            Err(OperationError::service_error(
                "HNSW links Data needs to be saved to file before it can be loaded as mmap",
            ))
        }
    }

    fn num_points(&self) -> usize {
        self.header.point_count as usize
    }

    fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        self.view().for_each_link(point_id, level, f)
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.view().point_level(point_id)
    }
}

#[derive(Debug)]
struct GraphLinksView<'a> {
    data: &'a [u8],
    header: &'a GraphLinksFileHeader,
    level_offsets: &'a [u64],
}

impl<'a> GraphLinksView<'a> {
    fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        mut f: impl FnMut(PointOffsetType),
    ) {
        let idx = if level == 0 {
            point_id as usize
        } else {
            self.level_offsets[level] as usize + self.reindex(point_id) as usize
        };
        let links_range = self.get_links_range(idx);
        for &link in self.get_links(links_range) {
            f(link);
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
                // incorrect level because this level is larger that available levels
                return level - 1;
            }
        }
        unreachable!()
    }

    fn get_level_offsets_range(&self, level: usize) -> Option<Range<usize>> {
        if level < self.level_offsets.len() {
            let layer_offsets_start = self.level_offsets[level] as usize;
            let layer_offsets_end = if level + 1 < self.level_offsets.len() {
                // `level` is not last, next level_offsets is end of range
                self.level_offsets[level + 1] as usize
            } else {
                // `level` is last, next `offsets.len()` is end of range
                self.header.get_offsets_range().len() / size_of::<u64>() - 1
            };
            Some(layer_offsets_start..layer_offsets_end)
        } else {
            None
        }
    }

    fn get_links_offset(offsets_data: &[u8], idx: usize) -> usize {
        let begin = size_of::<u64>() * idx;
        let end = begin + size_of::<u64>();
        let bytes = &offsets_data[begin..end];
        // unwrap is safe because we know that bytes slice is always 8 bytes
        let bytes: [u8; 8] = bytes.try_into().unwrap();
        u64::from_ne_bytes(bytes) as usize
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        let offsets_range = self.header.get_offsets_range();
        let data: &[u8] = &self.data[offsets_range];
        Self::get_links_offset(data, idx)..Self::get_links_offset(data, idx + 1)
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        let reindex_range = self.header.get_reindex_range();
        let reindex_byte_slice = &self.data[reindex_range];
        mmap_ops::transmute_from_u8_to_slice(reindex_byte_slice)[point_id as usize]
    }

    fn get_links(&self, range: Range<usize>) -> &'a [PointOffsetType] {
        let links_range = self.header.get_links_range();
        let links_byte_slice = &self.data[links_range];
        &mmap_ops::transmute_from_u8_to_slice(links_byte_slice)[range]
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use tempfile::Builder;

    use super::*;

    fn to_vec<TGraphLinks: GraphLinks>(links: &TGraphLinks) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut result = Vec::new();
        let num_points = links.num_points();
        for i in 0..num_points {
            let mut layers = Vec::new();
            let num_levels = links.point_level(i as PointOffsetType) + 1;
            for level in 0..num_levels {
                let links = links.links_vec(i as PointOffsetType, level);
                layers.push(links);
            }
            result.push(layers);
        }
        result
    }

    fn random_links(
        points_count: usize,
        max_levels_count: usize,
    ) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut rng = rand::thread_rng();
        (0..points_count)
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
            .collect()
    }

    /// Test that random links can be saved by `GraphLinksConverter` and loaded correctly by a GraphLinks impl.
    fn test_save_load<A>(points_count: usize, max_levels_count: usize)
    where
        A: GraphLinks,
    {
        let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_file = path.path().join("links.bin");
        let links = random_links(points_count, max_levels_count);
        {
            let mut links_converter = GraphLinksConverter::new(links.clone());
            links_converter.save_as(&links_file).unwrap();
        }
        let cmp_links = to_vec(&A::load_from_file(&links_file).unwrap());
        assert_eq!(links, cmp_links);
    }

    #[test]
    fn test_graph_links_construction() {
        // no points
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 2 points without any links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![]], vec![vec![]]];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // one link at level 0
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![1]], vec![vec![0]]];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 3 levels with no links at second level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2], vec![], vec![2]],
            vec![vec![0, 1], vec![], vec![1]],
        ];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 3 levels with no links at last level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2], vec![2], vec![]],
            vec![vec![0, 2], vec![1], vec![]],
            vec![vec![0, 1]],
        ];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // 4 levels with random nonexistent links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2, 5, 6]],
            vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
            vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
            vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
            vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
        ];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);

        // fully random links
        let links = random_links(100, 10);
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone())).unwrap(),
        );
        assert_eq!(links, cmp_links);
    }

    #[test]
    fn test_graph_links_mmap_ram_compatibility() {
        test_save_load::<GraphLinksRam>(1000, 10);
        test_save_load::<GraphLinksMmap>(1000, 10);
    }
}
