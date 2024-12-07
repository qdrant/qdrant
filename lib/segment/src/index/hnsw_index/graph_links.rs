use std::cmp::Reverse;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::types::PointOffsetType;
use common::zeros::WriteZerosExt as _;
use memmap2::Mmap;
use memory::{madvise, mmap_ops};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

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
struct GraphLinksFileInfo {
    point_count: usize,
    reindex_start: usize,
    links_start: usize,
    offsets_start: usize,
    offsets_end: usize,
}

#[derive(AsBytes, FromBytes, FromZeroes)]
#[repr(C)]
struct GraphLinksFileHeader {
    point_count: u64,
    levels_count: u64,
    total_links_len: u64,
    total_offsets_len: u64,
    offsets_padding: u64, // 0 or 4
}

impl GraphLinksFileInfo {
    pub fn load(data: &[u8]) -> Option<GraphLinksFileInfo> {
        let header = GraphLinksFileHeader::ref_from_prefix(data)?;

        let reindex_start = HEADER_SIZE + header.levels_count as usize * size_of::<u64>();
        let links_start =
            reindex_start + header.point_count as usize * size_of::<PointOffsetType>();
        let offsets_start = links_start
            + header.total_links_len as usize * size_of::<PointOffsetType>()
            + header.offsets_padding as usize;
        let offsets_end = offsets_start + header.total_offsets_len as usize * size_of::<u64>();

        Some(GraphLinksFileInfo {
            point_count: header.point_count as usize,
            reindex_start,
            links_start,
            offsets_start,
            offsets_end,
        })
    }

    pub fn level_offsets(&self) -> Range<usize> {
        HEADER_SIZE..self.reindex_start
    }

    pub fn get_level_offsets<'a>(&self, data: &'a [u8]) -> &'a [u64] {
        u64::slice_from(&data[self.level_offsets()]).unwrap()
    }

    pub fn reindex_range(&self) -> Range<usize> {
        self.reindex_start..self.links_start
    }

    pub fn links_range(&self) -> Range<usize> {
        self.links_start..self.offsets_start
    }

    pub fn offsets_range(&self) -> Range<usize> {
        self.offsets_start..self.offsets_end
    }
}

pub struct GraphLinksConverter {
    edges: Vec<Vec<Vec<PointOffsetType>>>,
    reindex: Vec<PointOffsetType>,
    back_index: Vec<usize>,
    total_links_len: usize,
    total_offsets_len: usize,
    path: Option<PathBuf>,
    level_offsets: Vec<u64>,
    point_count_by_level: Vec<u64>,
}

impl GraphLinksConverter {
    pub fn new(edges: Vec<Vec<Vec<PointOffsetType>>>, _compressed: bool, _m: usize) -> Self {
        if edges.is_empty() {
            return Self {
                edges,
                reindex: Vec::new(),
                back_index: Vec::new(),
                total_links_len: 0,
                total_offsets_len: 1,
                path: None,
                level_offsets: Vec::new(),
                point_count_by_level: Vec::new(),
            };
        }

        // create map from index in `offsets` to point_id
        let mut back_index: Vec<usize> = (0..edges.len()).collect();
        // sort by max layer and use this map to build `Self.reindex`
        back_index.sort_unstable_by_key(|&i| Reverse(edges[i].len()));

        // `reindex` is map from point id to index in `Self.offsets`
        let mut reindex = vec![0; back_index.len()];
        for i in 0..back_index.len() {
            reindex[back_index[i]] = i as PointOffsetType;
        }

        let levels_count = back_index
            .first()
            .map_or(0, |&point_id| edges[point_id].len());
        let mut point_count_by_level = vec![0; levels_count];

        // estimate size of `links` and `offsets`
        let mut total_links_len = 0;
        for point in edges.iter() {
            point_count_by_level[point.len() - 1] += 1;
            total_links_len += point.iter().map(Vec::len).sum::<usize>();
        }

        let mut total_offsets_len = 0;
        let mut suffix_sum = point_count_by_level.iter().sum::<u64>();
        let mut level_offsets = Vec::with_capacity(levels_count);
        for &value in point_count_by_level.iter() {
            level_offsets.push(total_offsets_len);
            total_offsets_len += suffix_sum;
            suffix_sum -= value;
        }
        total_offsets_len += 1;

        Self {
            edges,
            reindex,
            back_index,
            total_links_len,
            total_offsets_len: total_offsets_len as usize,
            path: None,
            level_offsets,
            point_count_by_level,
        }
    }

    /// Size of compacted graph in bytes.
    pub fn data_size(&self) -> usize {
        HEADER_SIZE
            + self.point_count_by_level.len() * size_of::<u64>()
            + self.reindex.len() * size_of::<PointOffsetType>()
            + self.total_links_len * size_of::<PointOffsetType>()
            + self.offsets_padding()
            + self.total_offsets_len * size_of::<u64>()
    }

    fn offsets_padding(&self) -> usize {
        (self.total_links_len + self.reindex.len()) % 2 * size_of::<u32>()
    }

    fn serialize_to_vec(&self) -> Vec<u8> {
        let size = self.data_size();
        let mut data = Vec::with_capacity(size);
        // Unwrap should be the safe as `impl Write` for `Vec` never fails.
        self.serialize_to_writer(&mut data).unwrap();
        debug_assert_eq!(data.len(), size);
        data
    }

    fn serialize_to_writer(&self, writer: &mut impl Write) -> std::io::Result<()> {
        let header = GraphLinksFileHeader {
            point_count: self.reindex.len() as u64,
            levels_count: self.point_count_by_level.len() as u64,
            total_links_len: self.total_links_len as u64,
            total_offsets_len: self.total_offsets_len as u64,
            offsets_padding: self.offsets_padding() as u64,
        };

        // 1. header
        writer.write_all(header.as_bytes())?;

        // 2. header padding
        writer.write_zeros(HEADER_SIZE - size_of::<GraphLinksFileHeader>())?;

        // 3. level_offsets
        writer.write_all(self.level_offsets.as_bytes())?;

        // 4. reindex
        writer.write_all(self.reindex.as_bytes())?;

        let mut offsets = Vec::with_capacity(header.total_offsets_len as usize);
        offsets.push(0);

        // 5. links
        let mut links_pos = 0;
        let mut write_links = |links: &[PointOffsetType]| {
            writer.write_all(links.as_bytes())?;
            links_pos += links.len();
            offsets.push(links_pos as u64);
            std::io::Result::Ok(())
        };
        for point in &self.edges {
            write_links(&point[0])?;
        }
        for level in 1..header.levels_count as usize {
            let count = self.point_count_by_level.iter().skip(level).sum::<u64>() as usize;
            for i in 0..count {
                write_links(&self.edges[self.back_index[i]][level])?;
            }
        }

        debug_assert_eq!(links_pos, self.total_links_len);

        // 6. padding for offsets
        writer.write_zeros(self.offsets_padding())?;

        // 7. offsets
        writer.write_all(offsets.as_bytes())?;

        Ok(())
    }

    pub fn save_as(&mut self, path: &Path) -> OperationResult<()> {
        self.path = Some(path.to_path_buf());
        let temp_path = path.with_extension("tmp");
        let file = File::create(temp_path.as_path())?;
        let mut buf = std::io::BufWriter::new(file);
        self.serialize_to_writer(&mut buf)?;
        std::fs::rename(temp_path, path)?;
        Ok(())
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
    info: GraphLinksFileInfo,
    level_offsets: Vec<u64>,
}

impl GraphLinksRam {
    fn from_bytes(data: Vec<u8>) -> Self {
        let info = GraphLinksFileInfo::load(&data).unwrap();
        let level_offsets = info.get_level_offsets(&data).to_vec();
        Self {
            data,
            info,
            level_offsets,
        }
    }

    fn view(&self) -> GraphLinksView {
        GraphLinksView {
            data: &self.data,
            info: &self.info,
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
        Ok(Self::from_bytes(converter.serialize_to_vec()))
    }

    fn num_points(&self) -> usize {
        self.info.point_count
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
    info: GraphLinksFileInfo,
    level_offsets: Vec<u64>,
}

impl GraphLinksMmap {
    pub fn prefault_mmap_pages(&self, path: &Path) -> mmap_ops::PrefaultMmapPages {
        mmap_ops::PrefaultMmapPages::new(Arc::clone(&self.mmap), Some(path))
    }

    fn view(&self) -> GraphLinksView {
        GraphLinksView {
            data: &self.mmap,
            info: &self.info,
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

        let info = GraphLinksFileInfo::load(&mmap).unwrap();
        let level_offsets = info.get_level_offsets(&mmap).to_vec();

        Ok(Self {
            mmap: Arc::new(mmap),
            info,
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
        self.info.point_count
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
    info: &'a GraphLinksFileInfo,
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
                self.info.offsets_range().len() / size_of::<u64>() - 1
            };
            Some(layer_offsets_start..layer_offsets_end)
        } else {
            None
        }
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        let offsets = u64::slice_from(&self.data[self.info.offsets_range()]).unwrap();
        offsets[idx] as usize..offsets[idx + 1] as usize
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        let idx = &self.data[self.info.reindex_range()];
        PointOffsetType::slice_from(idx).unwrap()[point_id as usize]
    }

    fn get_links(&self, range: Range<usize>) -> &'a [PointOffsetType] {
        let idx = &self.data[self.info.links_range()];
        PointOffsetType::slice_from(idx)
            .unwrap()
            .get(range)
            .unwrap()
    }
}

/// Sort the first `m` values in `links` and return them. Used to compare stored
/// links where the order of the first `m` links is not preserved.
#[cfg(test)]
pub(super) fn normalize_links(m: usize, mut links: Vec<PointOffsetType>) -> Vec<PointOffsetType> {
    let first = links.len().min(m);
    links[..first].sort_unstable();
    links
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rstest::rstest;
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
        m: usize,
    ) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut rng = rand::thread_rng();
        (0..points_count)
            .map(|_| {
                let levels_count = rng.gen_range(1..max_levels_count);
                (0..levels_count)
                    .map(|_| {
                        let links_count = rng.gen_range(0..m * 4);
                        (0..links_count)
                            .map(|_| rng.gen_range(0..points_count) as PointOffsetType)
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    fn compare_links(
        mut left: Vec<Vec<Vec<PointOffsetType>>>,
        mut right: Vec<Vec<Vec<PointOffsetType>>>,
        m: Option<usize>,
    ) {
        let m = m.unwrap_or(0);
        for links in [&mut left, &mut right].iter_mut() {
            links.iter_mut().for_each(|levels| {
                levels
                    .iter_mut()
                    .enumerate()
                    .for_each(|(level_idx, links)| {
                        *links = normalize_links(
                            if level_idx == 0 { m * 2 } else { m },
                            std::mem::take(links),
                        );
                    })
            });
        }
        assert_eq!(left, right);
    }

    /// Test that random links can be saved by `GraphLinksConverter` and loaded correctly by a GraphLinks impl.
    fn test_save_load<A>(points_count: usize, max_levels_count: usize, compressed: bool, m: usize)
    where
        A: GraphLinks,
    {
        let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_file = path.path().join("links.bin");
        let links = random_links(points_count, max_levels_count, m);
        {
            let mut links_converter = GraphLinksConverter::new(links.clone(), compressed, m);
            links_converter.save_as(&links_file).unwrap();
        }
        let cmp_links = to_vec(&A::load_from_file(&links_file).unwrap());
        compare_links(links, cmp_links, compressed.then_some(m));
    }

    #[rstest]
    #[case::uncompressed(false)]
    #[case::compressed(true)]
    fn test_graph_links_construction(#[case] compressed: bool) {
        let m = 2;

        // no points
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, m))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(m));

        // 2 points without any links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![]], vec![vec![]]];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, m))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(m));

        // one link at level 0
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![1]], vec![vec![0]]];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, m))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(m));

        // 3 levels with no links at second level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2], vec![], vec![2]],
            vec![vec![0, 1], vec![], vec![1]],
        ];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, m))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(m));

        // 3 levels with no links at last level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2], vec![2], vec![]],
            vec![vec![0, 2], vec![1], vec![]],
            vec![vec![0, 1]],
        ];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, m))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(m));

        // 4 levels with random nonexistent links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2, 5, 6]],
            vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
            vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
            vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
            vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
        ];
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, m))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(m));

        // fully random links
        let links = random_links(100, 10, 8);
        let cmp_links = to_vec(
            &GraphLinksRam::from_converter(GraphLinksConverter::new(links.clone(), compressed, 8))
                .unwrap(),
        );
        compare_links(links, cmp_links, compressed.then_some(8));
    }

    #[test]
    fn test_graph_links_mmap_ram_compatibility() {
        test_save_load::<GraphLinksRam>(1000, 10, true, 8);
        test_save_load::<GraphLinksMmap>(1000, 10, true, 8);
        test_save_load::<GraphLinksRam>(1000, 10, false, 8);
        test_save_load::<GraphLinksMmap>(1000, 10, false, 8);
    }
}
