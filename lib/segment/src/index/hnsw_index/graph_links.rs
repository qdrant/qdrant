use std::cmp::Reverse;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write};
use std::mem::take;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::bitpacking::packed_bits;
use common::bitpacking_links::{for_each_packed_link, pack_links, MIN_BITS_PER_VALUE};
use common::types::PointOffsetType;
use common::zeros::WriteZerosExt as _;
use itertools::Either;
use memmap2::Mmap;
use memory::{madvise, mmap_ops};
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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

#[derive(Clone, Debug)]
struct GraphLinksFileInfo {
    point_count: usize,
    reindex_start: usize,
    links_start: usize,
    offsets_start: usize,
    offsets_end: usize,
    compression: Option<CompressionInfo>,
}

#[derive(Clone, Debug)]
struct CompressionInfo {
    m: usize,
    m0: usize,
    bits_per_unsorted: u8,
}

/// File header for the plain format.
#[derive(FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
struct HeaderPlain {
    point_count: u64,
    levels_count: u64,
    total_links_count: u64,
    total_offset_count: u64,
    /// Either 0 or 4.
    offsets_padding_bytes: u64,
    zero_padding: [u8; 24],
}

/// File header for the compressed format.
#[derive(FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
struct HeaderCompressed {
    point_count: U64,
    /// Should be [`HEADER_VERSION_COMPRESSED`].
    ///
    /// Deliberately placed at the same offset as [`HeaderPlain::levels_count`]
    /// and set to an impossibly large number to make old Qdrant versions fail
    /// fast when trying to read the new format.
    version: U64,
    levels_count: U64,
    total_links_bytes: U64,
    total_offset_count: U64,
    m: U64,
    m0: U64,
    zero_padding: [u8; 8],
}

const HEADER_VERSION_COMPRESSED: u64 = 0xFFFF_FFFF_FFFF_FF01;

impl GraphLinksFileInfo {
    pub fn load(data: &[u8]) -> OperationResult<GraphLinksFileInfo> {
        let levels_count_or_version = data
            .get(size_of::<u64>()..)
            .and_then(|x| U64::ref_from_prefix(x).ok())
            .ok_or_else(Self::error_unsufficent_size)?
            .0
            .get();

        // Header for the plain format lacks the version field, but we can be
        // sure that it contains no more than 2^32 levels.
        let is_plain = u64::from_le(levels_count_or_version) <= 1 << 32;

        match levels_count_or_version {
            _ if is_plain => {
                let (header, _) = HeaderPlain::ref_from_prefix(data)
                    .map_err(|_| Self::error_unsufficent_size())?;
                let reindex_start = HEADER_SIZE + header.levels_count as usize * size_of::<u64>();
                let links_start =
                    reindex_start + header.point_count as usize * size_of::<PointOffsetType>();
                let offsets_start = links_start
                    + header.total_links_count as usize * size_of::<PointOffsetType>()
                    + header.offsets_padding_bytes as usize;
                Ok(GraphLinksFileInfo {
                    point_count: header.point_count as usize,
                    reindex_start,
                    links_start,
                    offsets_start,
                    offsets_end: offsets_start
                        + header.total_offset_count as usize * size_of::<u64>(),
                    compression: None,
                })
            }
            HEADER_VERSION_COMPRESSED => {
                let (header, _) = HeaderCompressed::ref_from_prefix(data)
                    .map_err(|_| Self::error_unsufficent_size())?;
                debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
                let point_count = header.point_count.get() as usize;
                let reindex_start =
                    HEADER_SIZE + header.levels_count.get() as usize * size_of::<u64>();
                let links_start = reindex_start
                    + header.point_count.get() as usize * size_of::<PointOffsetType>();
                let offsets_start = (links_start + header.total_links_bytes.get() as usize)
                    .next_multiple_of(size_of::<u64>());
                Ok(GraphLinksFileInfo {
                    point_count,
                    reindex_start,
                    links_start,
                    offsets_start,
                    offsets_end: offsets_start
                        + header.total_offset_count.get() as usize * size_of::<u64>(),
                    compression: Some(CompressionInfo {
                        m: header.m.get() as usize,
                        m0: header.m0.get() as usize,
                        bits_per_unsorted: MIN_BITS_PER_VALUE.max(packed_bits(
                            u32::try_from(point_count.saturating_sub(1)).map_err(|_| {
                                OperationError::service_error("Too many points in GraphLinks file")
                            })?,
                        )),
                    }),
                })
            }
            _ => Err(OperationError::service_error(
                "Unsupported version of GraphLinks file",
            )),
        }
    }

    fn error_unsufficent_size() -> OperationError {
        OperationError::service_error("Unsufficent file size for GraphLinks file")
    }

    pub fn level_offsets(&self) -> Range<usize> {
        HEADER_SIZE..self.reindex_start
    }

    pub fn get_level_offsets<'a>(&self, data: &'a [u8]) -> &'a [u64] {
        <[u64]>::ref_from_bytes(&data[self.level_offsets()]).unwrap()
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
    compressed: bool,
    m: usize,
    m0: usize,
    links: Vec<u8>,
    offsets: Vec<u64>,
    reindex: Vec<PointOffsetType>,
    path: Option<PathBuf>,
    level_offsets: Vec<u64>,
    offsets_padding: usize,
}

impl GraphLinksConverter {
    pub fn new(
        mut edges: Vec<Vec<Vec<PointOffsetType>>>,
        compressed: bool,
        m: usize,
        m0: usize,
    ) -> Self {
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
        for point in &edges {
            point_count_by_level[point.len() - 1] += 1;
        }

        let mut total_offsets_len = 0;
        let mut level_offsets = Vec::with_capacity(levels_count);
        let mut suffix_sum = point_count_by_level.iter().sum::<u64>();
        for &value in point_count_by_level.iter() {
            level_offsets.push(total_offsets_len);
            total_offsets_len += suffix_sum;
            suffix_sum -= value;
        }
        total_offsets_len += 1;

        let mut links = Vec::new();
        let mut offsets = Vec::with_capacity(total_offsets_len as usize);
        offsets.push(0);
        let bits_per_unsorted = packed_bits(u32::try_from(edges.len().saturating_sub(1)).unwrap())
            .max(MIN_BITS_PER_VALUE);

        for level in 0..levels_count {
            let count = point_count_by_level.iter().skip(level).sum::<u64>() as usize;
            let (sorted_count, iter) = match level {
                0 => (m0, Either::Left(0..count)),
                _ => (m, Either::Right(back_index[..count].iter().copied())),
            };
            iter.for_each(|id| {
                let raw_links = take(&mut edges[id][level]);
                if compressed {
                    pack_links(&mut links, raw_links, bits_per_unsorted, sorted_count);
                    offsets.push(links.len() as u64);
                } else {
                    links.extend_from_slice(raw_links.as_bytes());
                    offsets.push((links.len() as u64) / size_of::<PointOffsetType>() as u64);
                }
            });
        }

        let offsets_padding = {
            let len = links.len() + reindex.as_bytes().len();
            len.next_multiple_of(size_of::<u64>()) - len
        };

        Self {
            compressed,
            m,
            m0,
            links,
            offsets,
            reindex,
            path: None,
            level_offsets,
            offsets_padding,
        }
    }

    /// Size of compacted graph in bytes.
    fn data_size(&self) -> usize {
        HEADER_SIZE
            + self.level_offsets.as_bytes().len()
            + self.reindex.as_bytes().len()
            + self.links.len()
            + self.offsets_padding
            + self.offsets.as_bytes().len()
    }

    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.data_size());
        // Unwrap should be the safe as `impl Write` for `Vec` never fails.
        self.serialize_to_writer(&mut data).unwrap();
        debug_assert_eq!(data.len(), self.data_size());
        data
    }

    fn serialize_to_writer(&self, writer: &mut impl Write) -> std::io::Result<()> {
        if self.compressed {
            let header = HeaderCompressed {
                version: HEADER_VERSION_COMPRESSED.into(),
                point_count: U64::new(self.reindex.len() as u64),
                total_links_bytes: U64::new(self.links.len() as u64),
                total_offset_count: U64::new(self.offsets.len() as u64),
                levels_count: U64::new(self.level_offsets.len() as u64),
                m: U64::new(self.m as u64),
                m0: U64::new(self.m0 as u64),
                zero_padding: [0; 8],
            };
            writer.write_all(header.as_bytes())?;
        } else {
            let header = HeaderPlain {
                point_count: self.reindex.len() as u64,
                levels_count: self.level_offsets.len() as u64,
                total_links_count: self.links.len() as u64 / size_of::<PointOffsetType>() as u64,
                total_offset_count: self.offsets.len() as u64,
                offsets_padding_bytes: self.offsets_padding as u64,
                zero_padding: [0; 24],
            };
            writer.write_all(header.as_bytes())?;
        }

        writer.write_all(self.level_offsets.as_bytes())?;
        writer.write_all(self.reindex.as_bytes())?;
        writer.write_all(&self.links)?;
        writer.write_zeros(self.offsets_padding)?;
        writer.write_all(self.offsets.as_bytes())?;

        Ok(())
    }

    pub fn save_as(&mut self, path: &Path) -> OperationResult<()> {
        self.path = Some(path.to_path_buf());
        let temp_path = path.with_extension("tmp");
        let file = File::create(temp_path.as_path())?;
        let mut buf = std::io::BufWriter::new(&file);
        self.serialize_to_writer(&mut buf)?;
        file.sync_all()?;
        std::fs::rename(temp_path, path)?;
        Ok(())
    }
}

pub fn convert_to_compressed(path: &Path, m: usize, m0: usize) -> OperationResult<()> {
    let start = std::time::Instant::now();

    let links = GraphLinksMmap::load_from_file(path)?;
    if links.info.compression.is_some() {
        return Ok(());
    }

    let edges = (0..links.num_points())
        .map(|point_id| {
            let num_levels = links.point_level(point_id as PointOffsetType) + 1;
            (0..num_levels)
                .map(|level| links.links_vec(point_id as PointOffsetType, level))
                .collect::<Vec<_>>()
        })
        .collect();
    drop(links);
    let mut converter = GraphLinksConverter::new(edges, true, m, m0);

    let original_size = path.metadata()?.len();
    converter.save_as(path)?;
    let new_size = path.metadata()?.len();

    log::debug!(
        "Compressed HNSW graph links in {:.1?}: {:.1}MB -> {:.1}MB ({:.1}%)",
        start.elapsed(),
        original_size as f64 / 1024.0 / 1024.0,
        new_size as f64 / 1024.0 / 1024.0,
        new_size as f64 / original_size as f64 * 100.0,
    );

    Ok(())
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

impl GraphLinksView<'_> {
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
        let all_links = &self.data[self.info.links_range()];

        let offsets = <[u64]>::ref_from_bytes(&self.data[self.info.offsets_range()]).unwrap();
        let offset0 = offsets[idx];
        let offset1 = offsets[idx + 1];

        let links_range = (offset0 as usize)..(offset1 as usize);

        if let Some(compression) = &self.info.compression {
            for_each_packed_link(
                &all_links[links_range],
                compression.bits_per_unsorted,
                if level == 0 {
                    compression.m0
                } else {
                    compression.m
                },
                f,
            );
        } else {
            let all_links = <[PointOffsetType]>::ref_from_bytes(all_links).unwrap();
            for &link in &all_links[links_range] {
                f(link);
            }
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

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        let idx = &self.data[self.info.reindex_range()];
        <[PointOffsetType]>::ref_from_bytes(idx).unwrap()[point_id as usize]
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
        m0: usize,
    ) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut rng = rand::thread_rng();
        (0..points_count)
            .map(|_| {
                let levels_count = rng.gen_range(1..max_levels_count);
                (0..levels_count)
                    .map(|level| {
                        let mut max_links_count = if level == 0 { m0 } else { m };
                        max_links_count *= 2; // Simulate additional payload links.
                        let links_count = rng.gen_range(0..max_links_count);
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
        compressed: bool,
        m: usize,
        m0: usize,
    ) {
        for links in [&mut left, &mut right].iter_mut() {
            links.iter_mut().for_each(|levels| {
                levels
                    .iter_mut()
                    .enumerate()
                    .for_each(|(level_idx, links)| {
                        *links = normalize_links(
                            if compressed {
                                if level_idx == 0 {
                                    m0
                                } else {
                                    m
                                }
                            } else {
                                0
                            },
                            std::mem::take(links),
                        );
                    })
            });
        }
        assert_eq!(left, right);
    }

    /// Test that random links can be saved by `GraphLinksConverter` and loaded correctly by a GraphLinks impl.
    fn test_save_load<A>(
        points_count: usize,
        max_levels_count: usize,
        compressed: bool,
        m: usize,
        m0: usize,
    ) where
        A: GraphLinks,
    {
        let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_file = path.path().join("links.bin");
        let links = random_links(points_count, max_levels_count, m, m0);
        {
            let mut links_converter = GraphLinksConverter::new(links.clone(), compressed, m, m0);
            links_converter.save_as(&links_file).unwrap();
        }
        let cmp_links = to_vec(&A::load_from_file(&links_file).unwrap());
        compare_links(links, cmp_links, compressed, m, m0);
    }

    #[rstest]
    #[case::uncompressed(false)]
    #[case::compressed(true)]
    fn test_graph_links_construction(#[case] compressed: bool) {
        let m = 2;
        let m0 = m * 2;

        let make_cmp_links = |links: Vec<Vec<Vec<PointOffsetType>>>,
                              m: usize,
                              m0: usize|
         -> Vec<Vec<Vec<PointOffsetType>>> {
            to_vec(
                &GraphLinksRam::from_converter(GraphLinksConverter::new(
                    links.clone(),
                    compressed,
                    m,
                    m0,
                ))
                .unwrap(),
            )
        };

        // no points
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![];
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);

        // 2 points without any links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![]], vec![vec![]]];
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);

        // one link at level 0
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![vec![vec![1]], vec![vec![0]]];
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);

        // 3 levels with no links at second level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2], vec![], vec![2]],
            vec![vec![0, 1], vec![], vec![1]],
        ];
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);

        // 3 levels with no links at last level
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2], vec![2], vec![]],
            vec![vec![0, 2], vec![1], vec![]],
            vec![vec![0, 1]],
        ];
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);

        // 4 levels with random nonexistent links
        let links: Vec<Vec<Vec<PointOffsetType>>> = vec![
            vec![vec![1, 2, 5, 6]],
            vec![vec![0, 2, 7, 8], vec![], vec![34, 45, 10]],
            vec![vec![0, 1, 1, 2], vec![3, 5, 9], vec![9, 8], vec![9], vec![]],
            vec![vec![0, 1, 5, 6], vec![1, 5, 0]],
            vec![vec![0, 1, 9, 18], vec![1, 5, 6], vec![5], vec![9]],
        ];
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);

        // fully random links
        let m = 8;
        let m0 = m * 2;
        let links = random_links(100, 10, m, m0);
        let cmp_links = make_cmp_links(links.clone(), m, m0);
        compare_links(links, cmp_links, compressed, m, m0);
    }

    #[test]
    fn test_graph_links_mmap_ram_compatibility() {
        let m = 8;
        let m0 = m * 2;
        test_save_load::<GraphLinksRam>(1000, 10, true, m, m0);
        test_save_load::<GraphLinksMmap>(1000, 10, true, m, m0);
        test_save_load::<GraphLinksRam>(1000, 10, false, m, m0);
        test_save_load::<GraphLinksMmap>(1000, 10, false, m, m0);
    }
}
