use std::cmp::Reverse;
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Write};
use std::mem::take;
use std::path::Path;
use std::sync::Arc;

use common::bitpacking::packed_bits;
use common::bitpacking_links::{for_each_packed_link, pack_links, MIN_BITS_PER_VALUE};
use common::bitpacking_ordered;
use common::types::PointOffsetType;
use common::zeros::WriteZerosExt as _;
use itertools::{Either, Itertools as _};
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

#[derive(Debug)]
struct GraphLinksView<'a> {
    reindex: &'a [PointOffsetType],
    compression: CompressionInfo<'a>,
    /// Level offsets, copied into RAM for faster access.
    /// Has at least two elements:
    /// - `GraphLinksConverter` always writes `0` as the first element.
    /// - Additional element is added during deserialization.
    level_offsets: Vec<u64>,
}

#[derive(Debug)]
enum CompressionInfo<'a> {
    Uncompressed {
        links: &'a [u32],
        offsets: &'a [u64],
    },
    Compressed {
        compressed_links: &'a [u8],
        offsets: bitpacking_ordered::Reader<'a>,
        m: usize,
        m0: usize,
        bits_per_unsorted: u8,
    },
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
#[repr(C, align(8))]
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
    offsets_parameters: bitpacking_ordered::Parameters,
    m: U64,
    m0: U64,
    zero_padding: [u8; 5],
}

const HEADER_VERSION_COMPRESSED: u64 = 0xFFFF_FFFF_FFFF_FF01;

impl GraphLinksView<'_> {
    fn load(data: &[u8]) -> OperationResult<GraphLinksView> {
        let levels_count_or_version = data
            .get(size_of::<u64>()..)
            .and_then(|x| U64::ref_from_prefix(x).ok())
            .ok_or_else(Self::error_unsufficent_size)?
            .0
            .get();

        match levels_count_or_version {
            // Header for the plain format lacks the version field, but we can
            // be sure that it contains no more than 2^32 levels.
            _ if u64::from_le(levels_count_or_version) <= 1 << 32 => Self::load_plain(data),
            HEADER_VERSION_COMPRESSED => Self::load_compressed(data),
            _ => Err(OperationError::service_error(
                "Unsupported version of GraphLinks file",
            )),
        }
    }

    fn load_plain(data: &[u8]) -> OperationResult<GraphLinksView> {
        let (header, data) =
            HeaderPlain::ref_from_prefix(data).map_err(|_| Self::error_unsufficent_size())?;
        let (level_offsets, data) =
            Self::read_level_offsets(data, header.levels_count, header.total_offset_count)?;
        let (reindex, data) = Self::get_slice::<PointOffsetType>(data, header.point_count)?;
        let (links, data) = Self::get_slice::<u32>(data, header.total_links_count)?;
        let (_, data) = Self::get_slice::<u8>(data, header.offsets_padding_bytes)?;
        let (offsets, _bytes) = Self::get_slice::<u64>(data, header.total_offset_count)?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Uncompressed { links, offsets },
            level_offsets,
        })
    }

    fn load_compressed(data: &[u8]) -> OperationResult<GraphLinksView> {
        let (header, data) =
            HeaderCompressed::ref_from_prefix(data).map_err(|_| Self::error_unsufficent_size())?;
        debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
        let (level_offsets, data) = Self::read_level_offsets(
            data,
            header.levels_count.get(),
            header.offsets_parameters.length.get(),
        )?;
        let (reindex, data) = Self::get_slice::<PointOffsetType>(data, header.point_count.get())?;
        let (compressed_links, data) = Self::get_slice::<u8>(data, header.total_links_bytes.get())?;
        let (offsets, _bytes) = bitpacking_ordered::Reader::new(header.offsets_parameters, data)
            .map_err(|e| {
                OperationError::service_error(format!("Can't create decompressor: {e}"))
            })?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Compressed {
                compressed_links,
                offsets,
                m: header.m.get() as usize,
                m0: header.m0.get() as usize,
                bits_per_unsorted: MIN_BITS_PER_VALUE.max(packed_bits(
                    u32::try_from(header.point_count.get().saturating_sub(1)).map_err(|_| {
                        OperationError::service_error("Too many points in GraphLinks file")
                    })?,
                )),
            },
            level_offsets,
        })
    }

    fn read_level_offsets(
        bytes: &[u8],
        levels_count: u64,
        total_offset_count: u64,
    ) -> OperationResult<(Vec<u64>, &[u8])> {
        let (level_offsets, bytes) = Self::get_slice::<u64>(bytes, levels_count)?;
        let mut result = Vec::with_capacity(level_offsets.len() + 1);
        result.extend_from_slice(level_offsets);
        result.push(total_offset_count.checked_sub(1).ok_or_else(|| {
            OperationError::service_error(
                "Total offset count should be at least 1 in GraphLinks file",
            )
        })?);
        Ok((result, bytes))
    }

    fn get_slice<T: FromBytes + Immutable>(
        data: &[u8],
        length: u64,
    ) -> OperationResult<(&[T], &[u8])> {
        <[T]>::ref_from_prefix_with_elems(data, length as usize)
            .map_err(|_| Self::error_unsufficent_size())
    }

    fn error_unsufficent_size() -> OperationError {
        OperationError::service_error("Unsufficent file size for GraphLinks file")
    }

    fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        let idx = if level == 0 {
            point_id as usize
        } else {
            self.level_offsets[level] as usize + self.reindex[point_id as usize] as usize
        };

        match self.compression {
            CompressionInfo::Uncompressed { links, offsets } => {
                let links_range = offsets[idx] as usize..offsets[idx + 1] as usize;
                links[links_range].iter().copied().for_each(f)
            }
            CompressionInfo::Compressed {
                compressed_links,
                ref offsets,
                m,
                m0,
                bits_per_unsorted,
            } => {
                let links_range =
                    offsets.get(idx).unwrap() as usize..offsets.get(idx + 1).unwrap() as usize;
                for_each_packed_link(
                    &compressed_links[links_range],
                    bits_per_unsorted,
                    if level == 0 { m0 } else { m },
                    f,
                );
            }
        }
    }

    fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed_point_id = u64::from(self.reindex[point_id as usize]);
        for (level, (&a, &b)) in self
            .level_offsets
            .iter()
            .skip(1)
            .tuple_windows()
            .enumerate()
        {
            if reindexed_point_id >= b - a {
                return level;
            }
        }
        // See the doc comment on `level_offsets`.
        self.level_offsets.len() - 2
    }
}

pub struct GraphLinksConverter {
    m: usize,
    m0: usize,
    links: Vec<u8>,
    kind: GraphLinksConverterKind,
    reindex: Vec<PointOffsetType>,
    level_offsets: Vec<u64>,
}

enum GraphLinksConverterKind {
    Uncompressed {
        offsets_padding: usize,
        offsets: Vec<u64>,
    },
    Compressed {
        compressed_offsets: Vec<u8>,
        offsets_parameters: bitpacking_ordered::Parameters,
    },
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

        let kind = if compressed {
            let (compressed_offsets, offsets_parameters) = bitpacking_ordered::compress(&offsets);
            GraphLinksConverterKind::Compressed {
                compressed_offsets,
                offsets_parameters,
            }
        } else {
            let len = links.len() + reindex.as_bytes().len();
            GraphLinksConverterKind::Uncompressed {
                offsets_padding: len.next_multiple_of(size_of::<u64>()) - len,
                offsets,
            }
        };

        Self {
            m,
            m0,
            links,
            kind,
            reindex,
            level_offsets,
        }
    }

    pub fn to_graph_links_ram(&self) -> GraphLinks {
        let size = self.level_offsets.as_bytes().len()
            + self.reindex.as_bytes().len()
            + self.links.len()
            + (match &self.kind {
                GraphLinksConverterKind::Uncompressed {
                    offsets_padding: padding,
                    offsets,
                } => size_of::<HeaderPlain>() + padding + offsets.as_bytes().len(),
                GraphLinksConverterKind::Compressed {
                    compressed_offsets,
                    offsets_parameters: _,
                } => size_of::<HeaderCompressed>() + compressed_offsets.len(),
            });

        let mut data = Vec::with_capacity(size);
        // Unwrap should be the safe as `impl Write` for `Vec` never fails.
        self.serialize_to_writer(&mut data).unwrap();
        debug_assert_eq!(data.len(), size);
        // Unwrap should be safe as we just created the data.
        GraphLinks::try_new(GraphLinksEnum::Ram(data), |x| x.load_view()).unwrap()
    }

    fn serialize_to_writer(&self, writer: &mut impl Write) -> std::io::Result<()> {
        match &self.kind {
            GraphLinksConverterKind::Uncompressed {
                offsets_padding,
                offsets,
            } => {
                let header = HeaderPlain {
                    point_count: self.reindex.len() as u64,
                    levels_count: self.level_offsets.len() as u64,
                    total_links_count: self.links.len() as u64
                        / size_of::<PointOffsetType>() as u64,
                    total_offset_count: offsets.len() as u64,
                    offsets_padding_bytes: *offsets_padding as u64,
                    zero_padding: [0; 24],
                };
                writer.write_all(header.as_bytes())?;
            }
            GraphLinksConverterKind::Compressed {
                compressed_offsets: _,
                offsets_parameters,
            } => {
                let header = HeaderCompressed {
                    version: HEADER_VERSION_COMPRESSED.into(),
                    point_count: U64::new(self.reindex.len() as u64),
                    total_links_bytes: U64::new(self.links.len() as u64),
                    offsets_parameters: *offsets_parameters,
                    levels_count: U64::new(self.level_offsets.len() as u64),
                    m: U64::new(self.m as u64),
                    m0: U64::new(self.m0 as u64),
                    zero_padding: [0; 5],
                };
                writer.write_all(header.as_bytes())?;
            }
        }

        writer.write_all(self.level_offsets.as_bytes())?;
        writer.write_all(self.reindex.as_bytes())?;
        writer.write_all(&self.links)?;
        match &self.kind {
            GraphLinksConverterKind::Uncompressed {
                offsets_padding: padding,
                offsets,
            } => {
                writer.write_zeros(*padding)?;
                writer.write_all(offsets.as_bytes())?;
            }
            GraphLinksConverterKind::Compressed {
                compressed_offsets,
                offsets_parameters: _,
            } => {
                writer.write_all(compressed_offsets)?;
            }
        }

        Ok(())
    }

    pub fn save_as(&self, path: &Path) -> OperationResult<()> {
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

    let links = GraphLinks::load_from_file(path, true)?;
    if links.compressed() {
        return Ok(());
    }

    let original_size = path.metadata()?.len();
    GraphLinksConverter::new(links.into_edges(), true, m, m0).save_as(path)?;
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

self_cell::self_cell! {
    pub struct GraphLinks {
        owner: GraphLinksEnum,
        #[covariant]
        dependent: GraphLinksView,
    }

    impl {Debug}
}

#[derive(Debug)]
enum GraphLinksEnum {
    Ram(Vec<u8>),
    Mmap(Arc<Mmap>),
}

impl GraphLinksEnum {
    fn load_view(&self) -> OperationResult<GraphLinksView> {
        let data = match self {
            GraphLinksEnum::Ram(data) => data.as_slice(),
            GraphLinksEnum::Mmap(mmap) => &mmap[..],
        };
        GraphLinksView::load(data)
    }
}

impl GraphLinks {
    pub fn load_from_file(path: &Path, on_disk: bool) -> OperationResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        if on_disk {
            let len = file.metadata()?.len();
            let mut data = Vec::new();
            data.try_set_capacity_exact(len as usize)?;
            file.take(len).read_to_end(&mut data)?;
            Self::try_new(GraphLinksEnum::Ram(data), |x| x.load_view())
        } else {
            let mmap = unsafe { Mmap::map(&file)? };
            madvise::madvise(&mmap, madvise::get_global())?;
            Self::try_new(GraphLinksEnum::Mmap(Arc::new(mmap)), |x| x.load_view())
        }
    }

    fn view(&self) -> &GraphLinksView {
        self.borrow_dependent()
    }

    pub fn compressed(&self) -> bool {
        matches!(self.view().compression, CompressionInfo::Compressed { .. })
    }

    pub fn on_disk(&self) -> bool {
        matches!(self.borrow_owner(), GraphLinksEnum::Ram(_))
    }

    pub fn num_points(&self) -> usize {
        self.view().reindex.len()
    }

    pub fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        self.view().for_each_link(point_id, level, f)
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.view().point_level(point_id)
    }

    pub fn links_vec(&self, point_id: PointOffsetType, level: usize) -> Vec<PointOffsetType> {
        let mut links = Vec::new();
        self.for_each_link(point_id, level, |link| links.push(link));
        links
    }

    /// Convert the graph links to a vector of edges, suitable for passing into
    /// [`GraphLinksConverter::new`] or using in tests.
    pub fn into_edges(self) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut edges = Vec::with_capacity(self.num_points());
        for point_id in 0..self.num_points() {
            let num_levels = self.point_level(point_id as PointOffsetType) + 1;
            let mut levels = Vec::with_capacity(num_levels);
            for level in 0..num_levels {
                levels.push(self.links_vec(point_id as PointOffsetType, level));
            }
            edges.push(levels);
        }
        edges
    }

    pub fn prefault_mmap_pages(&self, path: &Path) -> Option<mmap_ops::PrefaultMmapPages> {
        match self.borrow_owner() {
            GraphLinksEnum::Mmap(mmap) => Some(mmap_ops::PrefaultMmapPages::new(
                Arc::clone(mmap),
                Some(path.to_owned()),
            )),
            GraphLinksEnum::Ram(_) => None,
        }
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
    fn test_save_load(
        points_count: usize,
        max_levels_count: usize,
        on_disk: bool,
        compressed: bool,
        m: usize,
        m0: usize,
    ) {
        let path = Builder::new().prefix("graph_dir").tempdir().unwrap();
        let links_file = path.path().join("links.bin");
        let links = random_links(points_count, max_levels_count, m, m0);
        GraphLinksConverter::new(links.clone(), compressed, m, m0)
            .save_as(&links_file)
            .unwrap();
        let cmp_links = GraphLinks::load_from_file(&links_file, on_disk)
            .unwrap()
            .into_edges();
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
            GraphLinksConverter::new(links, compressed, m, m0)
                .to_graph_links_ram()
                .into_edges()
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
        test_save_load(1000, 10, true, true, m, m0);
        test_save_load(1000, 10, false, true, m, m0);
        test_save_load(1000, 10, true, false, m, m0);
        test_save_load(1000, 10, false, false, m, m0);
    }
}
