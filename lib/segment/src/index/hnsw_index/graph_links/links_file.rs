use std::alloc::Layout;
use std::num::NonZero;

use common::bitpacking_links::{PackedLinksIterator, iterate_packed_links};
use common::bitpacking_ordered;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::{ReadBytesItem, ReadRange, UniversalRead};
use itertools::Itertools;
use zerocopy::FromBytes;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_links::GraphLinksFormat;
use crate::index::hnsw_index::graph_links::header::{
    HEADER_VERSION_COMPRESSED, HEADER_VERSION_COMPRESSED_WITH_VECTORS, HeaderCompressed,
    HeaderCompressedWithVectors,
};
use crate::index::hnsw_index::graph_links::view_utils::{
    bits_per_unsorted, error_size, find_level, last_offset_idx, link_vector_size,
    parse_links_with_vectors,
};

/// UIO-backed counterpart of [super::view::GraphLinksView].
pub struct GraphLinksFile<S: UniversalRead> {
    file: S,
    point_count: u64,
    /// See [`super::view::GraphLinksView::level_offsets`].
    level_offsets: Vec<u64>,
    /// Byte offset of the reindex table ([`PointOffsetType`] per point).
    reindex_offset: u64,
    /// Byte offset of the neighbors data.
    neighbors_offset: u64,
    offsets: bitpacking_ordered::Reader,
    /// Byte offset of the compressed offsets data.
    offsets_offset: u64,
    hnsw_m: HnswM,
    bits_per_unsorted: u8,
    compression: CompressionInfo,
}

#[derive(Debug)]
pub(super) enum CompressionInfo {
    Compressed,
    CompressedWithVectors {
        base_vector_layout: Layout,
        /// `NonZero` to avoid handling unlikely corner cases.
        link_vector_size: NonZero<usize>,
        link_vector_alignment: u8,
    },
}

/// Data parsed from a format-specific header.
struct CommonHeader {
    point_count: u64,
    levels_count: usize,
    total_neighbors_bytes: u64,
    offsets_parameters: bitpacking_ordered::Parameters,
    hnsw_m: HnswM,
    neighbors_alignment: u64,
    compression: CompressionInfo,
}

/// Upper bound of the number of levels in a HNSW graph.
///
/// Most graphs have no more than 8 levels, but to be safe, let's assume the
/// worst case: `N_POINTS = 2**32`, `HNSW_M = 2`, and `CONFIDENCE = 0.999`.
/// `math.log(N_POINTS / -math.log(CONFIDENCE), HNSW_M) - 0.5`
const MAX_HNSW_LEVELS_GUESS: usize = 42;

impl<S: UniversalRead> GraphLinksFile<S> {
    pub fn load(file: S, format: GraphLinksFormat) -> OperationResult<Self> {
        match format {
            GraphLinksFormat::Plain => {
                let err = "Plain graph links are not supported by the batched reader";
                debug_assert!(false, "{err}");
                Err(OperationError::service_error(err))
            }

            GraphLinksFormat::Compressed => Self::load_impl(file, |header: &HeaderCompressed| {
                debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
                Ok(CommonHeader {
                    point_count: header.point_count.get(),
                    levels_count: header.levels_count.get() as usize,
                    total_neighbors_bytes: header.total_neighbors_bytes.get(),
                    offsets_parameters: header.offsets_parameters,
                    hnsw_m: HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                    neighbors_alignment: 1,
                    compression: CompressionInfo::Compressed,
                })
            }),

            GraphLinksFormat::CompressedWithVectors => {
                Self::load_impl(file, |header: &HeaderCompressedWithVectors| {
                    debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED_WITH_VECTORS);
                    let base_vector_layout = header.base_vector_layout.try_into_layout()?;
                    let link_vector_layout = header.link_vector_layout.try_into_layout()?;
                    let alignment =
                        std::cmp::max(base_vector_layout.align(), link_vector_layout.align());
                    Ok(CommonHeader {
                        point_count: header.point_count.get(),
                        levels_count: header.levels_count.get() as usize,
                        total_neighbors_bytes: header.total_neighbors_bytes.get(),
                        offsets_parameters: header.offsets_parameters,
                        hnsw_m: HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                        neighbors_alignment: alignment as u64,
                        compression: CompressionInfo::CompressedWithVectors {
                            base_vector_layout,
                            link_vector_size: link_vector_size(link_vector_layout)?,
                            link_vector_alignment: link_vector_layout.align() as u8,
                        },
                    })
                })
            }
        }
    }

    fn load_impl<H: FromBytes>(
        file: S,
        parse: impl FnOnce(&H) -> OperationResult<CommonHeader>,
    ) -> OperationResult<Self> {
        let file_len = file.len::<u8>()?;

        // Read the header and, hopefully, all level offsets in a single read.
        let guess_size = (size_of::<H>() + MAX_HNSW_LEVELS_GUESS * size_of::<u64>()) as u64;
        let bytes = file.read_bytes(0..guess_size.min(file_len), Random, align_of::<H>())?;
        let (header, level_offsets_bytes) =
            H::read_from_prefix(&bytes).map_err(|_| error_size())?;
        let common = parse(&header)?;

        let mut level_offsets = Vec::with_capacity(common.levels_count + 1);
        if let Ok((offsets, _)) =
            <[u64]>::ref_from_prefix_with_elems(level_offsets_bytes, common.levels_count)
        {
            level_offsets.extend_from_slice(offsets);
        } else {
            // More levels than `MAX_HNSW_LEVELS_GUESS`: fall back to a
            // separate read.
            let range = ReadRange {
                byte_offset: size_of::<H>() as u64,
                length: common.levels_count as u64,
            };
            level_offsets.extend_from_slice(&file.read::<_, u64>(range, Random)?);
        }
        level_offsets.push(last_offset_idx(common.offsets_parameters.length.get())?);

        let reindex_offset =
            size_of::<H>() as u64 + common.levels_count as u64 * size_of::<u64>() as u64;
        let reindex_end = reindex_offset + common.point_count * size_of::<PointOffsetType>() as u64;
        let neighbors_offset = reindex_end.next_multiple_of(common.neighbors_alignment);
        let offsets_offset = neighbors_offset + common.total_neighbors_bytes;
        let offsets = common.offsets_parameters.validate()?;
        if offsets_offset + offsets.compressed_size_bytes() as u64 > file_len {
            return Err(error_size());
        }

        Ok(Self {
            file,
            point_count: common.point_count,
            level_offsets,
            reindex_offset,
            neighbors_offset,
            offsets,
            offsets_offset,
            hnsw_m: common.hnsw_m,
            bits_per_unsorted: bits_per_unsorted(common.point_count)?,
            compression: common.compression,
        })
    }

    pub fn num_points(&self) -> usize {
        self.point_count as usize
    }

    pub fn format(&self) -> GraphLinksFormat {
        match self.compression {
            CompressionInfo::Compressed => GraphLinksFormat::Compressed,
            CompressionInfo::CompressedWithVectors { .. } => {
                GraphLinksFormat::CompressedWithVectors
            }
        }
    }

    /// Returns `true` if the links format contains inline vectors.
    pub fn is_with_vectors(&self) -> bool {
        self.format().is_with_vectors()
    }

    /// See [`super::view::GraphLinksView::point_level`].
    pub fn point_level(&self, point_id: PointOffsetType) -> OperationResult<usize> {
        let range = self.reindex_range(point_id);
        let reindexed = self.file.read::<_, PointOffsetType>(range, Random)?[0];
        Ok(find_level(u64::from(reindexed), &self.level_offsets))
    }

    pub fn populate(&self) -> OperationResult<()> {
        Ok(self.file.populate()?)
    }

    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(self.file.clear_ram_cache()?)
    }

    /// Read links for a batch of points at the given level.
    pub fn links(
        &self,
        arena: &mut stumpalo::Arena,
        point_ids: &[PointOffsetType],
        level: usize,
        mut callback: impl FnMut(usize, PackedLinksIterator<'_>),
    ) -> OperationResult<()> {
        match self.compression {
            CompressionInfo::Compressed => {
                let sorted_count = self.hnsw_m.level_m(level);
                self.read_neighbors(arena, point_ids, level, 1, |position, _start, data| {
                    callback(
                        position,
                        iterate_packed_links(data, self.bits_per_unsorted, sorted_count),
                    );
                    OperationResult::Ok(())
                })
            }
            CompressionInfo::CompressedWithVectors { .. } => self.links_with_vectors(
                arena,
                point_ids,
                level,
                |position, _base_vector, links, _link_vectors| {
                    callback(position, links);
                    Ok(())
                },
            ),
        }
    }

    /// See [`super::view::GraphLinksView::links_with_vectors`].
    pub fn links_with_vectors(
        &self,
        arena: &mut stumpalo::Arena,
        point_ids: &[PointOffsetType],
        level: usize,
        mut callback: impl FnMut(
            usize,
            &[u8],
            PackedLinksIterator<'_>,
            std::slice::ChunksExact<'_, u8>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let (base_vector_layout, link_vector_size, link_vector_alignment) = match self.compression {
            CompressionInfo::Compressed => unimplemented!(),
            CompressionInfo::CompressedWithVectors {
                base_vector_layout,
                link_vector_size,
                link_vector_alignment,
            } => (base_vector_layout, link_vector_size, link_vector_alignment),
        };

        let sorted_count = self.hnsw_m.level_m(level);
        let align = std::cmp::max(base_vector_layout.align(), link_vector_alignment as usize);
        self.read_neighbors(arena, point_ids, level, align, |position, start, data| {
            let (base_vector, links, link_vectors) = parse_links_with_vectors(
                data,
                start as usize,
                (level == 0).then_some(base_vector_layout),
                self.bits_per_unsorted,
                sorted_count,
                link_vector_size,
                link_vector_alignment,
            );
            callback(position, base_vector, links, link_vectors)
        })
    }

    fn read_neighbors(
        &self,
        arena: &stumpalo::Arena,
        point_ids: &[PointOffsetType],
        level: usize,
        align: usize,
        mut callback: impl FnMut(usize, u64, &[u8]) -> OperationResult<()>,
    ) -> OperationResult<()> {
        // Compute an offset index for each point.
        let offset_indices = if level == 0 {
            arena.alloc_slice_fill_iter(point_ids.iter().map(|&id| id as usize))
        } else {
            // Read the reindex
            let level_offset = self.level_offsets[level] as usize;
            let indices = arena.alloc_slice_fill_default(point_ids.len());
            let ranges = std::iter::zip(point_ids, &mut *indices)
                .map(|(&id, out)| (out, self.reindex_range(id)));
            self.file
                .read_batch(ranges, Random, |out, reindexed: &[PointOffsetType]| {
                    *out = level_offset + reindexed[0] as usize;
                    OperationResult::Ok(())
                })?;
            indices
        };

        let pairs =
            self.offsets
                .read_pairs_iter(&self.file, self.offsets_offset, offset_indices)?;
        pairs.process_results(|pairs| {
            let items = pairs.map(|(position, (start, end))| ReadBytesItem {
                user_data: (position, start),
                range: self.neighbors_offset + start..self.neighbors_offset + end,
                align,
            });
            for result in self.file.read_bytes_iter(items, Random)? {
                let ((position, start), data) = result?;
                callback(position, start, &data)?;
            }
            Ok(())
        })?
    }

    /// Byte range of the reindex table entry for the given point.
    fn reindex_range(&self, point_id: PointOffsetType) -> ReadRange {
        let offset =
            self.reindex_offset + u64::from(point_id) * size_of::<PointOffsetType>() as u64;
        ReadRange::one(offset)
    }
}
