use std::alloc::Layout;
use std::iter::{Copied, Zip};
use std::num::NonZero;
use std::slice::ChunksExact;

use common::bitpacking_links::{PackedLinksIterator, iterate_packed_links};
use common::bitpacking_ordered;
use common::types::PointOffsetType;
use itertools::Either;
use zerocopy::native_endian::U64 as NativeU64;
use zerocopy::{FromBytes, Immutable};

use super::GraphLinksFormat;
use super::header::{HEADER_VERSION_COMPRESSED, HeaderCompressed, HeaderPlain};
use super::view_utils::{
    bits_per_unsorted, error_insufficient_size, find_level, last_offset_idx, link_vector_size,
    parse_links_with_vectors,
};
use crate::common::operation_error::OperationResult;
use crate::index::hnsw_index::HnswM;
use crate::index::hnsw_index::graph_links::header::{
    HEADER_VERSION_COMPRESSED_WITH_VECTORS, HeaderCompressedWithVectors,
};

/// An (almost) zero-copy, non-owning view into serialized graph links stored
/// as a `&[u8]` slice.
#[derive(Debug)]
pub(super) struct GraphLinksView<'a> {
    pub(super) reindex: &'a [PointOffsetType],
    pub(super) compression: CompressionInfo<'a>,
    /// Level offsets, copied into RAM for faster access.
    /// Has at least two elements:
    /// - [`super::serialize_graph_links`] always writes `0` as the first element.
    /// - Additional element is added during deserialization.
    pub(super) level_offsets: Vec<u64>,
}

/// An iterator type returned by [`GraphLinksView::links`].
pub type LinksIterator<'a> = Either<Copied<std::slice::Iter<'a, u32>>, PackedLinksIterator<'a>>;

/// An iterator type returned by [`super::GraphLinks::links_with_vectors`].
/// Iterates over pairs of ([`PointOffsetType`], `&[u8]`). The second element is
/// quantized vector bytes.
pub type LinksWithVectorsIterator<'a> = Zip<PackedLinksIterator<'a>, ChunksExact<'a, u8>>;

#[derive(Debug)]
pub(super) enum CompressionInfo<'a> {
    Uncompressed {
        /// Uncompressed links.
        ///
        /// A flat array of `u32` values (neighbor ids).
        /// ```text
        /// [uuuuuuuuuuuuuuuuuuuu][uuuuuuuuuuuuuuuuuuuu][uuuuuuuuuuuuuuuuuuuu]...
        /// [neighbors for node 0][neighbors for node 1][neighbors for node 2]...
        /// ```
        /// Where:
        /// 1. `u` are uncompressed links (i.e. it represents `Vec<u32>`).
        neighbors: &'a [u32],
        offsets: &'a [NativeU64],
    },
    Compressed {
        /// Compressed links.
        ///
        /// Similar to [`CompressionInfo::Uncompressed`], but compressed.
        ///
        /// ```text
        /// [cccccccccccccccccccc][cccccccccccccccccccc][cccccccccccccccccccc]...
        /// [neighbors for node 0][neighbors for node 1][neighbors for node 2]...
        /// ```
        /// Where
        /// 1. `c` are compressed links (i.e. a compressed form of `Vec<u32>`).
        neighbors: &'a [u8],
        offsets: bitpacking_ordered::SliceReader<'a>,
        hnsw_m: HnswM,
        bits_per_unsorted: u8,
    },
    CompressedWithVectors {
        /// Compressed links with vectors.
        ///
        /// Similar to [`CompressionInfo::Compressed`], but includes vectors.
        /// - Each node on level 0 has a fixed-size "base" vector.
        /// - Each link is accompanied by a fixed-size "link" vector.
        ///
        /// ```text
        /// [BBBB#ccccccc_LLLLLL_][BBBB#ccccccc_LLLLLL_][BBBB#ccccccc_LLLLLL_]
        /// [neighbors for node 0][neighbors for node 1][neighbors for node 2]...
        /// ```
        /// Where:
        /// 1. `B` is a base vector (i.e. `Vec<u8>` of fixed size).
        ///    Only present on level 0, omitted on higher levels.
        /// 2. `#` is a varint-encoded length.
        ///    This value == number of links == number of link vectors.
        /// 3. `c` are compressed links (i.e. a compressed form of `Vec<u32>`).
        /// 4. `_` is a padding to make link vectors aligned.
        /// 5. `L` are encoded link vectors, one per link (i.e. `Vec<Vec<u8>>`).
        /// 6. `_` is a padding to make the next base vector aligned.
        ///    Only present on level 0, omitted on higher levels.
        neighbors: &'a [u8],
        offsets: bitpacking_ordered::SliceReader<'a>,
        hnsw_m: HnswM,
        bits_per_unsorted: u8,
        base_vector_layout: Layout,
        /// `NonZero` to avoid handling unlikely corner cases.
        link_vector_size: NonZero<usize>,
        link_vector_alignment: u8,
    },
}

impl GraphLinksView<'_> {
    pub(super) fn load(
        data: &[u8],
        format: GraphLinksFormat,
    ) -> OperationResult<GraphLinksView<'_>> {
        match format {
            GraphLinksFormat::Compressed => Self::load_compressed(data),
            GraphLinksFormat::Plain => Self::load_plain(data),
            GraphLinksFormat::CompressedWithVectors => Self::load_compressed_with_vectors(data),
        }
    }

    fn load_plain(data: &[u8]) -> OperationResult<GraphLinksView<'_>> {
        let (header, data) =
            HeaderPlain::ref_from_prefix(data).map_err(|_| error_insufficient_size())?;
        let (level_offsets, data) =
            read_level_offsets(data, header.levels_count, header.total_offset_count)?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count)?;
        let (neighbors, data) = get_slice::<u32>(data, header.total_neighbors_count)?;
        let (_, data) = get_slice::<u8>(data, header.offsets_padding_bytes)?;
        let (offsets, _bytes) = get_slice::<NativeU64>(data, header.total_offset_count)?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Uncompressed { neighbors, offsets },
            level_offsets,
        })
    }

    fn load_compressed(data: &[u8]) -> OperationResult<GraphLinksView<'_>> {
        let (header, data) =
            HeaderCompressed::ref_from_prefix(data).map_err(|_| error_insufficient_size())?;
        debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
        let (level_offsets, data) = read_level_offsets(
            data,
            header.levels_count.get(),
            header.offsets_parameters.length.get(),
        )?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count.get())?;
        let (neighbors, data) = get_slice::<u8>(data, header.total_neighbors_bytes.get())?;
        let offsets = header.offsets_parameters.validate()?.slice_reader(data)?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Compressed {
                neighbors,
                offsets,
                hnsw_m: HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                bits_per_unsorted: bits_per_unsorted(header.point_count.get())?,
            },
            level_offsets,
        })
    }

    fn load_compressed_with_vectors(data: &[u8]) -> OperationResult<GraphLinksView<'_>> {
        let total_len = data.len();

        let (header, data) = HeaderCompressedWithVectors::ref_from_prefix(data)
            .map_err(|_| error_insufficient_size())?;
        debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED_WITH_VECTORS);

        let base_vector_layout = header.base_vector_layout.try_into_layout()?;
        let link_vector_layout = header.link_vector_layout.try_into_layout()?;

        let (level_offsets, data) = read_level_offsets(
            data,
            header.levels_count.get(),
            header.offsets_parameters.length.get(),
        )?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count.get())?;
        let (_, data) = get_slice::<u8>(data, {
            let pos = total_len - data.len();
            let alignment = std::cmp::max(link_vector_layout.align(), base_vector_layout.align());
            (pos.next_multiple_of(alignment) - pos) as u64
        })?;
        let (neighbors, data) = get_slice::<u8>(data, header.total_neighbors_bytes.get())?;
        let offsets = header.offsets_parameters.validate()?.slice_reader(data)?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::CompressedWithVectors {
                neighbors,
                offsets,
                hnsw_m: HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                bits_per_unsorted: bits_per_unsorted(header.point_count.get())?,
                base_vector_layout,
                link_vector_size: link_vector_size(link_vector_layout)?,
                link_vector_alignment: link_vector_layout.align() as u8,
            },
            level_offsets,
        })
    }

    /// Note: it is safe to use `idx + 1` on the result of this function,
    /// because `level_offsets` always contains an additional element.
    #[inline]
    fn offset_idx(&self, point_id: PointOffsetType, level: usize) -> usize {
        if level == 0 {
            point_id as usize
        } else {
            self.level_offsets[level] as usize + self.reindex[point_id as usize] as usize
        }
    }

    /// Returns `true` if [`Self::links`] would return an empty iterator.
    pub(super) fn links_empty(&self, point_id: PointOffsetType, level: usize) -> bool {
        let idx = self.offset_idx(point_id, level);
        match self.compression {
            CompressionInfo::Uncompressed { offsets, .. } => {
                offsets[idx].get() == offsets[idx + 1].get()
            }
            CompressionInfo::Compressed { ref offsets, .. } => {
                let (start, end) = offsets.read_pair(idx).unwrap();
                start == end
            }
            CompressionInfo::CompressedWithVectors { .. } => {
                // Not intended to be used outside of tests.
                self.links(point_id, level).next().is_none()
            }
        }
    }

    pub(super) fn links(&self, point_id: PointOffsetType, level: usize) -> LinksIterator<'_> {
        let idx = self.offset_idx(point_id, level);
        match self.compression {
            CompressionInfo::Uncompressed { neighbors, offsets } => {
                let neighbors_range = offsets[idx].get() as usize..offsets[idx + 1].get() as usize;
                Either::Left(neighbors[neighbors_range].iter().copied())
            }
            CompressionInfo::Compressed {
                neighbors,
                ref offsets,
                ref hnsw_m,
                bits_per_unsorted,
            } => {
                let (start, end) = offsets.read_pair(idx).unwrap();
                Either::Right(iterate_packed_links(
                    &neighbors[start as usize..end as usize],
                    bits_per_unsorted,
                    hnsw_m.level_m(level),
                ))
            }
            CompressionInfo::CompressedWithVectors { .. } => {
                // Not intended to be used outside of tests.
                Either::Right(self.links_with_vectors(point_id, level).1)
            }
        }
    }

    /// Returns a tuple of three elements:
    /// - Base vector (only on level 0, empty slice on higher levels).
    /// - Links iterator.
    /// - Link vectors iterator.
    ///
    /// Both iterators have same length and can be combined into
    /// [`LinksWithVectorsIterator`].
    ///
    /// # Panics
    ///
    /// Panics when using a format that does not support vectors.
    pub(super) fn links_with_vectors(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> (&[u8], PackedLinksIterator<'_>, ChunksExact<'_, u8>) {
        let idx = self.offset_idx(point_id, level);
        match self.compression {
            CompressionInfo::Uncompressed { .. } => unimplemented!(),
            CompressionInfo::Compressed { .. } => unimplemented!(),
            CompressionInfo::CompressedWithVectors {
                neighbors,
                ref offsets,
                ref hnsw_m,
                bits_per_unsorted,
                base_vector_layout,
                link_vector_size,
                link_vector_alignment,
            } => {
                let (start, end) = offsets.read_pair(idx).unwrap();
                let (start, end) = (start as usize, end as usize);
                common::mmap::advice::will_need_multiple_pages(&neighbors[start..end]);
                parse_links_with_vectors(
                    &neighbors[start..end],
                    start,
                    (level == 0).then_some(base_vector_layout),
                    bits_per_unsorted,
                    hnsw_m.level_m(level),
                    link_vector_size,
                    link_vector_alignment,
                )
            }
        }
    }

    pub(super) fn point_level(&self, point_id: PointOffsetType) -> usize {
        find_level(
            u64::from(self.reindex[point_id as usize]),
            &self.level_offsets,
        )
    }

    #[cfg(test)]
    pub(super) fn sorted_count(&self, level: usize) -> usize {
        match self.compression {
            CompressionInfo::Uncompressed { .. } => 0,
            CompressionInfo::Compressed { hnsw_m, .. } => hnsw_m.level_m(level),
            CompressionInfo::CompressedWithVectors { hnsw_m, .. } => hnsw_m.level_m(level),
        }
    }
}

fn read_level_offsets(
    bytes: &[u8],
    levels_count: u64,
    total_offset_count: u64,
) -> OperationResult<(Vec<u64>, &[u8])> {
    let (level_offsets, bytes) = get_slice::<u64>(bytes, levels_count)?;
    let mut result = Vec::with_capacity(level_offsets.len() + 1);
    result.extend_from_slice(level_offsets);
    result.push(last_offset_idx(total_offset_count)?);
    Ok((result, bytes))
}

fn get_slice<T: FromBytes + Immutable>(data: &[u8], length: u64) -> OperationResult<(&[T], &[u8])> {
    <[T]>::ref_from_prefix_with_elems(data, length as usize).map_err(|_| error_insufficient_size())
}
