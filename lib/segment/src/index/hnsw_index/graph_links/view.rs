use std::iter::{Copied, Zip};
use std::num::NonZero;

use common::bitpacking::packed_bits;
use common::bitpacking_links::{MIN_BITS_PER_VALUE, PackedLinksIterator, iterate_packed_links};
use common::bitpacking_ordered;
use common::types::PointOffsetType;
use integer_encoding::VarInt as _;
use itertools::{Either, Itertools as _};
use zerocopy::native_endian::U64 as NativeU64;
use zerocopy::{FromBytes, Immutable};

use super::GraphLinksFormat;
use super::header::{HEADER_VERSION_COMPRESSED, HeaderCompressed, HeaderPlain};
use crate::common::operation_error::{OperationError, OperationResult};
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
    /// - [`super::GraphLinksSerializer`] always writes `0` as the first element.
    /// - Additional element is added during deserialization.
    pub(super) level_offsets: Vec<u64>,
}

/// An iterator type returned by [`GraphLinksView::links`].
pub type LinksIterator<'a> = Either<Copied<std::slice::Iter<'a, u32>>, PackedLinksIterator<'a>>;

/// An iterator type returned by [`super::GraphLinks::links_with_vectors`].
/// Iterates over pairs of ([`PointOffsetType`], `&[u8]`). The second element is
/// quantized vector bytes.
pub type LinksWithVectorsIterator<'a> =
    Zip<PackedLinksIterator<'a>, std::slice::ChunksExact<'a, u8>>;

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
        offsets: bitpacking_ordered::Reader<'a>,
        hnsw_m: HnswM,
        bits_per_unsorted: u8,
    },
    CompressedWithVectors {
        /// Compressed links with vectors.
        ///
        /// Similar to [`CompressionInfo::Compressed`], but each `u32` value is
        /// accompanied by a fixed-size vector.
        ///
        /// ```text
        /// [N__VVVVVVVcccccccccc][N__VVVVVVVcccccccccc][N__VVVVVVVcccccccccc]
        /// [neighbors for node 0][neighbors for node 1][neighbors for node 2]...
        /// ```
        /// Where:
        /// 1. `N` is a varint-encoded length.
        ///    This value == number of links == number of vectors.
        /// 2. `_` is a padding to make vectors aligned.
        /// 3. `V` are encoded vectors, one per link (i.e. `Vec<Vec<u8>>`).
        /// 4. `c` are compressed links (i.e. a compressed form of `Vec<u32>`).
        neighbors: &'a [u8],
        offsets: bitpacking_ordered::Reader<'a>,
        hnsw_m: HnswM,
        bits_per_unsorted: u8,
        /// `NonZero` to avoid handling unlikely corner cases.
        vector_size_bytes: NonZero<usize>,
        vector_alignment: u8,
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
            HeaderPlain::ref_from_prefix(data).map_err(|_| error_unsufficent_size())?;
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
            HeaderCompressed::ref_from_prefix(data).map_err(|_| error_unsufficent_size())?;
        debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
        let (level_offsets, data) = read_level_offsets(
            data,
            header.levels_count.get(),
            header.offsets_parameters.length.get(),
        )?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count.get())?;
        let (neighbors, data) = get_slice::<u8>(data, header.total_neighbors_bytes.get())?;
        let (offsets, _bytes) = bitpacking_ordered::Reader::new(header.offsets_parameters, data)
            .map_err(|e| {
                OperationError::service_error(format!("Can't create decompressor: {e}"))
            })?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Compressed {
                neighbors,
                offsets,
                hnsw_m: HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                bits_per_unsorted: MIN_BITS_PER_VALUE.max(packed_bits(
                    u32::try_from(header.point_count.get().saturating_sub(1)).map_err(|_| {
                        OperationError::service_error("Too many points in GraphLinks file")
                    })?,
                )),
            },
            level_offsets,
        })
    }

    fn load_compressed_with_vectors(data: &[u8]) -> OperationResult<GraphLinksView<'_>> {
        let total_len = data.len();

        let (header, data) = HeaderCompressedWithVectors::ref_from_prefix(data)
            .map_err(|_| error_unsufficent_size())?;
        debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED_WITH_VECTORS);
        let vector_alignment = header.vector_alignment;
        if !vector_alignment.is_power_of_two() {
            return Err(OperationError::service_error(
                "In GraphLinks file, vector alignment should be a power of two",
            ));
        }

        let (level_offsets, data) = read_level_offsets(
            data,
            header.levels_count.get(),
            header.offsets_parameters.length.get(),
        )?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count.get())?;
        let (_, data) = get_slice::<u8>(data, {
            let pos = total_len - data.len();
            (pos.next_multiple_of(vector_alignment as usize) - pos) as u64
        })?;
        let (neighbors, data) = get_slice::<u8>(data, header.total_neighbors_bytes.get())?;
        let (offsets, _bytes) = bitpacking_ordered::Reader::new(header.offsets_parameters, data)
            .map_err(|e| {
                OperationError::service_error(format!("Can't create decompressor: {e}"))
            })?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::CompressedWithVectors {
                neighbors,
                offsets,
                hnsw_m: HnswM::new(header.m.get() as usize, header.m0.get() as usize),
                bits_per_unsorted: MIN_BITS_PER_VALUE.max(packed_bits(
                    u32::try_from(header.point_count.get().saturating_sub(1)).map_err(|_| {
                        OperationError::service_error("Too many points in GraphLinks file")
                    })?,
                )),
                vector_size_bytes: NonZero::try_from(header.vector_size_bytes.get() as usize)
                    .map_err(|_| {
                        OperationError::service_error("Zero vector size in GraphLinks file")
                    })?,
                vector_alignment,
            },
            level_offsets,
        })
    }

    pub(super) fn links(&self, point_id: PointOffsetType, level: usize) -> LinksIterator<'_> {
        let idx = if level == 0 {
            point_id as usize
        } else {
            self.level_offsets[level] as usize + self.reindex[point_id as usize] as usize
        };

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
                let neighbors_range =
                    offsets.get(idx).unwrap() as usize..offsets.get(idx + 1).unwrap() as usize;
                Either::Right(iterate_packed_links(
                    &neighbors[neighbors_range],
                    bits_per_unsorted,
                    hnsw_m.level_m(level),
                ))
            }
            CompressionInfo::CompressedWithVectors { .. } => {
                // Not intended to be used outside of tests.
                Either::Right(self.links_with_vectors(point_id, level).0)
            }
        }
    }

    /// Returns two iterators of the same length that could be combined into
    /// [`LinksWithVectorsIterator`].
    ///
    /// # Panics
    ///
    /// Panics when using a format that does not support vectors.
    pub(super) fn links_with_vectors(
        &self,
        point_id: PointOffsetType,
        level: usize,
    ) -> (PackedLinksIterator<'_>, std::slice::ChunksExact<'_, u8>) {
        let idx = if level == 0 {
            point_id as usize
        } else {
            self.level_offsets[level] as usize + self.reindex[point_id as usize] as usize
        };

        match self.compression {
            CompressionInfo::Uncompressed { .. } => unimplemented!(),
            CompressionInfo::Compressed { .. } => unimplemented!(),
            CompressionInfo::CompressedWithVectors {
                neighbors,
                ref offsets,
                ref hnsw_m,
                bits_per_unsorted,
                vector_size_bytes,
                vector_alignment,
            } => {
                let start = offsets.get(idx).unwrap() as usize;
                let end = offsets.get(idx + 1).unwrap() as usize;

                // 1. The varint-encoded length (`N` in the doc).
                let (vectors_count, vectors_count_size) =
                    u64::decode_var(&neighbors[start..end]).unwrap();

                // 2. Padding to align vectors (`_` in the doc).
                let vectors_start =
                    (start + vectors_count_size).next_multiple_of(vector_alignment as usize);

                // 3. Vectors (`V` in the doc).
                let vectors_bytes_len = vectors_count as usize * vector_size_bytes.get();
                let vectors = &neighbors[vectors_start..vectors_start + vectors_bytes_len];
                debug_assert!(vectors.as_ptr().addr() % vector_alignment as usize == 0);

                // 4. Compressed links (`c` in the doc).
                let links = iterate_packed_links(
                    &neighbors[vectors_start + vectors_bytes_len..end],
                    bits_per_unsorted,
                    hnsw_m.level_m(level),
                );

                (links, vectors.chunks_exact(vector_size_bytes.get()))
            }
        }
    }

    pub(super) fn point_level(&self, point_id: PointOffsetType) -> usize {
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
    result.push(total_offset_count.checked_sub(1).ok_or_else(|| {
        OperationError::service_error("Total offset count should be at least 1 in GraphLinks file")
    })?);
    Ok((result, bytes))
}

fn get_slice<T: FromBytes + Immutable>(data: &[u8], length: u64) -> OperationResult<(&[T], &[u8])> {
    <[T]>::ref_from_prefix_with_elems(data, length as usize).map_err(|_| error_unsufficent_size())
}

fn error_unsufficent_size() -> OperationError {
    OperationError::service_error("Unsufficent file size for GraphLinks file")
}
