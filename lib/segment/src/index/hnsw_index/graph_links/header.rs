use std::alloc::Layout;
use std::ops::Range;

use common::bitpacking_ordered;
use common::types::PointOffsetType;
use zerocopy::little_endian::U64 as LittleU64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::GraphLinksFormat;
use super::view_utils::error_size;
use crate::common::operation_error::{OperationError, OperationResult};

/// File header for the plain format.
#[derive(FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub(super) struct HeaderPlain {
    pub(super) point_count: u64,
    pub(super) levels_count: u64,
    pub(super) total_neighbors_count: u64,
    pub(super) total_offset_count: u64,
    /// Either 0 or 4.
    pub(super) offsets_padding_bytes: u64,
    pub(super) zero_padding: [u8; 24],
}

/// File header for the compressed format.
#[derive(FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C, align(8))]
pub(super) struct HeaderCompressed {
    pub(super) point_count: LittleU64,
    /// Should be [`HEADER_VERSION_COMPRESSED`].
    pub(super) version: LittleU64,
    pub(super) levels_count: LittleU64,
    pub(super) total_neighbors_bytes: LittleU64,
    pub(super) offsets_parameters: bitpacking_ordered::Parameters,
    pub(super) m: LittleU64,
    pub(super) m0: LittleU64,
    pub(super) zero_padding: [u8; 5], // for 8-byte alignment
}

/// File header for the compressed format with embedded vectors.
#[derive(FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C, align(8))]
pub(super) struct HeaderCompressedWithVectors {
    pub(super) point_count: LittleU64,
    /// Should be [`HEADER_VERSION_COMPRESSED_WITH_VECTORS`].
    pub(super) version: LittleU64,
    pub(super) levels_count: LittleU64,
    pub(super) total_neighbors_bytes: LittleU64,
    pub(super) offsets_parameters: bitpacking_ordered::Parameters,
    pub(super) m: LittleU64,
    pub(super) m0: LittleU64,
    pub(super) base_vector_layout: PackedVectorLayout,
    pub(super) link_vector_layout: PackedVectorLayout,
    pub(super) zero_padding: [u8; 3], // for 8-byte alignment
}

pub(super) const HEADER_VERSION_COMPRESSED: u64 = 0xFFFF_FFFF_FFFF_FF01;
pub(super) const HEADER_VERSION_COMPRESSED_WITH_VECTORS: u64 = 0xFFFF_FFFF_FFFF_FF02;

pub(super) enum Header {
    Plain(HeaderPlain),
    Compressed(HeaderCompressed),
    CompressedWithVectors(HeaderCompressedWithVectors),
}

pub(super) const HEADER_MAX_SIZE: usize = const_max([
    size_of::<HeaderPlain>(),
    size_of::<HeaderCompressed>(),
    size_of::<HeaderCompressedWithVectors>(),
]);

impl Header {
    pub(super) fn parse(bytes: &[u8], format: GraphLinksFormat) -> OperationResult<Self> {
        fn read_prefix<T: FromBytes>(bytes: &[u8]) -> OperationResult<T> {
            match T::read_from_prefix(bytes) {
                Ok((header, _rest)) => Ok(header),
                Err(_) => Err(error_size()),
            }
        }

        Ok(match format {
            GraphLinksFormat::Plain => Self::Plain(read_prefix(bytes)?),
            GraphLinksFormat::Compressed => {
                let header: HeaderCompressed = read_prefix(bytes)?;
                debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
                Self::Compressed(header)
            }
            GraphLinksFormat::CompressedWithVectors => {
                let header: HeaderCompressedWithVectors = read_prefix(bytes)?;
                debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED_WITH_VECTORS);
                Self::CompressedWithVectors(header)
            }
        })
    }

    /// Which part of the file contains the offsets.
    /// Used for prefetching.
    pub(super) fn offsets_range(&self) -> OperationResult<Range<u64>> {
        let reindex_end = |size: usize, levels_count: u64, point_count: u64| {
            (size as u128)
                + u128::from(levels_count) * size_of::<u64>() as u128
                + u128::from(point_count) * size_of::<PointOffsetType>() as u128
        };

        let (start, len) = match self {
            Header::Plain(HeaderPlain {
                point_count,
                levels_count,
                total_neighbors_count,
                total_offset_count,
                offsets_padding_bytes,
                zero_padding: _,
            }) => {
                let reindex_end =
                    reindex_end(size_of::<HeaderPlain>(), *levels_count, *point_count);
                (
                    reindex_end
                        + u128::from(*total_neighbors_count) * size_of::<u32>() as u128
                        + u128::from(*offsets_padding_bytes),
                    u128::from(*total_offset_count) * size_of::<u64>() as u128,
                )
            }
            Header::Compressed(HeaderCompressed {
                point_count,
                version: _,
                levels_count,
                total_neighbors_bytes,
                offsets_parameters,
                m: _,
                m0: _,
                zero_padding: _,
            }) => {
                let reindex_end = reindex_end(
                    size_of::<HeaderCompressed>(),
                    levels_count.get(),
                    point_count.get(),
                );
                (
                    reindex_end + u128::from(total_neighbors_bytes.get()),
                    offsets_parameters.validate()?.compressed_size_bytes() as u128,
                )
            }
            Header::CompressedWithVectors(HeaderCompressedWithVectors {
                point_count,
                version: _,
                levels_count,
                total_neighbors_bytes,
                offsets_parameters,
                m: _,
                m0: _,
                base_vector_layout,
                link_vector_layout,
                zero_padding: _,
            }) => {
                let reindex_end = reindex_end(
                    size_of::<HeaderCompressedWithVectors>(),
                    levels_count.get(),
                    point_count.get(),
                );
                let alignment =
                    std::cmp::max(base_vector_layout.alignment, link_vector_layout.alignment);
                let neighbors_start = reindex_end
                    .checked_next_multiple_of(u128::from(alignment))
                    .ok_or_else(error_size)?;
                (
                    neighbors_start + u128::from(total_neighbors_bytes.get()),
                    offsets_parameters.validate()?.compressed_size_bytes() as u128,
                )
            }
        };
        let end = u64::try_from(start + len).map_err(|_| error_size())?;
        let start = end - len as u64;
        Ok(start..end)
    }
}

/// Packed representation of [`Layout`].
#[derive(Copy, Clone, FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub(super) struct PackedVectorLayout {
    pub(super) size: LittleU64,
    pub(super) alignment: u8,
}

impl PackedVectorLayout {
    pub(super) fn try_into_layout(self) -> OperationResult<Layout> {
        Layout::from_size_align(self.size.get() as usize, self.alignment as usize)
            .map_err(|_| OperationError::service_error("Invalid vector layout"))
    }
}

const fn const_max<const N: usize>(values: [usize; N]) -> usize {
    let mut max = 0;
    let mut i = 0;
    while i < N {
        if values[i] > max {
            max = values[i];
        }
        i += 1;
    }
    max
}
