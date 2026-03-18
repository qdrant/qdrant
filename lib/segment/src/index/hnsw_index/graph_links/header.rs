use std::alloc::Layout;

use common::bitpacking_ordered;
use zerocopy::little_endian::U64 as LittleU64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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
