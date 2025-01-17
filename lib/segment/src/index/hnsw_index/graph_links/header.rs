use common::bitpacking_ordered;
use zerocopy::little_endian::U64 as LittleU64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// File header for the plain format.
#[derive(FromBytes, Immutable, IntoBytes, KnownLayout)]
#[repr(C)]
pub(super) struct HeaderPlain {
    pub(super) point_count: u64,
    pub(super) levels_count: u64,
    pub(super) total_links_count: u64,
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
    pub(super) total_links_bytes: LittleU64,
    pub(super) offsets_parameters: bitpacking_ordered::Parameters,
    pub(super) m: LittleU64,
    pub(super) m0: LittleU64,
    pub(super) zero_padding: [u8; 5],
}

pub(super) const HEADER_VERSION_COMPRESSED: u64 = 0xFFFF_FFFF_FFFF_FF01;
