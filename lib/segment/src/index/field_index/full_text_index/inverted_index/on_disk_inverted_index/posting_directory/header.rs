use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub(super) const MAGIC: [u8; 8] = *b"QDRANTPD";
pub(super) const FORMAT_VERSION: u32 = 1;

/// Entries carry the size of each term's frequency data.
pub(super) const FLAG_TERM_FREQUENCIES: u32 = 1;

/// The fixed 64-byte header of a posting directory file, see the
/// [module docs](super).
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(super) struct Header {
    pub(super) magic: [u8; 8],
    pub(super) version: U32,
    pub(super) compression: U32,
    pub(super) count: U64,
    pub(super) body_len: U64,
    pub(super) data_start: U64,
    pub(super) data_end: U64,
    pub(super) entries_per_block: U32,
    pub(super) flags: U32,
    pub(super) frequencies_len: U64,
}

const _: () = assert!(size_of::<Header>() == 64);
