use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub(super) const MAGIC: [u8; 8] = *b"QDRANTVC";
pub(super) const FORMAT_VERSION: u32 = 1;

/// The fixed 64-byte header of a compact vocabulary file, see the
/// [module docs](super).
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(super) struct Header {
    pub(super) magic: [u8; 8],
    pub(super) version: U32,
    pub(super) compression: U32,
    pub(super) count: U64,
    pub(super) body_len: U64,
    pub(super) keys_per_block: U32,
    pub(super) reserved: [u8; 28],
}

const _: () = assert!(size_of::<Header>() == 64);
