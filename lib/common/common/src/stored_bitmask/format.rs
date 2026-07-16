//! On-disk layout: file header, payload encodings, and the decoded contents.

use std::borrow::Cow;

use roaring::RoaringBitmap;

use crate::bitvec::BitSlice;

pub(super) const MAGIC: [u8; 4] = *b"QBMK";
pub(super) const VERSION: u32 = 1;
pub(super) const HEADER_SIZE: usize = size_of::<BitmaskHeader>();

/// How the payload following the header encodes the mask.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum Encoding {
    /// Raw bits: `u64` words in `Lsb0` order, native (little-endian) byte
    /// order, matching the layout of [`crate::bitvec::BitVec`].
    Dense = 0,
    /// Roaring bitmap of the set positions; everything else in
    /// `0..logical_len` is unset.
    RoaringOnes = 1,
    /// Roaring bitmap of the unset positions; everything else in
    /// `0..logical_len` is set.
    RoaringZeros = 2,
}

impl Encoding {
    pub(super) fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::Dense),
            1 => Some(Self::RoaringOnes),
            2 => Some(Self::RoaringZeros),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub(super) struct BitmaskHeader {
    pub(super) magic: [u8; 4],
    pub(super) version: u32,
    /// Number of logical flags (bits) in the mask.
    pub(super) logical_len: u64,
    /// One of the [`Encoding`] discriminants.
    pub(super) encoding: u32,
    pub(super) _reserved: u32,
    /// Exact byte length of the payload following the header.
    pub(super) payload_len: u64,
}

/// Decoded mask contents, in the stored polarity: the bitmap always holds the
/// minority positions, so iterating it is never worse than the dense scan.
pub enum BitmaskContent<'a> {
    /// Raw bits, exactly `logical_len` of them. Borrowed (zero-copy) on
    /// backends that expose their bytes directly, e.g. mmap.
    Dense(Cow<'a, BitSlice>),
    /// Positions of set bits; everything else in `0..logical_len` is unset.
    Ones(RoaringBitmap),
    /// Positions of unset bits; everything else in `0..logical_len` is set.
    Zeros(RoaringBitmap),
}
