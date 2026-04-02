//! Zero-copy binary encoding and decoding for quantized vectors.
//!
//! Wire format (little-endian, no per-vector metadata beyond norms):
//!   [0..4)   norm: f32
//!   [4..8)   residual_norm: f32
//!   [8..8+idx_len)   packed centroid indices
//!   [8+idx_len..)    QJL sign bits
//!
//! The lengths `idx_len` and `qjl_len` are not stored — they are derived
//! from the [`TurboQuantizer`] that produced the data.

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::Quantized;

/// Fixed-size header (8 bytes): only per-vector scalars.
///
/// All fields are `[u8; 4]` so the struct has alignment 1 and can be
/// read from any byte offset without alignment issues.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Copy, Clone, Debug)]
#[repr(C)]
pub struct QuantizedHeader {
    residual_norm: [u8; 4],
}

impl QuantizedHeader {
    #[inline]
    pub fn residual_norm(&self) -> f32 {
        f32::from_le_bytes(self.residual_norm)
    }
}

/// Zero-copy view into an encoded quantized vector.
///
/// Borrows directly from the input byte slice — no allocation or copying
/// of the variable-length data.  The slice lengths are determined by the
/// [`TurboQuantizer`] that calls [`TurboQuantizer::decode`].
#[derive(Debug, Clone, Copy)]
pub struct QuantizedRef<'a> {
    header: &'a QuantizedHeader,
    pub packed_indices: &'a [u8],
    pub qjl_signs: &'a [u8],
}

impl<'a> QuantizedRef<'a> {
    /// Construct a `QuantizedRef` from its parts (called by `TurboQuantizer::decode`).
    pub(crate) fn from_parts(
        header: &'a QuantizedHeader,
        packed_indices: &'a [u8],
        qjl_signs: &'a [u8],
    ) -> Self {
        Self {
            header,
            packed_indices,
            qjl_signs,
        }
    }

    #[inline]
    pub fn residual_norm(&self) -> f32 {
        self.header.residual_norm()
    }
}

impl Quantized {
    /// Build the header for this quantized vector.
    fn header(&self) -> QuantizedHeader {
        QuantizedHeader {
            residual_norm: self.residual_norm.to_le_bytes(),
        }
    }

    /// Encode into a compact byte representation.
    ///
    /// The returned bytes can be decoded zero-copy via [`TurboQuantizer::decode`].
    pub fn encode(&self) -> Vec<u8> {
        let total = size_of::<QuantizedHeader>() + self.packed_indices.len() + self.qjl_signs.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(self.header().as_bytes());
        buf.extend_from_slice(&self.packed_indices);
        buf.extend_from_slice(&self.qjl_signs);
        buf
    }
}
