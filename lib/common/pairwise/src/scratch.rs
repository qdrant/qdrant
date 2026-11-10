use crate::{Aux, Bits, Codes2, Codes4, Codes8};

/// Buffers a kernel reuses across calls, owned by the [`TopK`](crate::TopK) it writes to.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scratch {
    /// Packed or decoded rows.
    pub rows: AlignedBuf,
    /// Packed or nibble-sliced columns.
    pub cols: AlignedBuf,
    /// Inner products of a block.
    pub out: AlignedBuf,
    /// A batch of decoded vectors before packing.
    pub decoded: AlignedBuf,
    /// A row block's panels and a column panel, unpacked for tiles that do that.
    pub unpacked_rows: AlignedBuf,
    pub unpacked_cols: AlignedBuf,
    /// Lookup tables of the x86 bits kernel.
    #[cfg(target_arch = "x86_64")]
    pub tables: AlignedBuf,
    /// Partial counts of the x86 bits kernel.
    #[cfg(target_arch = "x86_64")]
    pub partial: AlignedBuf,
    pub row_aux: Vec<Aux>,
    pub col_aux: Vec<Aux>,
    /// Scores of one row of a block, and of its transposed pairs.
    pub scores: Vec<f32>,
    pub scores_t: Vec<f32>,
}

/// Values of which every bit pattern is valid, with an alignment of at most 8.
///
/// # Safety
///
/// Implement only for such types.
pub(crate) unsafe trait Plain: Copy {}

// SAFETY: plain numbers and transparent bytes, alignments of 1 to 8.
unsafe impl Plain for f32 {}
unsafe impl Plain for i32 {}
unsafe impl Plain for u16 {}
unsafe impl Plain for i16 {}
unsafe impl Plain for u8 {}
unsafe impl Plain for Bits {}
unsafe impl Plain for Codes2 {}
unsafe impl Plain for Codes4 {}
unsafe impl Plain for Codes8 {}

/// Storage for [`Plain`] values, 64-byte aligned for vector loads.
#[derive(Clone, Default)]
pub(crate) struct AlignedBuf {
    words: Vec<u64>,
}

impl AlignedBuf {
    /// `len` values, holding whatever an earlier use left: callers write before they read.
    pub fn get<E: Plain>(&mut self, len: usize) -> &mut [E] {
        // Up to 7 words to reach a 64-byte boundary
        let words = (len * size_of::<E>()).div_ceil(8) + 7;
        if self.words.len() < words {
            self.words.resize(words, 0);
        }
        let offset = self.words.as_ptr().align_offset(64);
        assert!(offset <= 7);
        // SAFETY: the words from `offset` hold `len` values of `E`, initialized as integers,
        // which is a valid `E` for a `Plain` type. A word's alignment is a multiple of `E`'s.
        unsafe {
            std::slice::from_raw_parts_mut(self.words.as_mut_ptr().add(offset).cast::<E>(), len)
        }
    }
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("bytes", &(self.words.len() * 8))
            .finish()
    }
}

/// Binary codes as bytes.
#[cfg(target_arch = "x86_64")]
pub(crate) fn bits_as_bytes(bits: &[Bits]) -> &[u8] {
    // SAFETY: `Bits` is a transparent `u8`.
    unsafe { std::slice::from_raw_parts(bits.as_ptr().cast(), bits.len()) }
}
