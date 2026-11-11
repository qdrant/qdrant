//! Element types of codes mapped to small signed integers: TurboQuant's 2-bit and 4-bit
//! codebooks and its 8-bit grid. Each element is one stored byte; the kernels widen the codes
//! in registers.

use common::types::PointOffsetType;

use crate::operands::sealed::Sealed;
use crate::operands::{KernelElem, MatrixOperands};
use crate::{Cols, TopK};

/// Four 2-bit codes, the first in the lowest bits, valued by a [`Codebook`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct Codes2(pub u8);

/// Two 4-bit codes, the first in the low nibble, valued by a [`Codebook`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct Codes4(pub u8);

/// A value in `-128..=127`, stored as `value + 128` (wrapping), as TurboQuant's 8-bit grid
/// stores it. The default is the value 0, which pads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(transparent)]
pub struct Codes8(pub u8);

impl Default for Codes8 {
    fn default() -> Self {
        Codes8(0x80)
    }
}

impl Codes8 {
    #[inline]
    fn value(self) -> i64 {
        i64::from((self.0 ^ 0x80) as i8)
    }
}

/// The signed values of 2-bit or 4-bit codes.
///
/// Products of two values fit an `i16` (at most 128², for −128 · −128), which the kernels
/// rely on.
#[derive(Clone, Copy, Debug)]
pub struct Codebook {
    /// `values[q]` for code `q`
    pub(crate) values: [i8; 16],
    /// For a nibble `n` holding two 2-bit codes: the first's value, `values[n & 3]`
    #[cfg_attr(
        not(target_arch = "x86_64"),
        expect(dead_code, reason = "read only by the x86 code tile")
    )]
    pub(crate) first: [i8; 16],
    /// And the second's, `values[n >> 2]`
    #[cfg_attr(
        not(target_arch = "x86_64"),
        expect(dead_code, reason = "read only by the x86 code tile")
    )]
    pub(crate) second: [i8; 16],
}

impl Codebook {
    /// `values` of the codes: 4 for [`Codes2`], 16 for [`Codes4`].
    ///
    /// # Panics
    ///
    /// If there are more than 16 values.
    pub fn new(values: &[i8]) -> Self {
        assert!(values.len() <= 16, "at most 16 code values");
        let mut padded = [0; 16];
        padded[..values.len()].copy_from_slice(values);
        Self {
            values: padded,
            first: std::array::from_fn(|n| padded[n & 3]),
            second: std::array::from_fn(|n| padded[n >> 2]),
        }
    }

    /// `Σ values[a] · values[b]` over the codes of `a` and `b`, `CODES` per byte of `BITS`
    /// each, the first in the lowest bits. Exact.
    fn dot<const CODES: usize, const BITS: usize>(&self, a: &[u8], b: &[u8]) -> f32 {
        let mask = (1 << BITS) - 1;
        let value =
            |byte: u8, k: usize| i64::from(self.values[usize::from((byte >> (BITS * k)) & mask)]);
        let sum: i64 = a
            .iter()
            .zip(b)
            .map(|(&x, &y)| (0..CODES).map(|k| value(x, k) * value(y, k)).sum::<i64>())
            .sum();
        sum as f32
    }
}

/// The codes as their stored bytes.
fn bytes<E: Copy>(codes: &[E]) -> &[u8] {
    const { assert!(size_of::<E>() == 1) };
    // SAFETY: the code types are transparent bytes, checked above
    unsafe { std::slice::from_raw_parts(codes.as_ptr().cast(), codes.len()) }
}

/// Exact, converted to f32 once, as TurboQuant's `score_2bit_internal`.
impl KernelElem for Codes2 {
    type Param<'a> = Codebook;

    fn dot(a: &[Codes2], b: &[Codes2], codebook: Codebook) -> f32 {
        codebook.dot::<4, 2>(bytes(a), bytes(b))
    }
}

/// Exact, converted to f32 once, as TurboQuant's `score_4bit_internal`.
impl KernelElem for Codes4 {
    type Param<'a> = Codebook;

    fn dot(a: &[Codes4], b: &[Codes4], codebook: Codebook) -> f32 {
        codebook.dot::<2, 4>(bytes(a), bytes(b))
    }
}

/// Exact, converted to f32 once, as TurboQuant's `score_8bit_internal`.
impl KernelElem for Codes8 {
    type Param<'a> = ();

    fn dot(a: &[Codes8], b: &[Codes8], (): ()) -> f32 {
        let sum: i64 = a.iter().zip(b).map(|(x, y)| x.value() * y.value()).sum();
        sum as f32
    }
}

impl Sealed for Codes2 {
    #[cfg(target_arch = "x86_64")]
    unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        // SAFETY: AVX2 and FMA from the caller
        unsafe { x86_codes::<Self, F>(operands, rows, cols, out, 4, false) }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn top_k_avx512_vnni<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        // SAFETY: AVX-512F, AVX-512 VNNI, AVX2, and FMA from the caller
        unsafe { x86_codes::<Self, F>(operands, rows, cols, out, 4, true) }
    }

    /// No NEON kernel: TurboQuant decodes these codes to f32 on aarch64 instead
    #[cfg(target_arch = "aarch64")]
    unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        crate::fallback::top_k(operands, rows, cols, out)
    }
}

impl Sealed for Codes4 {
    #[cfg(target_arch = "x86_64")]
    unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        // SAFETY: AVX2 and FMA from the caller
        unsafe { x86_codes::<Self, F>(operands, rows, cols, out, 2, false) }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn top_k_avx512_vnni<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        // SAFETY: AVX-512F, AVX-512 VNNI, AVX2, and FMA from the caller
        unsafe { x86_codes::<Self, F>(operands, rows, cols, out, 2, true) }
    }

    /// No NEON kernel: TurboQuant decodes these codes to f32 on aarch64 instead
    #[cfg(target_arch = "aarch64")]
    unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        crate::fallback::top_k(operands, rows, cols, out)
    }
}

impl Sealed for Codes8 {
    #[cfg(target_arch = "x86_64")]
    unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        // SAFETY: AVX2 and FMA from the caller
        unsafe { x86_codes::<Self, F>(operands, rows, cols, out, 1, false) }
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn top_k_avx512_vnni<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        // SAFETY: AVX-512F, AVX-512 VNNI, AVX2, and FMA from the caller
        unsafe { x86_codes::<Self, F>(operands, rows, cols, out, 1, true) }
    }

    /// No NEON kernel: TurboQuant decodes these codes to f32 on aarch64 instead
    #[cfg(target_arch = "aarch64")]
    unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
        operands: &F,
        rows: &[PointOffsetType],
        cols: Cols<'_>,
        out: &mut TopK,
    ) -> Result<(), F::Error> {
        crate::fallback::top_k(operands, rows, cols, out)
    }
}

/// The x86 kernel of a code type with `codes` per element, VNNI or AVX2, or the scalar one past
/// the depth its i32 sums hold.
///
/// # Safety
///
/// The CPU must support AVX2 and FMA, and if `vnni`, AVX-512F and AVX-512 VNNI.
#[cfg(target_arch = "x86_64")]
unsafe fn x86_codes<E, F>(
    operands: &F,
    rows: &[PointOffsetType],
    cols: Cols<'_>,
    out: &mut TopK,
    codes: usize,
    vnni: bool,
) -> Result<(), F::Error>
where
    E: crate::x86_64::Unpack + crate::x86_64::Vnni,
    F: MatrixOperands<Elem = E>,
{
    if operands.depth() * codes > crate::x86_64::MAX_CODES {
        return crate::fallback::top_k(operands, rows, cols, out);
    }
    // SAFETY: the CPU from the caller, the depth checked above
    unsafe {
        if vnni {
            crate::x86_64::top_k_vnni(operands, rows, cols, out)
        } else {
            crate::x86_64::top_k_codes(operands, rows, cols, out)
        }
    }
}
