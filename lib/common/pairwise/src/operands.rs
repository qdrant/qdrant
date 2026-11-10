use common::types::PointOffsetType;

use crate::combine::{Aux, CombineDispatch};

/// Element type of decoded vectors. Each has its own inner product, and later its own
/// accelerated kernels.
pub trait KernelElem: Copy + Default + Send + Sync + 'static + sealed::Sealed {
    /// What the inner product needs besides the two vectors, the same for every pair: `()`
    /// for most types.
    type Param<'a>: Copy + Send + Sync;

    /// Inner product of two decoded vectors, plain and unoptimized: the fallback, and the
    /// reference the accelerated kernels are tested against.
    fn dot(a: &[Self], b: &[Self], param: Self::Param<'_>) -> f32;
}

/// Depth slice of every f32 kernel, in elements: where the chain of fused multiply-adds
/// restarts.
pub(crate) const F32_SLICE: usize = 256;

/// Fused multiply-adds in order, restarted from zero every [`F32_SLICE`] elements, each slice
/// added to the sum of the ones before: the sum every f32 kernel computes, so all of them give
/// bitwise the same scores. Slow on CPUs without FMA, which are the ones that use it.
///
/// The restarts make long sums more exact: summing 16-bit TurboQuant codes in one chain of
/// 1536 was 7x to 12x less exact than `score_internal`, restarted every 256, 2x to 3x.
impl KernelElem for f32 {
    type Param<'a> = ();

    fn dot(a: &[f32], b: &[f32], (): ()) -> f32 {
        let mut slices = a.chunks(F32_SLICE).zip(b.chunks(F32_SLICE)).map(|(a, b)| {
            a.iter()
                .zip(b)
                .fold(0.0, |sum, (x, y): (&f32, &f32)| x.mul_add(*y, sum))
        });
        // The first slice as it is, as the kernels store it
        let first = slices.next().unwrap_or(0.0);
        slices.fold(first, |sum, slice| sum + slice)
    }
}

/// Exact in `i32`, converted to `f32` once, as the scalar quantization scorer does.
///
/// Values must be at most 127, as scalar quantization codes are. Byte kernels multiply one
/// operand as a signed byte, and the sum stays below `i32::MAX` up to the maximum dimension
/// of 65,536 (65,536 · 127² ≈ 1.06e9).
impl KernelElem for u8 {
    type Param<'a> = ();

    fn dot(a: &[u8], b: &[u8], (): ()) -> f32 {
        let dot: i32 = a
            .iter()
            .zip(b)
            .map(|(&x, &y)| i32::from(x) * i32::from(y))
            .sum();
        dot as f32
    }
}

/// Eight packed bits of a binary quantized vector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct Bits(pub u8);

/// The number of differing bits.
impl KernelElem for Bits {
    type Param<'a> = ();

    fn dot(a: &[Bits], b: &[Bits], (): ()) -> f32 {
        let differing: u32 = a.iter().zip(b).map(|(x, y)| (x.0 ^ y.0).count_ones()).sum();
        differing as f32
    }
}

/// The kernels of each element type, behind the public traits.
pub(crate) mod sealed {
    use common::types::PointOffsetType;

    use super::{Bits, MatrixOperands};
    use crate::{Cols, TopK};

    pub trait Sealed: Sized {
        /// # Safety
        ///
        /// The CPU must support AVX2 and FMA.
        #[cfg(target_arch = "x86_64")]
        unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error>;

        /// The AVX2 kernel, unless the type has a wider one.
        ///
        /// # Safety
        ///
        /// The CPU must support AVX-512F, AVX2, and FMA.
        #[cfg(target_arch = "x86_64")]
        unsafe fn top_k_avx512<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // SAFETY: AVX2 and FMA, from the caller
            unsafe { Self::top_k_avx2(operands, rows, cols, out) }
        }

        /// # Safety
        ///
        /// The CPU must support NEON.
        #[cfg(target_arch = "aarch64")]
        unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error>;
    }

    impl Sealed for f32 {
        #[cfg(target_arch = "x86_64")]
        unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // SAFETY: forwarded from the caller
            unsafe { crate::x86_64::top_k_f32(operands, rows, cols, out) }
        }

        #[cfg(target_arch = "x86_64")]
        unsafe fn top_k_avx512<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // SAFETY: forwarded from the caller
            unsafe { crate::x86_64::top_k_f32_avx512(operands, rows, cols, out) }
        }

        #[cfg(target_arch = "aarch64")]
        unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // SAFETY: forwarded from the caller
            unsafe { crate::aarch64::top_k_f32(operands, rows, cols, out) }
        }
    }

    impl Sealed for u8 {
        #[cfg(target_arch = "x86_64")]
        unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // SAFETY: forwarded from the caller
            unsafe { crate::x86_64::top_k_u8(operands, rows, cols, out) }
        }

        #[cfg(target_arch = "aarch64")]
        unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // Optional before Armv8.4: older cores (Cortex-A72, Graviton 1) take the scalar kernel
            if !std::arch::is_aarch64_feature_detected!("dotprod") {
                return crate::fallback::top_k(operands, rows, cols, out);
            }
            // SAFETY: NEON from the caller, dot product instructions checked above
            unsafe { crate::aarch64::top_k_u8(operands, rows, cols, out) }
        }
    }

    impl Sealed for Bits {
        #[cfg(target_arch = "x86_64")]
        unsafe fn top_k_avx2<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // Counts are summed in u16
            if operands.depth() * 8 > usize::from(u16::MAX) {
                return crate::fallback::top_k(operands, rows, cols, out);
            }
            // SAFETY: forwarded from the caller
            unsafe { crate::x86_64::top_k_bits(operands, rows, cols, out) }
        }

        #[cfg(target_arch = "aarch64")]
        unsafe fn top_k_neon<F: MatrixOperands<Elem = Self>>(
            operands: &F,
            rows: &[PointOffsetType],
            cols: Cols<'_>,
            out: &mut TopK,
        ) -> Result<(), F::Error> {
            // Counts are summed in u16
            if operands.depth() * 8 > usize::from(u16::MAX) {
                return crate::fallback::top_k(operands, rows, cols, out);
            }
            // SAFETY: forwarded from the caller
            unsafe { crate::aarch64::top_k_bits(operands, rows, cols, out) }
        }
    }
}

/// How one storage's vectors become kernel operands.
///
/// The score of two stored points `a` and `b` must be
/// `score(Elem::dot(decoded a, decoded b, param()), &aux a, &aux b)` of the [`Combine`] that
/// `combine()` dispatches to.
///
/// [`Combine`]: crate::Combine
pub trait MatrixOperands: Sync {
    type Elem: KernelElem;
    type Combine: CombineDispatch;
    type Error;

    /// Length of a decoded vector.
    fn depth(&self) -> usize;

    fn combine(&self) -> Self::Combine;

    /// The parameter of [`KernelElem::dot`].
    fn param(&self) -> <Self::Elem as KernelElem>::Param<'_>;

    /// Decodes `ids` into `out`, `depth()` elements per id, and writes each vector's values for
    /// the combining step to `aux`. Takes a batch, so storages can pipeline their reads.
    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [Self::Elem],
        aux: &mut [Aux],
    ) -> Result<(), Self::Error>;
}
