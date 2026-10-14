use common::types::PointOffsetType;

use crate::combine::Combine;

/// Element type of decoded vectors. Each has its own inner product, and later its own
/// accelerated kernels.
pub trait KernelElem: Copy + Default + Send + Sync + 'static + sealed::Sealed {
    /// Inner product of two decoded vectors, plain and unoptimized: the fallback, and the
    /// reference the accelerated kernels are tested against.
    fn dot(a: &[Self], b: &[Self]) -> f32;
}

impl KernelElem for f32 {
    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }
}

/// Exact in `i32`, converted to `f32` once, as the scalar quantization scorer does.
impl KernelElem for u8 {
    fn dot(a: &[u8], b: &[u8]) -> f32 {
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
    fn dot(a: &[Bits], b: &[Bits]) -> f32 {
        let differing: u32 = a.iter().zip(b).map(|(x, y)| (x.0 ^ y.0).count_ones()).sum();
        differing as f32
    }
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for f32 {}
    impl Sealed for u8 {}
    impl Sealed for super::Bits {}
}

/// How one storage's vectors become kernel operands.
///
/// The score of two stored points `a` and `b` must be
/// `combine().score(Elem::dot(decoded a, decoded b), aux a, aux b)`.
pub trait MatrixOperands: Sync {
    type Elem: KernelElem;
    type Error;

    /// Length of a decoded vector.
    fn depth(&self) -> usize;

    fn combine(&self) -> Combine;

    /// Decodes `ids` into `out`, `depth()` elements per id, and writes each vector's value for
    /// [`Combine`] to `aux`. Takes a batch, so storages can pipeline their reads.
    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [Self::Elem],
        aux: &mut [f32],
    ) -> Result<(), Self::Error>;
}
