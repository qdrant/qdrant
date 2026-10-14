use common::types::PointOffsetType;
use pairwise::{Combine, MatrixOperands};

use crate::common::operation_error::OperationError;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::Distance;
use crate::vector_storage::DenseVectorStorageRead;

/// A stored element of a dense vector, decoded to an f32 operand.
pub(super) trait MatrixElement: PrimitiveVectorElement {
    /// How Cosine scores vectors of this type.
    const COSINE: Combine;

    fn to_f32(self) -> f32;
}

/// Float vectors are stored normalized, so Cosine is their dot product.
impl MatrixElement for VectorElementType {
    const COSINE: Combine = Combine::Plain;

    fn to_f32(self) -> f32 {
        self
    }
}

impl MatrixElement for VectorElementTypeHalf {
    const COSINE: Combine = Combine::Plain;

    fn to_f32(self) -> f32 {
        VectorElementTypeHalf::to_f32(self)
    }
}

/// Byte vectors are stored as they are, so Cosine divides by both norms.
impl MatrixElement for VectorElementTypeByte {
    const COSINE: Combine = Combine::Cos;

    fn to_f32(self) -> f32 {
        f32::from(self)
    }
}

/// Original dense vectors as f32 operands.
pub(super) struct DenseOperands<'a, T, S> {
    storage: &'a S,
    combine: Combine,
    _element: std::marker::PhantomData<fn() -> T>,
}

impl<'a, T: MatrixElement, S> DenseOperands<'a, T, S> {
    /// `None` for Manhattan, which has no matrix operands.
    pub fn new(storage: &'a S, distance: Distance) -> Option<Self> {
        let combine = match distance {
            Distance::Dot => Combine::Plain,
            Distance::Cosine => T::COSINE,
            Distance::Euclid => Combine::Euclid,
            Distance::Manhattan => return None,
        };
        Some(Self {
            storage,
            combine,
            _element: std::marker::PhantomData,
        })
    }
}

impl<T, S> MatrixOperands for DenseOperands<'_, T, S>
where
    T: MatrixElement,
    S: DenseVectorStorageRead<T> + Sync,
{
    type Elem = f32;
    type Error = OperationError;

    fn depth(&self) -> usize {
        self.storage.vector_dim()
    }

    fn combine(&self) -> Combine {
        self.combine
    }

    fn decode_batch(
        &self,
        ids: &[PointOffsetType],
        out: &mut [f32],
        aux: &mut [f32],
    ) -> Result<(), OperationError> {
        let depth = self.depth();
        self.storage.for_each_in_dense_batch(ids, |i, vector| {
            let decoded = &mut out[i * depth..(i + 1) * depth];
            for (o, &x) in decoded.iter_mut().zip(vector) {
                *o = x.to_f32();
            }
            aux[i] = match self.combine {
                Combine::Euclid => squared_norm(decoded),
                Combine::Cos => match squared_norm(decoded) {
                    // A zero vector scores 0, as in `cosine_similarity_bytes`
                    0.0 => 0.0,
                    norm => 1.0 / norm.sqrt(),
                },
                Combine::Plain | Combine::Affine { .. } | Combine::Hamming { .. } => 0.0,
            };
        })
    }
}

/// Summed in f64, so it stays exact for byte vectors of any dimension.
fn squared_norm(vector: &[f32]) -> f32 {
    vector
        .iter()
        .map(|&x| f64::from(x) * f64::from(x))
        .sum::<f64>() as f32
}
