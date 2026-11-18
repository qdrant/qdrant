//! Original dense vectors as [`F32Rows`].

use std::marker::PhantomData;

use common::types::PointOffsetType;
use half::slice::HalfFloatSliceExt as _;

use super::{F32Rows, RowIdentity};
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::vector_storage::turbo::shared::dequantize_for_requantization;
use crate::vector_storage::{DenseVectorStorageRead, TurboScoring, VectorStorageEnum};

/// The rows of `storage`, `None` if it has no dense vectors.
pub(super) fn f32_rows(storage: &VectorStorageEnum) -> Option<Box<dyn F32Rows + '_>> {
    let rows = match storage {
        VectorStorageEnum::DenseVolatile(vs) => dense(vs),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(vs) => dense(vs),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(vs) => dense(vs),

        VectorStorageEnum::DenseMemmap(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseMemmapByte(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseMemmapHalf(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseGraphInline(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseGraphInlineByte(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseGraphInlineHalf(vs) => dense(vs.as_ref()),

        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(vs) => dense(vs.as_ref()),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringByte(vs) => dense(vs.as_ref()),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringHalf(vs) => dense(vs.as_ref()),

        VectorStorageEnum::DenseAppendableMemmap(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseAppendableMemmapByte(vs) => dense(vs.as_ref()),
        VectorStorageEnum::DenseAppendableMemmapHalf(vs) => dense(vs.as_ref()),

        VectorStorageEnum::DenseTurboMemmap(vs) => turbo(vs.as_ref()),
        VectorStorageEnum::DenseTurboGraphInline(vs) => turbo(vs.as_ref()),
        VectorStorageEnum::DenseTurboAppendableMemmap(vs) => turbo(vs.as_ref()),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseTurboUring(vs) => turbo(vs.as_ref()),

        VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::EmptySparse(_) => return None,
        VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::MultiDenseTurbo(_) => return None,
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => return None,
        // No vectors to read
        VectorStorageEnum::EmptyDense(_) => return None,
    };
    Some(rows)
}

/// A stored element of a dense vector, decoded to f32.
pub(crate) trait F32Element: PrimitiveVectorElement {
    /// `src` into `out`, of the same length.
    fn decode(src: &[Self], out: &mut [f32]);

    /// `vector` as f32, decoded into `buffer` unless it is f32 already.
    fn as_f32<'a>(vector: &'a [Self], buffer: &'a mut Vec<f32>) -> &'a [f32] {
        buffer.resize(vector.len(), 0.0);
        Self::decode(vector, buffer);
        buffer
    }
}

impl F32Element for VectorElementType {
    fn decode(src: &[Self], out: &mut [f32]) {
        out.copy_from_slice(src);
    }

    fn as_f32<'a>(vector: &'a [Self], _: &'a mut Vec<f32>) -> &'a [f32] {
        vector
    }
}

impl F32Element for VectorElementTypeHalf {
    /// The slice conversion uses F16C several values at a time; `f16::to_f32` checks for it
    /// per value unless the build enables it, which cost a PiPNN leaf about 20%.
    fn decode(src: &[Self], out: &mut [f32]) {
        src.convert_to_f32_slice(out);
    }
}

impl F32Element for VectorElementTypeByte {
    fn decode(src: &[Self], out: &mut [f32]) {
        for (o, &x) in out.iter_mut().zip(src) {
            *o = f32::from(x);
        }
    }
}

/// Original dense vectors, converted to f32.
struct DenseRows<'a, T, S>(&'a S, PhantomData<fn() -> T>);

/// The [`DenseRows`] of `storage`, of the element type it stores.
fn dense<'a, T, S>(storage: &'a S) -> Box<dyn F32Rows + 'a>
where
    T: F32Element,
    S: DenseVectorStorageRead<T> + Sync,
{
    Box::new(DenseRows(storage, PhantomData))
}

impl<T, S> F32Rows for DenseRows<'_, T, S>
where
    T: F32Element,
    S: DenseVectorStorageRead<T> + Sync,
{
    fn dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn count(&self) -> usize {
        self.0.total_vector_count()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], RowIdentity<'_>),
    ) -> OperationResult<()> {
        let mut buffer = Vec::new();
        self.0.for_each_in_dense_batch(ids, |i, vector| {
            f(i, T::as_f32(vector, &mut buffer), RowIdentity::Values);
        })
    }
}

/// Vectors of a TurboQuant datatype storage, decoded but still rotated: the rotation is orthogonal
/// and the same for every vector, so the rotated vectors keep their geometry, and skipping the
/// inverse rotation saves its cost, in f64, per vector.
struct TurboRows<'a, S>(&'a S);

/// The [`TurboRows`] of `storage`.
fn turbo<S: TurboScoring + Sync>(storage: &S) -> Box<dyn F32Rows + '_> {
    Box::new(TurboRows(storage))
}

impl<S: TurboScoring + Sync> F32Rows for TurboRows<'_, S> {
    fn dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn count(&self) -> usize {
        self.0.total_vector_count()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], RowIdentity<'_>),
    ) -> OperationResult<()> {
        let (quantizer, dim) = (self.0.quantizer(), self.0.vector_dim());
        self.0.for_each_in_dense_tq_batch(ids, |i, encoded| {
            let vector = dequantize_for_requantization(quantizer, dim, encoded, true);
            f(i, &vector, RowIdentity::Values);
        })
    }
}
