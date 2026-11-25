//! Original dense vectors read by an [`F32VectorReader`].

use std::marker::PhantomData;

use common::types::PointOffsetType;
use quantization::DuplicateKey;

use super::F32VectorReader;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::F32Element;
use crate::vector_storage::turbo::shared::dequantize_for_requantization;
use crate::vector_storage::{DenseVectorStorageRead, TurboScoring, VectorStorageEnum};

/// Reads `storage` as f32, `None` if it has no dense vectors.
pub(super) fn f32_reader(storage: &VectorStorageEnum) -> Option<Box<dyn F32VectorReader + '_>> {
    let reader = match storage {
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
    Some(reader)
}

/// Original dense vectors, converted to f32.
struct DenseReader<'a, T, S>(&'a S, PhantomData<fn() -> T>);

/// The [`DenseReader`] of `storage`, of the element type it stores.
fn dense<'a, T, S>(storage: &'a S) -> Box<dyn F32VectorReader + 'a>
where
    T: F32Element,
    S: DenseVectorStorageRead<T> + Sync,
{
    Box::new(DenseReader(storage, PhantomData))
}

impl<T, S> F32VectorReader for DenseReader<'_, T, S>
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
        f: &mut dyn FnMut(usize, &[f32], DuplicateKey<'_>),
    ) -> OperationResult<()> {
        let mut buffer = Vec::new();
        self.0.for_each_in_dense_batch(ids, |i, vector| {
            f(i, T::as_f32(vector, &mut buffer), DuplicateKey::Values);
        })
    }
}

/// Vectors of a TurboQuant datatype storage, decoded but still rotated: the rotation is orthogonal
/// and the same for every vector, so the rotated vectors keep their geometry, and skipping the
/// inverse rotation saves its cost, in f64, per vector.
///
/// Decoded as for requantization, which only approximates the geometry their score sees: at 4 bits
/// it scales each vector by the RMS of its centroids rather than by its stored scale, and drops
/// the padding coordinate of an odd dimension. Sketching the scored vectors instead made no
/// measurable difference to recall. The duplicate key is the decoded values, so vectors whose
/// self-score is NaN only through overflow still count as copies.
struct TurboReader<'a, S>(&'a S);

/// The [`TurboReader`] of `storage`.
fn turbo<S: TurboScoring + Sync>(storage: &S) -> Box<dyn F32VectorReader + '_> {
    Box::new(TurboReader(storage))
}

impl<S: TurboScoring + Sync> F32VectorReader for TurboReader<'_, S> {
    fn dim(&self) -> usize {
        self.0.vector_dim()
    }

    fn count(&self) -> usize {
        self.0.total_vector_count()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], DuplicateKey<'_>),
    ) -> OperationResult<()> {
        let (quantizer, dim) = (self.0.quantizer(), self.0.vector_dim());
        self.0.for_each_in_dense_tq_batch(ids, |i, encoded| {
            let vector = dequantize_for_requantization(quantizer, dim, encoded, true);
            f(i, &vector, DuplicateKey::Values);
        })
    }
}
