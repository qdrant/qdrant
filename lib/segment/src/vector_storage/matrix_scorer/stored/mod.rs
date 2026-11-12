//! The vectors of a matrix scorer as they are stored: the layout of one record as data
//! ([`StoredForm`]) and the raw records themselves ([`StoredRecords`]), so they can be scored
//! elsewhere without decoding them first, by code that reads the layout and the combine values
//! the CPU kernels score with.

mod combine;
mod form;
mod records;
#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

use pairwise::MatrixOperands as _;
use quantization::encoded_storage::EncodedStorage;
use quantization::encoded_vectors_u8::ScalarOperands;

pub use self::combine::StoredCombine;
pub use self::form::{StoredForm, StoredFormat, StoredValue};
use self::records::DenseRecords;
pub use self::records::StoredRecords;
use super::dense_operands::{DenseCombine, DenseOperands};
use crate::data_types::vectors::VectorElementType;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::{DenseVectorStorageRead, VectorStorageEnum};

/// The stored records of the vectors [`super::new_matrix_scorer`] scores: the quantized ones if
/// configured, otherwise the original ones.
///
/// `None` for vectors without a stored form yet, among them all the matrix scorer has no
/// operands for. So far, only original f32 vectors (Dot, Cosine, Euclid) and scalar quantization
/// (Dot, Cosine, L2) have one. Quantized vectors without one return `None` too, never the
/// originals.
pub fn stored_records<'a>(
    vector_storage: &'a VectorStorageEnum,
    quantized_vectors: Option<&'a QuantizedVectors>,
) -> Option<StoredRecords<'a>> {
    match quantized_vectors {
        Some(quantized_vectors) => quantized_records(quantized_vectors.get_storage()),
        None => original_records(vector_storage),
    }
}

fn quantized_records(storage: &QuantizedVectorStorage) -> Option<StoredRecords<'_>> {
    match storage {
        QuantizedVectorStorage::ScalarRam(q) => ScalarOperands::new(q).map(scalar_records),
        QuantizedVectorStorage::ScalarMmap(q) => ScalarOperands::new(q).map(scalar_records),
        QuantizedVectorStorage::ScalarChunkedMmap(q) => ScalarOperands::new(q).map(scalar_records),
        QuantizedVectorStorage::BinaryRam(_)
        | QuantizedVectorStorage::BinaryMmap(_)
        | QuantizedVectorStorage::BinaryChunkedMmap(_)
        | QuantizedVectorStorage::TQRam(_)
        | QuantizedVectorStorage::TQMmap(_)
        | QuantizedVectorStorage::TQChunkedMmap(_)
        | QuantizedVectorStorage::PQRam(_)
        | QuantizedVectorStorage::PQMmap(_)
        | QuantizedVectorStorage::PQChunkedMmap(_) => None,
        QuantizedVectorStorage::ScalarRamMulti(_)
        | QuantizedVectorStorage::ScalarMmapMulti(_)
        | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
        | QuantizedVectorStorage::PQRamMulti(_)
        | QuantizedVectorStorage::PQMmapMulti(_)
        | QuantizedVectorStorage::PQChunkedMmapMulti(_)
        | QuantizedVectorStorage::BinaryRamMulti(_)
        | QuantizedVectorStorage::BinaryMmapMulti(_)
        | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
        | QuantizedVectorStorage::TQRamMulti(_)
        | QuantizedVectorStorage::TQMmapMulti(_)
        | QuantizedVectorStorage::TQChunkedMmapMulti(_) => None,
    }
}

fn original_records(storage: &VectorStorageEnum) -> Option<StoredRecords<'_>> {
    match storage {
        VectorStorageEnum::DenseVolatile(vs) => f32_records(vs),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(_) => None,
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(_) => None,

        VectorStorageEnum::DenseMemmap(vs) => f32_records(vs.as_ref()),
        VectorStorageEnum::DenseMemmapByte(_) | VectorStorageEnum::DenseMemmapHalf(_) => None,
        VectorStorageEnum::DenseGraphInline(vs) => f32_records(vs.as_ref()),
        VectorStorageEnum::DenseGraphInlineByte(_) | VectorStorageEnum::DenseGraphInlineHalf(_) => {
            None
        }

        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUring(vs) => f32_records(vs.as_ref()),
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseUringByte(_) | VectorStorageEnum::DenseUringHalf(_) => None,

        VectorStorageEnum::DenseAppendableMemmap(vs) => f32_records(vs.as_ref()),
        VectorStorageEnum::DenseAppendableMemmapByte(_)
        | VectorStorageEnum::DenseAppendableMemmapHalf(_) => None,

        VectorStorageEnum::DenseTurboMemmap(_)
        | VectorStorageEnum::DenseTurboGraphInline(_)
        | VectorStorageEnum::DenseTurboAppendableMemmap(_) => None,
        #[cfg(target_os = "linux")]
        VectorStorageEnum::DenseTurboUring(_) => None,

        VectorStorageEnum::SparseVolatile(_)
        | VectorStorageEnum::SparseMmap(_)
        | VectorStorageEnum::EmptySparse(_) => None,
        VectorStorageEnum::MultiDenseVolatile(_)
        | VectorStorageEnum::MultiDenseAppendableMemmap(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapByte(_)
        | VectorStorageEnum::MultiDenseAppendableMemmapHalf(_)
        | VectorStorageEnum::MultiDenseTurbo(_) => None,
        #[cfg(test)]
        VectorStorageEnum::MultiDenseVolatileByte(_)
        | VectorStorageEnum::MultiDenseVolatileHalf(_) => None,
        // Nothing to score
        VectorStorageEnum::EmptyDense(_) => None,
    }
}

/// Takes the depth and combine from the f32 matrix operands, so both score alike.
fn f32_records<S>(storage: &S) -> Option<StoredRecords<'_>>
where
    S: DenseVectorStorageRead<VectorElementType> + Sync,
{
    let operands = DenseOperands::<VectorElementType, S>::new(storage, storage.distance())?;
    let value = match operands.combine() {
        DenseCombine::Plain => StoredValue::Unused,
        DenseCombine::Euclid => StoredValue::SquaredNorm,
        // Inverse norms: byte vectors only, which have no stored form yet
        DenseCombine::Cos => return None,
    };
    let form = StoredForm::f32(operands.depth(), value, operands.combine());
    Some(StoredRecords::new(form, Box::new(DenseRecords(storage))))
}

fn scalar_records<TStorage>(operands: ScalarOperands<'_, TStorage>) -> StoredRecords<'_>
where
    TStorage: EncodedStorage + Sync,
{
    StoredRecords::new(StoredForm::scalar(&operands), Box::new(operands))
}
