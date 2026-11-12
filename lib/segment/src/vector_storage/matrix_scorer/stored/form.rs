use pairwise::MatrixOperands as _;
use quantization::encoded_storage::EncodedStorage;
use quantization::encoded_vectors_u8::ScalarOperands;

use super::StoredCombine;
use crate::data_types::vectors::VectorElementType;
use crate::vector_storage::matrix_scorer::DenseCombine;
use crate::vector_storage::matrix_scorer::dense_operands::squared_norm;

/// The layout of one stored record, and how two records score.
///
/// A record is [`Self::record_bytes`] bytes. Its [`Self::depth`] elements, of the type of
/// [`Self::format`], start at byte [`Self::elements_at`]. The score of records `a` and `b` is
/// `combine.score(dot, &[value a, 0, 0], &[value b, 0, 0])`, where `dot` is the inner product of
/// their elements as the CPU kernels compute it for the format, and the value of each is what
/// [`Self::value`] says.
///
/// `a` is the record of the row the score goes to, `b` the record of the column, as the CPU
/// kernels pass the row's values as `aux_a` and the column's as `aux_b`. Not every combine is
/// symmetric in them, so rows scored against each other combine each pair once per direction.
#[derive(Clone, Copy, Debug)]
pub struct StoredForm {
    format: StoredFormat,
    record_bytes: usize,
    elements_at: usize,
    depth: usize,
    value: StoredValue,
    combine: StoredCombine,
}

/// The type of a record's elements, and so the inner product of two records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredFormat {
    /// f32 in native byte order. The inner product is `<f32 as KernelElem>::dot`: fused
    /// multiply-adds in element order, restarted from 0 every 256 elements, the slices added up
    /// in order.
    F32,
    /// Scalar quantization codes, one `u8` each, at most 127. The inner product is
    /// `<u8 as KernelElem>::dot`: exact in `i32`, converted to f32 once.
    Sq,
}

/// Where the value a record is combined with, `aux[0]`, comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredValue {
    /// The combine doesn't read it.
    Unused,
    /// An f32 in native byte order, at this byte of the record.
    InRecord { at: usize },
    /// The squared norm of the record's elements: each element widened to f64 and squared, the
    /// squares summed in f64 in element order, the sum rounded to f32 once.
    SquaredNorm,
}

impl StoredForm {
    /// Original f32 vectors: `depth` elements from byte 0, nothing else.
    pub(super) fn f32(depth: usize, value: StoredValue, combine: DenseCombine) -> Self {
        Self {
            format: StoredFormat::F32,
            record_bytes: depth * size_of::<VectorElementType>(),
            elements_at: 0,
            depth,
            value,
            combine: StoredCombine::Dense(combine),
        }
    }

    /// Scalar quantized vectors, laid out as the storage says: the offset inside the record,
    /// the codes after it.
    pub(super) fn scalar<TStorage: EncodedStorage + Sync>(
        operands: &ScalarOperands<TStorage>,
    ) -> Self {
        Self {
            format: StoredFormat::Sq,
            record_bytes: operands.record_size(),
            elements_at: operands.codes_at(),
            depth: operands.depth(),
            value: StoredValue::InRecord {
                at: operands.offset_at(),
            },
            combine: StoredCombine::Scalar(operands.combine()),
        }
    }

    pub fn format(&self) -> StoredFormat {
        self.format
    }

    /// Bytes of one record.
    pub fn record_bytes(&self) -> usize {
        self.record_bytes
    }

    /// The byte of a record where its elements start.
    pub fn elements_at(&self) -> usize {
        self.elements_at
    }

    /// Elements per record, the length of a decoded vector (`MatrixOperands::depth`).
    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn value(&self) -> StoredValue {
        self.value
    }

    /// The value `record` is combined with, `aux[0]`, as [`Self::value`] says: bitwise what the
    /// matrix operands' `decode_batch` writes to `aux[i][0]` for the same record. Doesn't
    /// allocate; reads the record unaligned, in native byte order.
    ///
    /// [`StoredValue::SquaredNorm`] is the squared norm of the record's elements for every
    /// format, codes widened to f32 first, though only f32 forms take it today.
    ///
    /// # Panics
    ///
    /// If `record` is shorter than [`Self::record_bytes`].
    pub fn value_of(&self, record: &[u8]) -> f32 {
        let record = &record[..self.record_bytes];
        match self.value {
            StoredValue::Unused => 0.0,
            StoredValue::InRecord { at } => {
                let (bytes, _) = record[at..]
                    .split_first_chunk()
                    .expect("an f32 in the record");
                f32::from_ne_bytes(*bytes)
            }
            StoredValue::SquaredNorm => match self.format {
                StoredFormat::F32 => {
                    let bytes = &record[self.elements_at..][..self.depth * size_of::<f32>()];
                    let (elements, _) = bytes.as_chunks();
                    squared_norm(elements.iter().map(|&x| f32::from_ne_bytes(x)))
                }
                StoredFormat::Sq => {
                    let codes = &record[self.elements_at..][..self.depth];
                    squared_norm(codes.iter().map(|&x| f32::from(x)))
                }
            },
        }
    }

    pub fn combine(&self) -> StoredCombine {
        self.combine
    }
}
