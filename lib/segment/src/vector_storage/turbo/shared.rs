//! Backend-agnostic TurboQuant logic shared by the single-file
//! [`TurboVectorStorageImpl`](super::turbo_vector_storage::TurboVectorStorageImpl)
//! and the appendable
//! [`AppendableMmapTurboVectorStorage`](super::appendable_turbo_vector_storage::AppendableMmapTurboVectorStorage).
//!
//! Everything here is a pure function of the quantizer, distance and
//! dimensionality — none of it touches the encoded-bytes backend — so both
//! storages delegate to it instead of duplicating the codec/scoring arithmetic.

use std::borrow::Cow;

use common::types::{PointOffsetType, ScoreType};
use quantization::encoded_storage::EncodedStorage;
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{EncodedQueryTQ, TQBits, TQMode, TQRotation};

use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, VectorStorageDatatype};

// TurboQuant DataType (TQDT) storages run without shift+scale error correction.
pub(crate) const TQDT_MODE: TQMode = TQMode::Normal;
pub(crate) const TQDT_ROTATION: TQRotation = TQRotation::Unpadded;

/// Encoded vectors file of the single-file (non-appendable) layout.
pub(crate) const VECTORS_PATH: &str = "tq_vectors.dat";
/// Encoded vectors directory (chunked) of the appendable layout.
pub(crate) const VECTORS_DIR_PATH: &str = "tq_vectors";
pub(crate) const DELETED_DIR_PATH: &str = "deleted";

/// Bit width of a TurboQuant datatype, `None` for the other datatypes.
pub fn datatype_bits(datatype: VectorStorageDatatype) -> Option<TQBits> {
    match datatype {
        VectorStorageDatatype::Turbo4 => Some(TQBits::Bits4),
        VectorStorageDatatype::Turbo8 => Some(TQBits::Bits8),
        VectorStorageDatatype::Float32
        | VectorStorageDatatype::Float16
        | VectorStorageDatatype::Uint8 => None,
    }
}

/// Bit width of a TurboQuant datatype.
///
/// # Panics
/// Panics for a non-TurboQuant datatype.
pub fn tq_bits(datatype: VectorStorageDatatype) -> TQBits {
    datatype_bits(datatype).unwrap_or_else(|| panic!("{datatype:?} is not a TurboQuant datatype"))
}

/// The datatype of a storage encoded with `quantizer`.
pub(crate) fn storage_datatype(quantizer: &TurboQuantizer) -> VectorStorageDatatype {
    match quantizer.bits() {
        TQBits::Bits8 => VectorStorageDatatype::Turbo8,
        TQBits::Bits4 => VectorStorageDatatype::Turbo4,
        bits @ (TQBits::Bits2 | TQBits::Bits1_5 | TQBits::Bits1) => {
            unreachable!("no TurboQuant datatype stores {bits:?}")
        }
    }
}

/// Build the quantizer for a dense TurboQuant datatype storage; fully
/// determined by `(dim, distance, bits)` and the fixed TQDT constants.
pub(crate) fn build_quantizer(dim: usize, distance: Distance, bits: TQBits) -> TurboQuantizer {
    TurboQuantizer::new(
        dim,
        bits,
        TQDT_MODE,
        quantization::DistanceType::from(distance),
        TQDT_ROTATION,
        None,
    )
}

/// Size in bytes of one encoded vector of a dense `Turbo4` storage, or of one
/// inner vector of a multivector one. Equal to
/// `build_quantizer(dim, distance).quantized_size()`, without building the
/// rotation tables, so it is cheap enough to call per point.
pub(crate) fn quantized_vector_size(dim: usize, distance: Distance) -> usize {
    let vector_parameters = quantization::VectorParameters {
        dim,
        distance_type: quantization::DistanceType::from(distance),
        invert: false,
        deprecated_count: None,
    };
    quantization::encoded_vectors_tq::get_quantized_vector_size(
        &vector_parameters,
        TQDT_BITS,
        TQDT_MODE,
    )
}

/// Quantize then dequantize `vector` exactly as a dense TQ storage with this
/// `distance` does across `insert_vector` + `get_vector`. Pure function of its inputs:
/// the quantizer is fully determined by `(dim, distance, bits)` (the rotation derives
/// from fixed seeds), so the result is identical across storage instances, segment
/// rebuilds, and reloads. Lets model-based tests predict the read-back value of a
/// TurboQuant-backed vector without opening a storage.
pub fn turbo_storage_roundtrip(vector: &[f32], distance: Distance, bits: TQBits) -> Vec<f32> {
    let dim = vector.len();
    let quantizer = build_quantizer(dim, distance, bits);
    let mut buf = vec![0.0; quantizer.get_padded_dim()];
    let encoded = quantizer.quantize(vector, &mut buf);
    // Mirror of `dequantize_vector`: dequantize, rotate back, drop the padding
    // tail, cast to f32.
    let mut dequantized = quantizer.dequantize::<f64>(&encoded);
    quantizer.apply_inverse_rotation(&mut dequantized);
    dequantized[..dim].iter().map(|&x| x as f32).collect()
}

/// Whether scores must be negated to follow qdrant's "higher = better"
/// convention: TurboQuant returns a distance (lower = better) for the
/// Euclid/Manhattan metrics, mirroring `VectorParameters::invert`.
pub(super) fn invert_score(distance: Distance) -> bool {
    matches!(distance, Distance::Euclid | Distance::Manhattan)
}

/// Preprocess a raw query for `distance` (e.g. cosine normalization) and
/// precompute its asymmetric-scoring encoding. The returned [`EncodedQueryTQ`]
/// is reused across all `score_query_bytes` calls so the Hadamard rotation runs
/// once, not per score.
pub(super) fn preprocess_query(
    quantizer: &TurboQuantizer,
    distance: Distance,
    query: DenseVector,
) -> EncodedQueryTQ {
    let preprocessed = match distance {
        Distance::Cosine => <CosineMetric as Metric<VectorElementType>>::preprocess(query),
        Distance::Euclid => <EuclidMetric as Metric<VectorElementType>>::preprocess(query),
        Distance::Dot => <DotProductMetric as Metric<VectorElementType>>::preprocess(query),
        Distance::Manhattan => <ManhattanMetric as Metric<VectorElementType>>::preprocess(query),
    };
    quantizer.precompute_query(&preprocessed)
}

/// Asymmetric score of a precomputed query against already-fetched encoded
/// `bytes`, applying the metric sign convention. Pure: no IO, no hardware
/// accounting.
pub(crate) fn score_query_bytes(
    quantizer: &TurboQuantizer,
    distance: Distance,
    query: &EncodedQueryTQ,
    bytes: &[u8],
) -> ScoreType {
    let score = quantizer.score_precomputed(query, bytes);
    if invert_score(distance) {
        -score
    } else {
        score
    }
}

/// Batch counterpart of [`score_query_bytes`] over an [`EncodedStorage`]:
/// `scores[i]` ← score of `query` against `ids[i]`.  Coalesces consecutive ids
/// into contiguous storage runs and scores each run with one batched quantizer
/// call, so a sequential scan pays the storage resolution and kernel setup per
/// run rather than per vector.
///
/// Whether an id list takes the run path is the storage's call, see
/// [`EncodedStorage::prefers_run_scoring`]: lists whose runs are short on
/// average — HNSW neighbors, and filtered scans sparse enough that consecutive
/// ids are incidental — fall back to per-vector [`score_query_bytes`].
pub(super) fn score_query_batch<TStorage: EncodedStorage>(
    storage: &TStorage,
    quantizer: &TurboQuantizer,
    distance: Distance,
    query: &EncodedQueryTQ,
    ids: &[PointOffsetType],
    scores: &mut [ScoreType],
) {
    debug_assert_eq!(ids.len(), scores.len());

    if !TStorage::prefers_run_scoring(ids) {
        storage.for_each_batch(ids, |idx, bytes| {
            scores[idx] = score_query_bytes(quantizer, distance, query, &bytes);
        });
        return;
    }

    // The record size every Turbo datatype storage is created with.
    let stride = quantizer.quantized_size();
    storage.for_each_run(ids, |first, count, bytes| {
        quantizer.score_precomputed_batch(query, &bytes, stride, &mut scores[first..first + count]);
    });

    if invert_score(distance) {
        for score in scores {
            *score = -*score;
        }
    }
}

/// Symmetric score between two encoded vectors, applying the metric sign
/// convention.
pub(super) fn score_symmetric_bytes(
    quantizer: &TurboQuantizer,
    distance: Distance,
    a: &[u8],
    b: &[u8],
) -> ScoreType {
    let score = quantizer.score_symmetric(a, b);
    if invert_score(distance) {
        -score
    } else {
        score
    }
}

/// Dequantize + inverse-rotate a stored encoded vector back to `f32`, dropping
/// the padding tail: callers expect the original dimensionality.
pub(super) fn dequantize_vector<'a>(
    quantizer: &TurboQuantizer,
    dim: usize,
    quantized: &[u8],
) -> CowVector<'a> {
    let mut dequantized = quantizer.dequantize::<f64>(quantized);
    quantizer.apply_inverse_rotation(&mut dequantized);
    CowVector::Dense(Cow::Owned(
        dequantized[..dim].iter().map(|i| *i as f32).collect(),
    ))
}

/// Dequantize a stored encoded vector for a requantization build. When
/// `keep_rotated` is set the inverse rotation is skipped and the result is
/// dequantized straight into `f32`, avoiding the intermediate `Vec<f64>` that
/// the rotation would need.
pub(super) fn dequantize_for_requantization(
    quantizer: &TurboQuantizer,
    dim: usize,
    quantized: &[u8],
    keep_rotated: bool,
) -> DenseVector {
    if keep_rotated {
        let mut dequantized = quantizer.dequantize::<VectorElementType>(quantized);
        dequantized.truncate(dim);
        dequantized
    } else {
        let mut dequantized = quantizer.dequantize::<f64>(quantized);
        quantizer.apply_inverse_rotation(&mut dequantized);
        dequantized[..dim]
            .iter()
            .map(|&x| x as VectorElementType)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cheap size helper must match the record size of the quantizer every
    /// TQ storage builds, or merged placeholders would misalign records.
    #[test]
    fn quantized_vector_size_matches_quantizer() {
        let distances = [
            Distance::Cosine,
            Distance::Euclid,
            Distance::Dot,
            Distance::Manhattan,
        ];
        for distance in distances {
            for dim in [1, 4, 5, 127, 256, 1023] {
                assert_eq!(
                    quantized_vector_size(dim, distance),
                    build_quantizer(dim, distance).quantized_size(),
                    "dim {dim}, distance {distance:?}",
                );
            }
        }
    }
}
