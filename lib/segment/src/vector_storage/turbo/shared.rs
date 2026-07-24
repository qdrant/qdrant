//! Backend-agnostic TurboQuant logic shared by the single-file
//! [`TurboVectorStorageImpl`](super::turbo_vector_storage::TurboVectorStorageImpl)
//! and the appendable
//! [`AppendableMmapTurboVectorStorage`](super::appendable_turbo_vector_storage::AppendableMmapTurboVectorStorage).
//!
//! Everything here is a pure function of the quantizer, distance and
//! dimensionality — none of it touches the encoded-bytes backend — so both
//! storages delegate to it instead of duplicating the codec/scoring arithmetic.

use std::borrow::Cow;

use common::types::ScoreType;
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::turboquant::{EncodedQueryTQ, TQBits, TQMode, TQRotation};

use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::Distance;

// TurboQuant DataType (TQDT) always uses 4 bits without shift+scale error correction.
pub(crate) const TQDT_BITS: TQBits = TQBits::Bits4;
pub(crate) const TQDT_MODE: TQMode = TQMode::Normal;
pub(crate) const TQDT_ROTATION: TQRotation = TQRotation::Unpadded;

pub(crate) const VECTORS_PATH: &str = "tq_vectors.dat";
pub(crate) const DELETED_PATH: &str = "deleted.dat";

/// Build the quantizer for a dense `Turbo4` storage; fully determined by
/// `(dim, distance)` and the fixed TQDT constants.
pub(super) fn build_quantizer(dim: usize, distance: Distance) -> TurboQuantizer {
    TurboQuantizer::new(
        dim,
        TQDT_BITS,
        TQDT_MODE,
        quantization::DistanceType::from(distance),
        TQDT_ROTATION,
        None,
    )
}

/// Quantize then dequantize `vector` exactly as a dense TQ storage with this
/// `distance` does across `insert_vector` + `get_vector`. Pure function of its inputs:
/// the quantizer is fully determined by `(dim, distance)` (the rotation derives from
/// fixed seeds), so the result is identical across storage instances, segment rebuilds,
/// and reloads. Lets model-based tests predict the read-back value of a Turbo4-backed
/// vector without opening a storage.
pub fn turbo_storage_roundtrip(vector: &[f32], distance: Distance) -> Vec<f32> {
    let dim = vector.len();
    let quantizer = build_quantizer(dim, distance);
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
pub(super) fn score_query_bytes(
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
