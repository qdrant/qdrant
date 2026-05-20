//! Storage element wrapping a TurboQuant-encoded byte. Layout queries delegate
//! to [`turboquant::quantization::TurboQuantizer`] so the on-storage size and
//! padding stays consistent with the encoder. Encoding / decoding / scoring are
//! still stubbed.

use std::alloc::Layout;
use std::borrow::Cow;

use bytemuck::{Pod, Zeroable};
use common::types::ScoreType;
use serde::{Deserialize, Serialize};
use turboquant::quantization::TurboQuantizer;
use turboquant::{DistanceType, TQBits, TQMode};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};

/// Bit width of the TurboQuant codebook this storage uses. Fixed for now —
/// promoted to a per-collection config once the rest of the pipeline lands.
const BITS: TQBits = TQBits::Bits4;
/// TurboQuant mode. `Normal` requires no dataset-level statistics so it slots
/// cleanly into stateless `PrimitiveVectorElement` methods. `Plus` will need
/// the storage to own a configured `TurboQuantizer` instance and is deferred.
const MODE: TQMode = TQMode::Normal;

fn to_tq_distance(distance: Distance) -> DistanceType {
    match distance {
        Distance::Cosine => DistanceType::Cosine,
        Distance::Dot => DistanceType::Dot,
        Distance::Euclid => DistanceType::L2,
        Distance::Manhattan => DistanceType::L1,
    }
}

/// One byte of a TurboQuant-encoded vector. `repr(transparent)` over `u8`; the
/// surrounding `[TurboQuantElement]` slot is `dim → padded_dim` quantized
/// values plus the metric-specific `extras` trailer that TurboQuant appends.
#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    Serialize,
    Deserialize,
    FromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
    Zeroable,
    Pod,
)]
#[repr(transparent)]
pub struct TurboQuantElement(pub u8);

impl PrimitiveVectorElement for TurboQuantElement {
    fn slice_from_float_cow(vector: Cow<[VectorElementType]>, distance: Distance) -> Cow<[Self]> {
        let api_dim = vector.len();
        let quantizer = TurboQuantizer::new(api_dim, BITS, MODE, to_tq_distance(distance), None);
        let mut buf = vec![0.0_f64; quantizer.padded_dim];
        let bytes = quantizer.quantize(&vector, &mut buf);
        // `Vec<u8>` → `Vec<TurboQuantElement>` is sound because the element is
        // `#[repr(transparent)]` over `u8` and `Pod`. `cast_vec` handles the
        // matching size/align check and reuses the allocation.
        Cow::Owned(bytemuck::allocation::cast_vec(bytes))
    }

    fn slice_to_float_cow(vector: Cow<[Self]>, distance: Distance) -> Cow<[VectorElementType]> {
        // Public/API path: always returns vectors in the original basis.
        decode(&vector, distance, /* apply_inverse_rotation */ true)
    }

    fn decode_for_quantization(
        vector: Cow<[Self]>,
        distance: Distance,
    ) -> Cow<[VectorElementType]> {
        // For L2/Cosine/Dot the score is rotation-invariant, so we keep the
        // data in TurboQuant's rotated basis — the downstream quantizer is
        // expected to skip its own rotation step. L1 is not invariant under
        // rotation; revert before passing to the next stage.
        let revert = matches!(distance, Distance::Manhattan);
        decode(&vector, distance, revert)
    }

    fn is_prerotated_for_quantization(distance: Distance) -> bool {
        // Symmetric to `decode_for_quantization`: rotated basis exposed for
        // every rotation-invariant metric; not for L1.
        !matches!(distance, Distance::Manhattan)
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        distance: Distance,
        vector: Cow<'a, [Self]>,
    ) -> Cow<'a, [f32]> {
        Self::decode_for_quantization(vector, distance)
    }

    fn datatype() -> VectorStorageDatatype {
        VectorStorageDatatype::Uint8
    }

    fn from_float_multivector(
        _multivector: CowMultiVector<VectorElementType>,
    ) -> CowMultiVector<Self> {
        unimplemented!("TurboQuantElement::from_float_multivector")
    }

    fn into_float_multivector(
        _multivector: CowMultiVector<Self>,
    ) -> CowMultiVector<VectorElementType> {
        unimplemented!("TurboQuantElement::into_float_multivector")
    }

    /// Byte layout of one on-storage slot. Source of truth is the quantizer
    /// itself — it knows its own padding and extras shape. We just reflect
    /// what `quantized_size_for` reports back as `Layout`.
    fn storage_layout(api_dim: usize, distance: Distance) -> Layout {
        let bytes =
            TurboQuantizer::quantized_size_for(api_dim, BITS, to_tq_distance(distance), MODE);
        Layout::from_size_align(bytes, align_of::<f32>()).expect("valid layout")
    }

    /// Recover api-level dimension from the slot length. TQ rounds the
    /// requested `dim` up to a `bit_size`-aligned `padded_dim`; once rounded
    /// up we cannot tell the original dim apart from any other dim mapping to
    /// the same padded_dim. By convention this returns `padded_dim` — the
    /// effective api_dim after TQ's rounding.
    fn api_dim_from_storage_len(storage_len: usize, distance: Distance) -> usize {
        // `quantized_size_for(0, …)` returns only the extras trailer (since
        // padded_dim(0) == 0 yields zero packed bytes). Subtract it off to
        // recover the packed-data byte count.
        let extras_size =
            TurboQuantizer::quantized_size_for(0, BITS, to_tq_distance(distance), MODE);
        let packed_bytes = storage_len
            .checked_sub(extras_size)
            .expect("storage_len shorter than TurboQuant extras trailer");
        // `padded_bytes_to_dim` is implemented locally rather than via a
        // `TQBits` getter because `TQBits::bit_size` is crate-private. Two
        // public `quantized_size_for` calls let us infer the relationship.
        padded_bytes_to_dim(packed_bytes)
    }
}

/// Shared decode helper for [`TurboQuantElement::slice_to_float_cow`] and
/// [`TurboQuantElement::decode_for_quantization`]. Builds a stateless
/// quantizer, calls `dequantize`, optionally applies the inverse rotation,
/// and downcasts to `f32`. The output length is `padded_dim` (see
/// `api_dim_from_storage_len` for the dim convention).
fn decode(
    vector: &[TurboQuantElement],
    distance: Distance,
    apply_inverse_rotation: bool,
) -> Cow<'static, [VectorElementType]> {
    let api_dim = TurboQuantElement::api_dim_from_storage_len(vector.len(), distance);
    let quantizer = TurboQuantizer::new(api_dim, BITS, MODE, to_tq_distance(distance), None);
    let bytes: &[u8] = bytemuck::cast_slice(vector);
    let mut deq = quantizer.dequantize(bytes);
    if apply_inverse_rotation {
        quantizer.rotation.apply_inverse(&mut deq);
    }
    Cow::Owned(deq.into_iter().map(|x| x as f32).collect())
}

/// `padded_dim` from the number of bytes a TurboQuant slot devotes to packed
/// data. Probes the quantizer with two payload sizes and back-fits the
/// `padded_dim → packed_bytes` linear factor (`bit_size / 8`). Kept here so we
/// don't depend on `TQBits` internals.
fn padded_bytes_to_dim(packed_bytes: usize) -> usize {
    // Probe with two dims whose `quantized_size_for` differs only by the
    // packed-data portion. The smallest non-trivial step that survives every
    // bit-width's `padded_dim` rounding is `next_multiple_of(8)` — i.e. 64.
    let extras = TurboQuantizer::quantized_size_for(0, BITS, DistanceType::Cosine, MODE);
    let probe_dim = 64;
    let probe_packed =
        TurboQuantizer::quantized_size_for(probe_dim, BITS, DistanceType::Cosine, MODE) - extras;
    // `packed_bytes / probe_packed * probe_dim` gives us padded_dim back.
    packed_bytes * probe_dim / probe_packed
}

/// Symmetric score helper shared by every `Metric<TurboQuantElement>` impl.
/// Recovers `api_dim` from the slot length, builds a stateless quantizer,
/// reinterprets both slices as bytes, and delegates to `score_symmetric`.
fn turbo_score_symmetric(
    v1: &[TurboQuantElement],
    v2: &[TurboQuantElement],
    distance: Distance,
) -> ScoreType {
    debug_assert_eq!(
        v1.len(),
        v2.len(),
        "TurboQuant symmetric score requires matching slot lengths"
    );
    let api_dim = TurboQuantElement::api_dim_from_storage_len(v1.len(), distance);
    let quantizer = TurboQuantizer::new(api_dim, BITS, MODE, to_tq_distance(distance), None);
    let v1_bytes: &[u8] = bytemuck::cast_slice(v1);
    let v2_bytes: &[u8] = bytemuck::cast_slice(v2);
    quantizer.score_symmetric(v1_bytes, v2_bytes)
}

// `Metric<TurboQuantElement>` reuses the f32 metric's `preprocess` (it operates
// on api-level floats, oblivious to the storage encoding) and routes
// `similarity` through `TurboQuantizer::score_symmetric`.

impl Metric<TurboQuantElement> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[TurboQuantElement], v2: &[TurboQuantElement]) -> ScoreType {
        turbo_score_symmetric(v1, v2, Distance::Cosine)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        <CosineMetric as Metric<VectorElementType>>::preprocess(vector)
    }
}

impl Metric<TurboQuantElement> for EuclidMetric {
    fn distance() -> Distance {
        Distance::Euclid
    }

    fn similarity(v1: &[TurboQuantElement], v2: &[TurboQuantElement]) -> ScoreType {
        turbo_score_symmetric(v1, v2, Distance::Euclid)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        <EuclidMetric as Metric<VectorElementType>>::preprocess(vector)
    }
}

impl Metric<TurboQuantElement> for DotProductMetric {
    fn distance() -> Distance {
        Distance::Dot
    }

    fn similarity(v1: &[TurboQuantElement], v2: &[TurboQuantElement]) -> ScoreType {
        turbo_score_symmetric(v1, v2, Distance::Dot)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        <DotProductMetric as Metric<VectorElementType>>::preprocess(vector)
    }
}

impl Metric<TurboQuantElement> for ManhattanMetric {
    fn distance() -> Distance {
        Distance::Manhattan
    }

    fn similarity(v1: &[TurboQuantElement], v2: &[TurboQuantElement]) -> ScoreType {
        turbo_score_symmetric(v1, v2, Distance::Manhattan)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        <ManhattanMetric as Metric<VectorElementType>>::preprocess(vector)
    }
}
