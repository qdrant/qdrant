use common::types::ScoreType;
use quantization::EncodedQueryTQ;

use crate::data_types::primitive::{TurboQuantElement, quantizer_for_tq_slot};
use crate::data_types::vectors::{DenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::Distance;

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
    let quantizer = quantizer_for_tq_slot(v1.len(), distance);
    let v1_bytes: &[u8] = bytemuck::cast_slice(v1);
    let v2_bytes: &[u8] = bytemuck::cast_slice(v2);
    quantizer.score_symmetric(v1_bytes, v2_bytes)
}

fn turbo_score_precomputed(
    query: &EncodedQueryTQ,
    vector: &[TurboQuantElement],
    distance: Distance,
) -> ScoreType {
    let quantizer = quantizer_for_tq_slot(vector.len(), distance);
    let bytes: &[u8] = bytemuck::cast_slice(vector);
    quantizer.score_precomputed(query, bytes)
}

impl Metric<TurboQuantElement> for CosineMetric {
    fn distance() -> Distance {
        Distance::Cosine
    }

    fn similarity(v1: &[TurboQuantElement], v2: &[TurboQuantElement]) -> ScoreType {
        turbo_score_symmetric(v1, v2, Distance::Cosine)
    }

    fn query_similarity(query: &EncodedQueryTQ, vector: &[TurboQuantElement]) -> ScoreType {
        turbo_score_precomputed(query, vector, Distance::Cosine)
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

    fn query_similarity(query: &EncodedQueryTQ, vector: &[TurboQuantElement]) -> ScoreType {
        turbo_score_precomputed(query, vector, Distance::Euclid)
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

    fn query_similarity(query: &EncodedQueryTQ, vector: &[TurboQuantElement]) -> ScoreType {
        turbo_score_precomputed(query, vector, Distance::Dot)
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

    fn query_similarity(query: &EncodedQueryTQ, vector: &[TurboQuantElement]) -> ScoreType {
        turbo_score_precomputed(query, vector, Distance::Manhattan)
    }

    fn preprocess(vector: DenseVector) -> DenseVector {
        <ManhattanMetric as Metric<VectorElementType>>::preprocess(vector)
    }
}
