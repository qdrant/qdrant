use common::ambient::hw::HwScale;
use common::types::{PointOffsetType, ScoreType};
use quantization::encoded_vectors_u8::EncodedQueryBatchU8;
use quantization::{EncodedStorage, EncodedVectors, EncodedVectorsU8};

use super::quantized_query_scorer::encode_query;
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    QueryVector, VectorElementType, VectorElementTypeByte, VectorElementTypeHalf, VectorInternal,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};

/// A query-major score matrix over one shared block of point ids.
pub trait RawBatchScorer {
    /// Fill one score row per prepared query, preserving point order and propagating read errors.
    fn score_points(
        &mut self,
        points: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()>;
}

struct ScalarBatchScorer<'a, S: EncodedStorage> {
    storage: &'a EncodedVectorsU8<S>,
    queries: EncodedQueryBatchU8,
    hw: HwScale,
}

impl<S: EncodedStorage> RawBatchScorer for ScalarBatchScorer<'_, S> {
    /// Score all prepared queries while charging encoded-vector I/O once for the shared block.
    fn score_points(
        &mut self,
        points: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        // The encoded block is read once even though it contributes to every query.
        self.hw
            .vector_io_read(points.len() * self.storage.quantized_vector_size());
        self.storage
            .score_query_batch(&mut self.queries, points, scores)?;
        Ok(())
    }
}

/// Build a shared scorer for supported dense nearest queries, source datatypes and CPU kernels.
/// Return `None` when callers should retain their independent query scorers.
pub(super) fn scalar_batch_scorer<'a, S: EncodedStorage>(
    storage: &'a EncodedVectorsU8<S>,
    queries: &[&QueryVector],
    quantization_config: &QuantizationConfig,
    distance: Distance,
    datatype: VectorStorageDatatype,
) -> Option<Box<dyn RawBatchScorer + 'a>> {
    // Custom queries and multivectors retain the existing scorer and its validation.
    if queries.len() < 2
        || !queries
            .iter()
            .all(|query| matches!(query, QueryVector::Nearest(VectorInternal::Dense(_))))
    {
        return None;
    }
    macro_rules! with_element {
        ($element:ty) => {
            match distance {
                Distance::Cosine => {
                    prepare::<$element, CosineMetric, _>(storage, queries, quantization_config)
                }
                Distance::Dot => {
                    prepare::<$element, DotProductMetric, _>(storage, queries, quantization_config)
                }
                Distance::Euclid => {
                    prepare::<$element, EuclidMetric, _>(storage, queries, quantization_config)
                }
                Distance::Manhattan => None,
            }
        };
    }
    match datatype {
        VectorStorageDatatype::Float32
        | VectorStorageDatatype::Turbo4
        | VectorStorageDatatype::Turbo8
        | VectorStorageDatatype::Turbo16 => with_element!(VectorElementType),
        VectorStorageDatatype::Float16 => with_element!(VectorElementTypeHalf),
        VectorStorageDatatype::Uint8 => with_element!(VectorElementTypeByte),
    }
}

/// Lazily encode validated dense queries with their source datatype and metric preprocessing.
/// Unsupported batch kernels return before the query iterator is consumed.
fn prepare<'a, E, M, S>(
    storage: &'a EncodedVectorsU8<S>,
    queries: &[&QueryVector],
    config: &QuantizationConfig,
) -> Option<Box<dyn RawBatchScorer + 'a>>
where
    E: PrimitiveVectorElement,
    M: Metric<E>,
    S: EncodedStorage,
{
    let queries = queries.iter().map(|query| {
        let QueryVector::Nearest(VectorInternal::Dense(vector)) = query else {
            unreachable!("checked by scalar_batch_scorer")
        };
        encode_query::<E, M, _>(vector.clone(), storage, config)
    });
    let queries = storage.query_batch(queries)?;
    Some(Box::new(ScalarBatchScorer {
        storage,
        queries,
        hw: HwScale {
            cpu: 1,
            vector_io_read: usize::from(storage.is_cold()),
        },
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::ambient;
    use quantization::encoded_storage::TestEncodedStorageBuilder;
    use quantization::encoded_vectors_u8::{ScalarQuantizationMethod, get_quantized_vector_size};
    use quantization::{DistanceType, VectorParameters};

    use super::*;
    use crate::types::ScalarQuantizationConfig;
    use crate::vector_storage::quantized::quantized_query_scorer::QuantizedQueryScorer;
    use crate::vector_storage::query_scorer::QueryScorer;

    /// Compare source-datatype preprocessing bitwise and reject custom or multivector queries.
    #[allow(deprecated)]
    fn check_preprocessing<E, M>(distance: DistanceType)
    where
        E: PrimitiveVectorElement,
        M: Metric<E>,
    {
        let _scope = ambient::test_guard();
        let data: Vec<Vec<f32>> = (0..13)
            .map(|v| {
                (0..19)
                    .map(|d| ((v * 13 + d * 31) % 47) as f32 - 10.0)
                    .collect()
            })
            .collect();
        let params = VectorParameters {
            dim: 19,
            distance_type: distance,
            invert: false,
            deprecated_count: None,
        };
        let vectors = EncodedVectorsU8::encode(
            data.iter(),
            std::iter::repeat_n(true, data.len()),
            TestEncodedStorageBuilder::new(None, get_quantized_vector_size(&params)),
            &params,
            None,
            ScalarQuantizationMethod::Int8,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let queries: Vec<_> = data[..5]
            .iter()
            .map(|v| QueryVector::Nearest(VectorInternal::Dense(v.clone())))
            .collect();
        let refs: Vec<_> = queries.iter().collect();
        let config = QuantizationConfig::from(ScalarQuantizationConfig {
            r#type: crate::types::ScalarType::Int8,
            quantile: None,
            always_ram: None,
            memory: None,
        });
        let custom = QueryVector::RecommendBestScore(crate::vector_storage::query::RecoQuery::new(
            vec![VectorInternal::Dense(data[0].clone())],
            vec![],
        ));
        let multi = QueryVector::Nearest(VectorInternal::MultiDense(
            crate::data_types::vectors::MultiDenseVectorInternal::new(data[0].clone(), 19),
        ));
        for unsupported in [&custom, &multi] {
            assert!(
                scalar_batch_scorer(
                    &vectors,
                    &[refs[0], unsupported],
                    &config,
                    M::distance(),
                    VectorStorageDatatype::Float32
                )
                .is_none()
            );
        }
        let Some(mut batch) = prepare::<E, M, _>(&vectors, &refs, &config) else {
            return;
        };
        let ids = [12, 0, 3, 3, 1, 6, 4];
        let mut actual = vec![0.0; ids.len() * queries.len()];
        batch.score_points(&ids, &mut actual).unwrap();
        for (q, raw) in data[..5].iter().enumerate() {
            let reference = QuantizedQueryScorer::new::<E, M>(raw.clone(), &vectors, &config);
            let mut expected = vec![0.0; ids.len()];
            reference.score_stored_batch(&ids, &mut expected).unwrap();
            assert_eq!(
                actual[q * ids.len()..(q + 1) * ids.len()]
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
                expected.into_iter().map(f32::to_bits).collect::<Vec<_>>()
            );
        }
    }

    /// Exercise shared preprocessing for Float32, Float16 and Uint8 across supported distances.
    #[test]
    fn scalar_batch_reuses_source_datatype_preprocessing() {
        macro_rules! check {
            ($element:ty) => {
                check_preprocessing::<$element, CosineMetric>(DistanceType::Cosine);
                check_preprocessing::<$element, DotProductMetric>(DistanceType::Dot);
                check_preprocessing::<$element, EuclidMetric>(DistanceType::L2);
            };
        }
        check!(VectorElementType);
        check!(VectorElementTypeHalf);
        check!(VectorElementTypeByte);
    }
}
