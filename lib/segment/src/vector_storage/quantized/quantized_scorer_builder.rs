use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use quantization::EncodedVectors;

use super::quantized_custom_query_scorer::QuantizedCustomQueryScorer;
use super::quantized_query_scorer::{InternalScorerUnsupported, QuantizedQueryScorer};
use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, QueryVector, VectorElementType, VectorElementTypeByte,
    VectorElementTypeHalf,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{Distance, QuantizationConfig, VectorStorageDatatype};
use crate::vector_storage::quantized::quantized_multi_custom_query_scorer::QuantizedMultiCustomQueryScorer;
use crate::vector_storage::quantized::quantized_multi_query_scorer::QuantizedMultiQueryScorer;
use crate::vector_storage::quantized::quantized_multivector_storage::{
    MultivectorOffsetsStorage, QuantizedMultivectorStorage,
};
use crate::vector_storage::query::{
    ContextQuery, DiscoverQuery, NaiveFeedbackQuery, RecoBestScoreQuery, RecoQuery,
    RecoSumScoresQuery, TransformInto,
};
use crate::vector_storage::{RawScorer, RawScorerImpl, raw_scorer_from_query_scorer};

/// Per-variant scorer dispatch for a quantized storage enum.
///
/// Implemented by both the read-write [`QuantizedVectorStorage`] and the
/// read-only `ReadOnlyQuantizedVectorStorage<S>` enums. Keeping the datatype/distance
/// and per-query dispatch in [`QuantizedScorerBuilder`] (shared) and only the
/// per-variant `match` here avoids duplicating the scorer construction logic.
pub(in crate::vector_storage::quantized) trait QuantizedScorerDispatch {
    /// Build a raw scorer for the given metric, dispatching over the storage variants.
    fn build_metric_scorer<'a, TElement, TMetric>(
        &'a self,
        builder: QuantizedScorerBuilder<'a>,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a;

    /// Build a raw scorer for the specified `point_id`, dispatching over the storage variants.
    ///
    /// If not supported, returns [`InternalScorerUnsupported`] with the original `hardware_counter`.
    fn raw_internal_scorer<'a>(
        &'a self,
        point_id: PointOffsetType,
        hardware_counter: HardwareCounterCell,
    ) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported>;
}

/// Build an internal (point-to-point) raw scorer over a single-vector encoded storage.
///
/// Shared by every storage enum variant; the per-variant `match` lives in the
/// [`QuantizedScorerDispatch`] implementations.
pub(in crate::vector_storage::quantized) fn internal_raw_scorer<'a, TEncodedVectors>(
    point_id: PointOffsetType,
    quantized_data: &'a TEncodedVectors,
    hardware_counter: HardwareCounterCell,
) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported>
where
    TEncodedVectors: EncodedVectors,
{
    let query_scorer =
        QuantizedQueryScorer::new_internal(point_id, quantized_data, hardware_counter)?;
    Ok(Box::new(RawScorerImpl { query_scorer }))
}

/// Build an internal (point-to-point) raw scorer over a multi-vector encoded storage.
pub(in crate::vector_storage::quantized) fn internal_raw_multi_scorer<
    'a,
    QuantizedStorage,
    OffsetStorage,
>(
    point_id: PointOffsetType,
    quantized_data: &'a QuantizedMultivectorStorage<QuantizedStorage, OffsetStorage>,
    hardware_counter: HardwareCounterCell,
) -> Result<Box<dyn RawScorer + 'a>, InternalScorerUnsupported>
where
    QuantizedStorage: EncodedVectors + 'a,
    OffsetStorage: MultivectorOffsetsStorage + 'a,
{
    let query_scorer =
        QuantizedMultiQueryScorer::new_internal(point_id, quantized_data, hardware_counter)?;
    Ok(Box::new(RawScorerImpl { query_scorer }))
}

/// Build a query raw scorer over a quantized storage enum.
///
/// Shared by the read-write [`QuantizedVectors`] and read-only [`QuantizedVectorsRead`]
/// so the [`QuantizedScorerBuilder`] wiring lives in a single place.
///
/// [`QuantizedVectors`]: super::quantized_vectors::QuantizedVectors
/// [`QuantizedVectorsRead`]: super::quantized_vectors::ReadOnlyQuantizedVectors
pub(in crate::vector_storage::quantized) fn build_quantized_raw_scorer<'a, Q>(
    storage: &'a Q,
    quantization_config: &'a QuantizationConfig,
    distance: &'a Distance,
    datatype: VectorStorageDatatype,
    on_disk: bool,
    query: QueryVector,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>>
where
    Q: QuantizedScorerDispatch,
{
    QuantizedScorerBuilder::new(
        quantization_config,
        query,
        distance,
        datatype,
        hardware_counter,
        on_disk,
    )
    .build(storage)
}

pub(in crate::vector_storage::quantized) struct QuantizedScorerBuilder<'a> {
    quantization_config: &'a QuantizationConfig,
    query: QueryVector,
    distance: &'a Distance,
    datatype: VectorStorageDatatype,
    hardware_counter: HardwareCounterCell,
}

impl<'a> QuantizedScorerBuilder<'a> {
    pub fn new(
        quantization_config: &'a QuantizationConfig,
        query: QueryVector,
        distance: &'a Distance,
        datatype: VectorStorageDatatype,
        mut hardware_counter: HardwareCounterCell,
        on_disk: bool,
    ) -> Self {
        hardware_counter.set_vector_io_read_multiplier(usize::from(on_disk));

        Self {
            quantization_config,
            query,
            distance,
            datatype,
            hardware_counter,
        }
    }

    pub fn build<Q: QuantizedScorerDispatch>(
        self,
        storage: &'a Q,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        match self.datatype {
            // A Turbo4 source is dequantized to `f32` before re-quantization, so
            // its query is preprocessed as `VectorElementType` like Float32.
            VectorStorageDatatype::Float32 | VectorStorageDatatype::Turbo4 => match self.distance {
                Distance::Cosine => {
                    self.build_with_metric::<VectorElementType, CosineMetric, _>(storage)
                }
                Distance::Euclid => {
                    self.build_with_metric::<VectorElementType, EuclidMetric, _>(storage)
                }
                Distance::Dot => {
                    self.build_with_metric::<VectorElementType, DotProductMetric, _>(storage)
                }
                Distance::Manhattan => {
                    self.build_with_metric::<VectorElementType, ManhattanMetric, _>(storage)
                }
            },
            VectorStorageDatatype::Uint8 => match self.distance {
                Distance::Cosine => {
                    self.build_with_metric::<VectorElementTypeByte, CosineMetric, _>(storage)
                }
                Distance::Euclid => {
                    self.build_with_metric::<VectorElementTypeByte, EuclidMetric, _>(storage)
                }
                Distance::Dot => {
                    self.build_with_metric::<VectorElementTypeByte, DotProductMetric, _>(storage)
                }
                Distance::Manhattan => {
                    self.build_with_metric::<VectorElementTypeByte, ManhattanMetric, _>(storage)
                }
            },
            VectorStorageDatatype::Float16 => match self.distance {
                Distance::Cosine => {
                    self.build_with_metric::<VectorElementTypeHalf, CosineMetric, _>(storage)
                }
                Distance::Euclid => {
                    self.build_with_metric::<VectorElementTypeHalf, EuclidMetric, _>(storage)
                }
                Distance::Dot => {
                    self.build_with_metric::<VectorElementTypeHalf, DotProductMetric, _>(storage)
                }
                Distance::Manhattan => {
                    self.build_with_metric::<VectorElementTypeHalf, ManhattanMetric, _>(storage)
                }
            },
        }
    }

    fn build_with_metric<TElement, TMetric, Q: QuantizedScorerDispatch>(
        self,
        storage: &'a Q,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
    {
        storage.build_metric_scorer::<TElement, TMetric>(self)
    }

    pub(in crate::vector_storage::quantized) fn new_quantized_scorer<TElement, TMetric>(
        self,
        quantized_storage: &'a impl EncodedVectors,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
    {
        let Self {
            quantization_config,
            query,
            distance: _,
            datatype: _,
            hardware_counter,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer = QuantizedQueryScorer::<_>::new::<TElement, TMetric>(
                    DenseVector::try_from(vector)?,
                    quantized_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::RecommendBestScore(reco_query) => {
                let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _>::new(
                    RecoBestScoreQuery::from(reco_query),
                    quantized_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::RecommendSumScores(reco_query) => {
                let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _>::new(
                    RecoSumScoresQuery::from(reco_query),
                    quantized_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::Discover(discover_query) => {
                let discover_query: DiscoverQuery<DenseVector> = discover_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _>::new(
                    discover_query,
                    quantized_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _>::new(
                    context_query,
                    quantized_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::FeedbackNaive(feedback_query) => {
                let feedback_query: NaiveFeedbackQuery<DenseVector> =
                    feedback_query.transform_into()?;
                let query_scorer = QuantizedCustomQueryScorer::<TElement, TMetric, _, _>::new(
                    feedback_query.into_query(),
                    quantized_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
        }
    }

    pub(in crate::vector_storage::quantized) fn new_multi_quantized_scorer<
        TElement,
        TMetric,
        QuantizedStorage,
        OffsetStorage,
    >(
        self,
        quantized_multivector_storage: &'a QuantizedMultivectorStorage<
            QuantizedStorage,
            OffsetStorage,
        >,
    ) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement> + 'a,
        QuantizedStorage: quantization::EncodedVectors + 'a,
        OffsetStorage: MultivectorOffsetsStorage + 'a,
    {
        let Self {
            quantization_config,
            query,
            distance: _,
            datatype: _,
            hardware_counter,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                let query_scorer = QuantizedMultiQueryScorer::new_multi::<TElement, TMetric>(
                    &MultiDenseVectorInternal::try_from(vector)?,
                    quantized_multivector_storage,
                    quantization_config,
                    hardware_counter,
                );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::RecommendBestScore(reco_query) => {
                let reco_query: RecoQuery<MultiDenseVectorInternal> =
                    reco_query.transform_into()?;
                let query_scorer =
                    QuantizedMultiCustomQueryScorer::new_multi::<TElement, TMetric, _, _>(
                        RecoBestScoreQuery::from(reco_query),
                        quantized_multivector_storage,
                        quantization_config,
                        hardware_counter,
                    );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::RecommendSumScores(reco_query) => {
                let reco_query: RecoQuery<MultiDenseVectorInternal> =
                    reco_query.transform_into()?;
                let query_scorer =
                    QuantizedMultiCustomQueryScorer::new_multi::<TElement, TMetric, _, _>(
                        RecoSumScoresQuery::from(reco_query),
                        quantized_multivector_storage,
                        quantization_config,
                        hardware_counter,
                    );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::Discover(discover_query) => {
                let discover_query: DiscoverQuery<MultiDenseVectorInternal> =
                    discover_query.transform_into()?;
                let query_scorer =
                    QuantizedMultiCustomQueryScorer::new_multi::<TElement, TMetric, _, _>(
                        discover_query,
                        quantized_multivector_storage,
                        quantization_config,
                        hardware_counter,
                    );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<MultiDenseVectorInternal> =
                    context_query.transform_into()?;
                let query_scorer =
                    QuantizedMultiCustomQueryScorer::new_multi::<TElement, TMetric, _, _>(
                        context_query,
                        quantized_multivector_storage,
                        quantization_config,
                        hardware_counter,
                    );
                raw_scorer_from_query_scorer(query_scorer)
            }
            QueryVector::FeedbackNaive(feedback_query) => {
                let feedback_query: NaiveFeedbackQuery<MultiDenseVectorInternal> =
                    feedback_query.transform_into()?;
                let query_scorer =
                    QuantizedMultiCustomQueryScorer::new_multi::<TElement, TMetric, _, _>(
                        feedback_query.into_query(),
                        quantized_multivector_storage,
                        quantization_config,
                        hardware_counter,
                    );
                raw_scorer_from_query_scorer(query_scorer)
            }
        }
    }
}
