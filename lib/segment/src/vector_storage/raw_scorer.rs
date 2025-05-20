use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::BitSliceExt as _;
use common::types::{PointOffsetType, ScoreType};
use sparse::common::sparse_vector::SparseVector;

use super::query::{
    ContextQuery, DiscoveryQuery, RecoBestScoreQuery, RecoQuery, RecoSumScoresQuery, TransformInto,
};
use super::query_scorer::custom_query_scorer::CustomQueryScorer;
use super::query_scorer::multi_custom_query_scorer::MultiCustomQueryScorer;
use super::query_scorer::sparse_custom_query_scorer::SparseCustomQueryScorer;
use super::{DenseVectorStorage, MultiVectorStorage, SparseVectorStorage, VectorStorageEnum};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, QueryVector, VectorElementType, VectorElementTypeByte,
    VectorElementTypeHalf,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::Distance;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::query_scorer::metric_query_scorer::MetricQueryScorer;
use crate::vector_storage::query_scorer::multi_metric_query_scorer::MultiMetricQueryScorer;

pub trait RawScorer {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoreType]);

    /// Score stored vector with vector under the given index
    fn score_point(&self, point: PointOffsetType) -> ScoreType;

    /// Return distance between stored points selected by IDs
    ///
    /// # Panics
    ///
    /// Panics if any id is out of range
    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;
}

pub struct RawScorerImpl<TVector: ?Sized, TQueryScorer>
where
    TQueryScorer: QueryScorer<TVector>,
{
    pub query_scorer: TQueryScorer,
    vector: std::marker::PhantomData<*const TVector>,
}

pub fn new_raw_scorer<'a>(
    query: QueryVector,
    vector_storage: &'a VectorStorageEnum,
    hc: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage {
        VectorStorageEnum::DenseSimple(vs) => raw_scorer_impl(query, vs, hc),
        VectorStorageEnum::DenseSimpleByte(vs) => raw_scorer_byte_impl(query, vs, hc),
        VectorStorageEnum::DenseSimpleHalf(vs) => raw_scorer_half_impl(query, vs, hc),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatile(vs) => raw_scorer_impl(query, vs, hc),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileByte(vs) => raw_scorer_byte_impl(query, vs, hc),
        #[cfg(test)]
        VectorStorageEnum::DenseVolatileHalf(vs) => raw_scorer_half_impl(query, vs, hc),

        VectorStorageEnum::DenseMemmap(vs) => {
            if vs.has_async_reader() {
                #[cfg(target_os = "linux")]
                {
                    let scorer_result = super::async_raw_scorer::new(query.clone(), vs, hc.fork());
                    match scorer_result {
                        Ok(raw_scorer) => return Ok(raw_scorer),
                        Err(err) => log::error!("failed to initialize async raw scorer: {err}"),
                    };
                }

                #[cfg(not(target_os = "linux"))]
                log::warn!("async raw scorer is only supported on Linux");
            }

            raw_scorer_impl(query, vs.as_ref(), hc)
        }

        // TODO(byte_storage): Implement async raw scorer for DenseMemmapByte and DenseMemmapHalf
        VectorStorageEnum::DenseMemmapByte(vs) => raw_scorer_byte_impl(query, vs.as_ref(), hc),
        VectorStorageEnum::DenseMemmapHalf(vs) => raw_scorer_half_impl(query, vs.as_ref(), hc),

        VectorStorageEnum::DenseAppendableMemmap(vs) => raw_scorer_impl(query, vs.as_ref(), hc),
        VectorStorageEnum::DenseAppendableMemmapByte(vs) => {
            raw_scorer_byte_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::DenseAppendableMemmapHalf(vs) => {
            raw_scorer_half_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::DenseAppendableInRam(vs) => raw_scorer_impl(query, vs.as_ref(), hc),
        VectorStorageEnum::DenseAppendableInRamByte(vs) => {
            raw_scorer_byte_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::DenseAppendableInRamHalf(vs) => {
            raw_scorer_half_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::SparseSimple(vs) => raw_sparse_scorer_impl(query, vs, hc),
        VectorStorageEnum::SparseMmap(vs) => raw_sparse_scorer_impl(query, vs, hc),
        VectorStorageEnum::MultiDenseSimple(vs) => raw_multi_scorer_impl(query, vs, hc),
        VectorStorageEnum::MultiDenseSimpleByte(vs) => raw_multi_scorer_byte_impl(query, vs, hc),
        VectorStorageEnum::MultiDenseSimpleHalf(vs) => raw_multi_scorer_half_impl(query, vs, hc),
        VectorStorageEnum::MultiDenseAppendableMemmap(vs) => {
            raw_multi_scorer_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapByte(vs) => {
            raw_multi_scorer_byte_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapHalf(vs) => {
            raw_multi_scorer_half_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::MultiDenseAppendableInRam(vs) => {
            raw_multi_scorer_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::MultiDenseAppendableInRamByte(vs) => {
            raw_multi_scorer_byte_impl(query, vs.as_ref(), hc)
        }
        VectorStorageEnum::MultiDenseAppendableInRamHalf(vs) => {
            raw_multi_scorer_half_impl(query, vs.as_ref(), hc)
        }
    }
}

pub static DEFAULT_STOPPED: AtomicBool = AtomicBool::new(false);

pub fn raw_sparse_scorer_impl<'a, TVectorStorage: SparseVectorStorage>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(_vector) => Err(OperationError::service_error(
            "Raw scorer must not be used for nearest queries",
        )),
        QueryVector::RecommendBestScore(reco_query) => {
            let reco_query: RecoQuery<SparseVector> = reco_query.transform_into()?;
            let query_scorer = SparseCustomQueryScorer::<_, _>::new(
                RecoBestScoreQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<SparseVector> = reco_query.transform_into()?;
            let query_scorer = SparseCustomQueryScorer::<_, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<SparseVector> = discovery_query.transform_into()?;
            let query_scorer = SparseCustomQueryScorer::<_, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<SparseVector> = context_query.transform_into()?;
            let query_scorer = SparseCustomQueryScorer::<_, _>::new(
                context_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

#[cfg(feature = "testing")]
pub fn new_raw_scorer_for_test<'a>(
    vector: QueryVector,
    vector_storage: &'a VectorStorageEnum,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    new_raw_scorer(vector, vector_storage, HardwareCounterCell::new())
}

pub fn raw_scorer_impl<'a, TVectorStorage: DenseVectorStorage<VectorElementType>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => {
            new_scorer_with_metric::<CosineMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Euclid => {
            new_scorer_with_metric::<EuclidMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Dot => {
            new_scorer_with_metric::<DotProductMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Manhattan => {
            new_scorer_with_metric::<ManhattanMetric, _>(query, vector_storage, hardware_counter)
        }
    }
}

fn new_scorer_with_metric<
    'a,
    TMetric: Metric<VectorElementType> + 'a,
    TVectorStorage: DenseVectorStorage<VectorElementType>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(vector) => {
            let query_scorer = MetricQueryScorer::<_, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendBestScore(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                RecoBestScoreQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<DenseVector> = discovery_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                context_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

pub fn raw_scorer_byte_impl<'a, TVectorStorage: DenseVectorStorage<VectorElementTypeByte>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => {
            new_scorer_byte_with_metric::<CosineMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Euclid => {
            new_scorer_byte_with_metric::<EuclidMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Dot => new_scorer_byte_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Manhattan => new_scorer_byte_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
    }
}

fn new_scorer_byte_with_metric<
    'a,
    TMetric: Metric<VectorElementTypeByte> + 'a,
    TVectorStorage: DenseVectorStorage<VectorElementTypeByte>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(vector) => {
            let query_scorer = MetricQueryScorer::<_, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendBestScore(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                RecoBestScoreQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<DenseVector> = discovery_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                context_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

pub fn raw_scorer_half_impl<'a, TVectorStorage: DenseVectorStorage<VectorElementTypeHalf>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => {
            new_scorer_half_with_metric::<CosineMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Euclid => {
            new_scorer_half_with_metric::<EuclidMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Dot => new_scorer_half_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Manhattan => new_scorer_half_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
    }
}

fn new_scorer_half_with_metric<
    'a,
    TMetric: Metric<VectorElementTypeHalf> + 'a,
    TVectorStorage: DenseVectorStorage<VectorElementTypeHalf>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter_cell: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(vector) => {
            let query_scorer = MetricQueryScorer::<_, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
                hardware_counter_cell,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendBestScore(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                RecoBestScoreQuery::from(reco_query),
                vector_storage,
                hardware_counter_cell,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter_cell,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<DenseVector> = discovery_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter_cell,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
            let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                context_query,
                vector_storage,
                hardware_counter_cell,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

pub fn raw_scorer_from_query_scorer<'a, TVector, TQueryScorer>(
    query_scorer: TQueryScorer,
) -> OperationResult<Box<dyn RawScorer + 'a>>
where
    TVector: ?Sized + 'a,
    TQueryScorer: QueryScorer<TVector> + 'a,
{
    Ok(Box::new(RawScorerImpl::<TVector, TQueryScorer> {
        query_scorer,
        vector: std::marker::PhantomData,
    }))
}

pub fn raw_multi_scorer_impl<'a, TVectorStorage: MultiVectorStorage<VectorElementType>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => {
            new_multi_scorer_with_metric::<CosineMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Euclid => {
            new_multi_scorer_with_metric::<EuclidMetric, _>(query, vector_storage, hardware_counter)
        }
        Distance::Dot => new_multi_scorer_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Manhattan => new_multi_scorer_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
    }
}

fn new_multi_scorer_with_metric<
    'a,
    TMetric: Metric<VectorElementType> + 'a,
    TVectorStorage: MultiVectorStorage<VectorElementType>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(vector) => {
            let query_scorer = MultiMetricQueryScorer::<_, TMetric, _>::new(
                &vector.try_into()?,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendBestScore(reco_query) => {
            let query_scorer: RecoQuery<MultiDenseVectorInternal> = reco_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                RecoBestScoreQuery::from(query_scorer),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVectorInternal> = reco_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<MultiDenseVectorInternal> =
                discovery_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<MultiDenseVectorInternal> =
                context_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                context_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

pub fn raw_multi_scorer_byte_impl<'a, TVectorStorage: MultiVectorStorage<VectorElementTypeByte>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_multi_scorer_byte_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Euclid => new_multi_scorer_byte_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Dot => new_multi_scorer_byte_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Manhattan => new_multi_scorer_byte_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
    }
}

fn new_multi_scorer_byte_with_metric<
    'a,
    TMetric: Metric<VectorElementTypeByte> + 'a,
    TVectorStorage: MultiVectorStorage<VectorElementTypeByte>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(vector) => {
            let query_scorer = MultiMetricQueryScorer::<_, TMetric, _>::new(
                &vector.try_into()?,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendBestScore(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVectorInternal> = reco_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                RecoBestScoreQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVectorInternal> = reco_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<MultiDenseVectorInternal> =
                discovery_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<MultiDenseVectorInternal> =
                context_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                context_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

pub fn raw_multi_scorer_half_impl<'a, TVectorStorage: MultiVectorStorage<VectorElementTypeHalf>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_multi_scorer_half_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Euclid => new_multi_scorer_half_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Dot => new_multi_scorer_half_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
        Distance::Manhattan => new_multi_scorer_half_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            hardware_counter,
        ),
    }
}

fn new_multi_scorer_half_with_metric<
    'a,
    TMetric: Metric<VectorElementTypeHalf> + 'a,
    TVectorStorage: MultiVectorStorage<VectorElementTypeHalf>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match query {
        QueryVector::Nearest(vector) => {
            let query_scorer = MultiMetricQueryScorer::<_, TMetric, _>::new(
                &vector.try_into()?,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendBestScore(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVectorInternal> = reco_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                RecoBestScoreQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::RecommendSumScores(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVectorInternal> = reco_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                RecoSumScoresQuery::from(reco_query),
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<MultiDenseVectorInternal> =
                discovery_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                discovery_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<MultiDenseVectorInternal> =
                context_query.transform_into()?;
            let query_scorer = MultiCustomQueryScorer::<_, TMetric, _, _>::new(
                context_query,
                vector_storage,
                hardware_counter,
            );
            raw_scorer_from_query_scorer(query_scorer)
        }
    }
}

impl<TVector, TQueryScorer> RawScorer for RawScorerImpl<TVector, TQueryScorer>
where
    TVector: ?Sized,
    TQueryScorer: QueryScorer<TVector>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoreType]) {
        assert_eq!(points.len(), scores.len());

        let (mut remaining_points, mut remaining_scores) = (points, scores);
        while !remaining_points.is_empty() {
            let chunk_size = remaining_points.len().min(VECTOR_READ_BATCH_SIZE);

            let (chunk_points, rest_points) = remaining_points.split_at(chunk_size);
            let (chunk_scores, rest_scores) = remaining_scores.split_at_mut(chunk_size);
            remaining_points = rest_points;
            remaining_scores = rest_scores;

            self.query_scorer
                .score_stored_batch(chunk_points, chunk_scores);
        }
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.query_scorer.score_stored(point)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.query_scorer.score_internal(point_a, point_b)
    }
}

#[inline]
pub fn check_deleted_condition(
    point: PointOffsetType,
    vec_deleted: &BitSlice,
    point_deleted: &BitSlice,
) -> bool {
    // Deleted points propagate to vectors; check vector deletion for possible early return
    // Default to not deleted if our deleted flags failed grow
    !vec_deleted.get_bit(point as usize).unwrap_or(false)
        // Additionally check point deletion for integrity if delete propagation to vector failed
        // Default to deleted if the point mapping was removed from the ID tracker
        && !point_deleted.get_bit(point as usize).unwrap_or(true)
}
