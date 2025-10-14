use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use super::query::{
    ContextQuery, DiscoveryQuery, RecoBestScoreQuery, RecoQuery, RecoSumScoresQuery, TransformInto,
};
use super::query_scorer::custom_query_scorer::CustomQueryScorer;
use super::query_scorer::{QueryScorerBytes, QueryScorerBytesImpl};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{DenseVector, QueryVector, VectorElementType, VectorInternal};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::Distance;
use crate::vector_storage::dense::memmap_dense_vector_storage::MemmapDenseVectorStorage;
use crate::vector_storage::dense::mmap_dense_vectors::MmapDenseVectors;
use crate::vector_storage::query::FeedbackQueryInternal;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::query_scorer::metric_query_scorer::MetricQueryScorer;
use crate::vector_storage::{RawScorer, VectorStorage as _};

pub fn new<'a>(
    query: QueryVector,
    storage: &'a MemmapDenseVectorStorage<VectorElementType>,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    AsyncRawScorerBuilder::new(query, storage, hardware_counter).build()
}

pub struct AsyncRawScorerImpl<'a, TQueryScorer: QueryScorer<TVector = [VectorElementType]>> {
    query_scorer: TQueryScorer,
    storage: &'a MmapDenseVectors<VectorElementType>,
}

impl<'a, TQueryScorer> AsyncRawScorerImpl<'a, TQueryScorer>
where
    TQueryScorer: QueryScorer<TVector = [VectorElementType]>,
{
    fn new(query_scorer: TQueryScorer, storage: &'a MmapDenseVectors<VectorElementType>) -> Self {
        Self {
            query_scorer,
            storage,
        }
    }
}

impl<TQueryScorer> RawScorer for AsyncRawScorerImpl<'_, TQueryScorer>
where
    TQueryScorer: QueryScorer<TVector = [VectorElementType]>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoreType]) {
        assert_eq!(points.len(), scores.len());
        let points_stream = points.iter().copied();

        self.storage
            .read_vectors_async(points_stream, |idx, _point_id, other_vector| {
                scores[idx] = self.query_scorer.score(other_vector);
            })
            .unwrap();

        // ToDo: io_uring is experimental, it can fail if it is not supported.
        // Instead of silently falling back to the sync implementation, we prefer to panic
        // and notify the user that they better use the default IO implementation.
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.query_scorer.score_stored(point)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.query_scorer.score_internal(point_a, point_b)
    }

    fn scorer_bytes(&self) -> Option<&dyn QueryScorerBytes> {
        QueryScorerBytesImpl::new(&self.query_scorer).map(|s| s as _)
    }
}

struct AsyncRawScorerBuilder<'a> {
    query: QueryVector,
    storage: &'a MemmapDenseVectorStorage<VectorElementType>,
    distance: Distance,
    hardware_counter: HardwareCounterCell,
}

impl<'a> AsyncRawScorerBuilder<'a> {
    pub fn new(
        query: QueryVector,
        storage: &'a MemmapDenseVectorStorage<VectorElementType>,
        hardware_counter: HardwareCounterCell,
    ) -> Self {
        Self {
            query,
            storage,
            distance: storage.distance(),
            hardware_counter,
        }
    }

    pub fn build(self) -> OperationResult<Box<dyn RawScorer + 'a>> {
        match self.distance {
            Distance::Cosine => self._build_with_metric::<CosineMetric>(),
            Distance::Euclid => self._build_with_metric::<EuclidMetric>(),
            Distance::Dot => self._build_with_metric::<DotProductMetric>(),
            Distance::Manhattan => self._build_with_metric::<ManhattanMetric>(),
        }
    }

    fn _build_with_metric<TMetric: Metric<VectorElementType> + 'a>(
        self,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        let Self {
            query,
            storage,
            distance: _,
            hardware_counter,
        } = self;

        match query {
            QueryVector::Nearest(vector) => {
                match vector {
                    VectorInternal::Dense(dense_vector) => {
                        let query_scorer = MetricQueryScorer::<_, TMetric, _>::new(
                            dense_vector,
                            storage,
                            hardware_counter,
                        );
                        Ok(async_raw_scorer_from_query_scorer(query_scorer, storage))
                    }
                    VectorInternal::Sparse(_sparse_vector) => Err(OperationError::service_error(
                        "sparse vectors are not supported for async scorer",
                    )), // TODO(sparse) add support?
                    VectorInternal::MultiDense(_multi_dense_vector) => {
                        Err(OperationError::service_error(
                            "multi-dense vectors are not supported for async scorer",
                        ))
                    } // TODO(colbert) add support?
                }
            }
            QueryVector::RecommendBestScore(reco_query) => {
                let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
                let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                    RecoBestScoreQuery::from(reco_query),
                    storage,
                    hardware_counter,
                );
                Ok(async_raw_scorer_from_query_scorer(query_scorer, storage))
            }
            QueryVector::RecommendSumScores(reco_query) => {
                let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
                let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                    RecoSumScoresQuery::from(reco_query),
                    storage,
                    hardware_counter,
                );
                Ok(async_raw_scorer_from_query_scorer(query_scorer, storage))
            }
            QueryVector::Discovery(discovery_query) => {
                let discovery_query: DiscoveryQuery<DenseVector> =
                    discovery_query.transform_into()?;
                let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                    discovery_query,
                    storage,
                    hardware_counter,
                );
                Ok(async_raw_scorer_from_query_scorer(query_scorer, storage))
            }
            QueryVector::Context(context_query) => {
                let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
                let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                    context_query,
                    storage,
                    hardware_counter,
                );
                Ok(async_raw_scorer_from_query_scorer(query_scorer, storage))
            }
            QueryVector::FeedbackSimple(feedback_query) => {
                let feedback_query: FeedbackQueryInternal<DenseVector, _> =
                    feedback_query.transform_into()?;
                let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                    feedback_query.into_query(),
                    storage,
                    hardware_counter,
                );
                Ok(async_raw_scorer_from_query_scorer(query_scorer, storage))
            }
        }
    }
}

fn async_raw_scorer_from_query_scorer<'a, TQueryScorer>(
    query_scorer: TQueryScorer,
    storage: &'a MemmapDenseVectorStorage<VectorElementType>,
) -> Box<dyn RawScorer + 'a>
where
    TQueryScorer: QueryScorer<TVector = [VectorElementType]> + 'a,
{
    Box::new(AsyncRawScorerImpl::new(
        query_scorer,
        storage.get_mmap_vectors(),
    ))
}
