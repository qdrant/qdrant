use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::UniversalRead;

use super::query::{
    ContextQuery, DiscoverQuery, RecoBestScoreQuery, RecoQuery, RecoSumScoresQuery, TransformInto,
};
use super::query_scorer::custom_query_scorer::CustomQueryScorer;
use super::query_scorer::{QueryScorerBytes, QueryScorerBytesImpl};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, QueryVector, VectorInternal};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::Distance;
use crate::vector_storage::dense::dense_vector_storage::DenseVectorStorageImpl;
use crate::vector_storage::dense::immutable_dense_vectors::ImmutableDenseVectors;
use crate::vector_storage::query::NaiveFeedbackQuery;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::query_scorer::metric_query_scorer::MetricQueryScorer;
use crate::vector_storage::{RawScorer, VectorStorage as _};

pub fn new<'a, T, Storage>(
    query: QueryVector,
    storage: &'a DenseVectorStorageImpl<T, Storage>,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Box<dyn RawScorer + 'a>>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
    CosineMetric: Metric<T>,
    EuclidMetric: Metric<T>,
    DotProductMetric: Metric<T>,
    ManhattanMetric: Metric<T>,
{
    AsyncRawScorerBuilder::new(query, storage, hardware_counter).build()
}

pub struct AsyncRawScorerImpl<'a, T, Storage, Scorer>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
    Scorer: QueryScorer<TVector = [T]>,
{
    query_scorer: Scorer,
    storage: &'a ImmutableDenseVectors<T, Storage>,
}

impl<'a, T, Storage, Scorer> AsyncRawScorerImpl<'a, T, Storage, Scorer>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
    Scorer: QueryScorer<TVector = [T]>,
{
    fn new(query_scorer: Scorer, storage: &'a ImmutableDenseVectors<T, Storage>) -> Self {
        Self {
            query_scorer,
            storage,
        }
    }
}

impl<'a, T, Storage, Scorer> RawScorer for AsyncRawScorerImpl<'a, T, Storage, Scorer>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
    Scorer: QueryScorer<TVector = [T]>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoreType]) {
        assert_eq!(points.len(), scores.len());

        self.storage
            .read_vectors_async::<Random>(points, |idx, _point_id, other_vector| {
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

struct AsyncRawScorerBuilder<'a, T, Storage>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
{
    query: QueryVector,
    storage: &'a DenseVectorStorageImpl<T, Storage>,
    distance: Distance,
    hardware_counter: HardwareCounterCell,
}

impl<'a, T, Storage> AsyncRawScorerBuilder<'a, T, Storage>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
{
    pub fn new(
        query: QueryVector,
        storage: &'a DenseVectorStorageImpl<T, Storage>,
        hardware_counter: HardwareCounterCell,
    ) -> Self {
        Self {
            query,
            storage,
            distance: storage.distance(),
            hardware_counter,
        }
    }

    pub fn build(self) -> OperationResult<Box<dyn RawScorer + 'a>>
    where
        CosineMetric: Metric<T>,
        EuclidMetric: Metric<T>,
        DotProductMetric: Metric<T>,
        ManhattanMetric: Metric<T>,
    {
        match self.distance {
            Distance::Cosine => self._build_with_metric::<CosineMetric>(),
            Distance::Euclid => self._build_with_metric::<EuclidMetric>(),
            Distance::Dot => self._build_with_metric::<DotProductMetric>(),
            Distance::Manhattan => self._build_with_metric::<ManhattanMetric>(),
        }
    }

    fn _build_with_metric<TMetric: Metric<T> + 'a>(
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
            QueryVector::Discover(discover_query) => {
                let discover_query: DiscoverQuery<DenseVector> = discover_query.transform_into()?;
                let query_scorer = CustomQueryScorer::<_, TMetric, _, _>::new(
                    discover_query,
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
            QueryVector::FeedbackNaive(feedback_query) => {
                let feedback_query: NaiveFeedbackQuery<DenseVector> =
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

fn async_raw_scorer_from_query_scorer<'a, T, Storage, Scorer>(
    query_scorer: Scorer,
    storage: &'a DenseVectorStorageImpl<T, Storage>,
) -> Box<dyn RawScorer + 'a>
where
    T: PrimitiveVectorElement,
    Storage: UniversalRead<T>,
    Scorer: QueryScorer<TVector = [T]> + 'a,
{
    Box::new(AsyncRawScorerImpl::new(
        query_scorer,
        storage.get_mmap_vectors(),
    ))
}
