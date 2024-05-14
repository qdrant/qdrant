use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::prelude::BitSlice;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use sparse::common::sparse_vector::SparseVector;

use super::query::context_query::ContextQuery;
use super::query::discovery_query::DiscoveryQuery;
use super::query::reco_query::RecoQuery;
use super::query::TransformInto;
use super::query_scorer::custom_query_scorer::CustomQueryScorer;
use super::query_scorer::multi_custom_query_scorer::MultiCustomQueryScorer;
use super::query_scorer::sparse_custom_query_scorer::SparseCustomQueryScorer;
use super::{DenseVectorStorage, MultiVectorStorage, SparseVectorStorage, VectorStorageEnum};
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::{
    DenseVector, MultiDenseVector, QueryVector, VectorElementType, VectorElementTypeByte,
    VectorElementTypeHalf,
};
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::spaces::tools::peek_top_largest_iterable;
use crate::types::Distance;
use crate::vector_storage::query_scorer::metric_query_scorer::MetricQueryScorer;
use crate::vector_storage::query_scorer::multi_metric_query_scorer::MultiMetricQueryScorer;
use crate::vector_storage::query_scorer::QueryScorer;

/// RawScorer composition:
///                                              Metric
///                                             ┌───────────────────┐
///                                             │  - Cosine         │
///   RawScorer            QueryScorer          │  - Dot            │
///  ┌────────────────┐   ┌──────────────┐  ┌───┤  - Euclidean      │
///  │                │   │              │  │   │                   │
///  │       ┌─────┐  │   │    ┌─────┐   │  │   └───────────────────┘
///  │       │     │◄─┼───┤    │     │◄──┼──┘   - Vector Distance
///  │       └─────┘  │   │    └─────┘   │
///  │                │   │              │
///  └────────────────┘   │    ┌─────┐   │        Query
///  - Deletions          │    │     │◄──┼───┐   ┌───────────────────┐
///  - Stopping           │    └─────┘   │   │   │  - RecoQuery      │
///  - Access patterns    │              │   │   │  - DiscoveryQuery │
///                       └──────────────┘   └───┤  - ContextQuery   │
///                       - Query holding        │                   │
///                       - Vector storage       └───────────────────┘
///                                              - Scoring logic
///                                              - Complex queries
///
/// Optimized scorer for multiple scoring requests comparing with a single query
/// Holds current query and params, receives only subset of points to score
pub trait RawScorer {
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize;

    /// Score points without excluding deleted and filtered points
    ///
    /// # Arguments
    ///
    /// * `points` - points to score
    ///
    /// # Returns
    ///
    /// Vector of scored points
    fn score_points_unfiltered(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<ScoredPointOffset>;

    /// Return true if vector satisfies current search context for given point (exists and not deleted)
    fn check_vector(&self, point: PointOffsetType) -> bool;

    /// Score stored vector with vector under the given index
    fn score_point(&self, point: PointOffsetType) -> ScoreType;

    /// Return distance between stored points selected by IDs
    ///
    /// # Panics
    ///
    /// Panics if any id is out of range
    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType;

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset>;

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset>;
}

pub struct RawScorerImpl<'a, TVector: ?Sized, TQueryScorer>
where
    TQueryScorer: QueryScorer<TVector>,
{
    pub query_scorer: TQueryScorer,
    /// Point deleted flags should be explicitly present as `false`
    /// for each existing point in the segment.
    /// If there are no flags for some points, they are considered deleted.
    /// [`BitSlice`] defining flags for deleted points (and thus these vectors).
    pub point_deleted: &'a BitSlice,
    /// [`BitSlice`] defining flags for deleted vectors in this segment.
    pub vec_deleted: &'a BitSlice,
    /// This flag indicates that the search process is stopped externally,
    /// the search result is no longer needed and the search process should be stopped as soon as possible.
    pub is_stopped: &'a AtomicBool,

    vector: std::marker::PhantomData<*const TVector>,
}

pub fn new_stoppable_raw_scorer<'a>(
    query: QueryVector,
    vector_storage: &'a VectorStorageEnum,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage {
        VectorStorageEnum::DenseSimple(vs) => raw_scorer_impl(query, vs, point_deleted, is_stopped),
        VectorStorageEnum::DenseSimpleByte(vs) => {
            raw_scorer_byte_impl(query, vs, point_deleted, is_stopped)
        }
        VectorStorageEnum::DenseSimpleHalf(vs) => {
            raw_scorer_half_impl(query, vs, point_deleted, is_stopped)
        }

        VectorStorageEnum::DenseMemmap(vs) => {
            if vs.has_async_reader() {
                #[cfg(target_os = "linux")]
                {
                    let scorer_result =
                        super::async_raw_scorer::new(query.clone(), vs, point_deleted, is_stopped);
                    match scorer_result {
                        Ok(raw_scorer) => return Ok(raw_scorer),
                        Err(err) => log::error!("failed to initialize async raw scorer: {err}"),
                    };
                }

                #[cfg(not(target_os = "linux"))]
                log::warn!("async raw scorer is only supported on Linux");
            }

            raw_scorer_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }

        // TODO(byte_storage): Implement async raw scorer for DenseMemmapByte and DenseMemmapHalf
        VectorStorageEnum::DenseMemmapByte(vs) => {
            raw_scorer_byte_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
        VectorStorageEnum::DenseMemmapHalf(vs) => {
            raw_scorer_half_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }

        VectorStorageEnum::DenseAppendableMemmap(vs) => {
            raw_scorer_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
        VectorStorageEnum::DenseAppendableMemmapByte(vs) => {
            raw_scorer_byte_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
        VectorStorageEnum::DenseAppendableMemmapHalf(vs) => {
            raw_scorer_half_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
        VectorStorageEnum::SparseSimple(vs) => {
            raw_sparse_scorer_impl(query, vs, point_deleted, is_stopped)
        }
        VectorStorageEnum::MultiDenseSimple(vs) => {
            raw_multi_scorer_impl(query, vs, point_deleted, is_stopped)
        }
        VectorStorageEnum::MultiDenseSimpleByte(vs) => {
            raw_multi_scorer_byte_impl(query, vs, point_deleted, is_stopped)
        }
        VectorStorageEnum::MultiDenseSimpleHalf(vs) => {
            raw_multi_scorer_half_impl(query, vs, point_deleted, is_stopped)
        }
        VectorStorageEnum::MultiDenseAppendableMemmap(vs) => {
            raw_multi_scorer_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapByte(vs) => {
            raw_multi_scorer_byte_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
        VectorStorageEnum::MultiDenseAppendableMemmapHalf(vs) => {
            raw_multi_scorer_half_impl(query, vs.as_ref(), point_deleted, is_stopped)
        }
    }
}

pub static DEFAULT_STOPPED: AtomicBool = AtomicBool::new(false);

pub fn raw_sparse_scorer_impl<'a, TVectorStorage: SparseVectorStorage>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(_vector) => Err(OperationError::service_error(
            "Raw scorer must not be used for nearest queries",
        )),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<SparseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                SparseCustomQueryScorer::<_, _>::new(reco_query, vector_storage),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<SparseVector> = discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                SparseCustomQueryScorer::<_, _>::new(discovery_query, vector_storage),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<SparseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                SparseCustomQueryScorer::<_, _>::new(context_query, vector_storage),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

pub fn new_raw_scorer<'a>(
    vector: QueryVector,
    vector_storage: &'a VectorStorageEnum,
    point_deleted: &'a BitSlice,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    new_stoppable_raw_scorer(vector, vector_storage, point_deleted, &DEFAULT_STOPPED)
}

pub fn raw_scorer_impl<'a, TVectorStorage: DenseVectorStorage<VectorElementType>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_scorer_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_scorer_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_scorer_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Manhattan => new_scorer_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
    }
}

fn new_scorer_with_metric<
    'a,
    TMetric: Metric<VectorElementType> + 'a,
    TVectorStorage: DenseVectorStorage<VectorElementType>,
>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MetricQueryScorer::<VectorElementType, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
            ),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementType, TMetric, _, _, _>::new(
                    reco_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<DenseVector> = discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementType, TMetric, _, _, _>::new(
                    discovery_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementType, TMetric, _, _, _>::new(
                    context_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

pub fn raw_scorer_byte_impl<'a, TVectorStorage: DenseVectorStorage<VectorElementTypeByte>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_scorer_byte_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_scorer_byte_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_scorer_byte_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Manhattan => new_scorer_byte_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
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
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MetricQueryScorer::<VectorElementTypeByte, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
            ),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementTypeByte, TMetric, _, _, _>::new(
                    reco_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<DenseVector> = discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementTypeByte, TMetric, _, _, _>::new(
                    discovery_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementTypeByte, TMetric, _, _, _>::new(
                    context_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

pub fn raw_scorer_half_impl<'a, TVectorStorage: DenseVectorStorage<VectorElementTypeHalf>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_scorer_half_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_scorer_half_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_scorer_half_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Manhattan => new_scorer_half_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
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
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MetricQueryScorer::<VectorElementTypeHalf, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
            ),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<DenseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementTypeHalf, TMetric, _, _, _>::new(
                    reco_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<DenseVector> = discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementTypeHalf, TMetric, _, _, _>::new(
                    discovery_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<DenseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                CustomQueryScorer::<VectorElementTypeHalf, TMetric, _, _, _>::new(
                    context_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

pub fn raw_scorer_from_query_scorer<'a, TVector, TQueryScorer>(
    query_scorer: TQueryScorer,
    point_deleted: &'a BitSlice,
    vec_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>>
where
    TVector: ?Sized + 'a,
    TQueryScorer: QueryScorer<TVector> + 'a,
{
    Ok(Box::new(RawScorerImpl::<TVector, TQueryScorer> {
        query_scorer,
        point_deleted,
        vec_deleted,
        is_stopped,
        vector: std::marker::PhantomData,
    }))
}

pub fn raw_multi_scorer_impl<'a, TVectorStorage: MultiVectorStorage<VectorElementType>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_multi_scorer_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_multi_scorer_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_multi_scorer_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Manhattan => new_multi_scorer_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
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
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MultiMetricQueryScorer::<VectorElementType, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
            ),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementType, TMetric, _, _, _>::new(
                    reco_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<MultiDenseVector> =
                discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementType, TMetric, _, _, _>::new(
                    discovery_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<MultiDenseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementType, TMetric, _, _, _>::new(
                    context_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

pub fn raw_multi_scorer_byte_impl<'a, TVectorStorage: MultiVectorStorage<VectorElementTypeByte>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_multi_scorer_byte_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_multi_scorer_byte_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_multi_scorer_byte_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Manhattan => new_multi_scorer_byte_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
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
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MultiMetricQueryScorer::<VectorElementTypeByte, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
            ),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementTypeByte, TMetric, _, _, _>::new(
                    reco_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<MultiDenseVector> =
                discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementTypeByte, TMetric, _, _, _>::new(
                    discovery_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<MultiDenseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementTypeByte, TMetric, _, _, _>::new(
                    context_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

pub fn raw_multi_scorer_half_impl<'a, TVectorStorage: MultiVectorStorage<VectorElementTypeHalf>>(
    query: QueryVector,
    vector_storage: &'a TVectorStorage,
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    match vector_storage.distance() {
        Distance::Cosine => new_multi_scorer_half_with_metric::<CosineMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Euclid => new_multi_scorer_half_with_metric::<EuclidMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Dot => new_multi_scorer_half_with_metric::<DotProductMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
        ),
        Distance::Manhattan => new_multi_scorer_half_with_metric::<ManhattanMetric, _>(
            query,
            vector_storage,
            point_deleted,
            is_stopped,
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
    point_deleted: &'a BitSlice,
    is_stopped: &'a AtomicBool,
) -> OperationResult<Box<dyn RawScorer + 'a>> {
    let vec_deleted = vector_storage.deleted_vector_bitslice();
    match query {
        QueryVector::Nearest(vector) => raw_scorer_from_query_scorer(
            MultiMetricQueryScorer::<VectorElementTypeHalf, TMetric, _>::new(
                vector.try_into()?,
                vector_storage,
            ),
            point_deleted,
            vec_deleted,
            is_stopped,
        ),
        QueryVector::Recommend(reco_query) => {
            let reco_query: RecoQuery<MultiDenseVector> = reco_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementTypeHalf, TMetric, _, _, _>::new(
                    reco_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Discovery(discovery_query) => {
            let discovery_query: DiscoveryQuery<MultiDenseVector> =
                discovery_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementTypeHalf, TMetric, _, _, _>::new(
                    discovery_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
        QueryVector::Context(context_query) => {
            let context_query: ContextQuery<MultiDenseVector> = context_query.transform_into()?;
            raw_scorer_from_query_scorer(
                MultiCustomQueryScorer::<VectorElementTypeHalf, TMetric, _, _, _>::new(
                    context_query,
                    vector_storage,
                ),
                point_deleted,
                vec_deleted,
                is_stopped,
            )
        }
    }
}

impl<'a, TVector, TQueryScorer> RawScorer for RawScorerImpl<'a, TVector, TQueryScorer>
where
    TVector: ?Sized,
    TQueryScorer: QueryScorer<TVector>,
{
    fn score_points(&self, points: &[PointOffsetType], scores: &mut [ScoredPointOffset]) -> usize {
        if self.is_stopped.load(Ordering::Relaxed) {
            return 0;
        }
        let mut size: usize = 0;
        for point_id in points.iter().copied() {
            if !self.check_vector(point_id) {
                continue;
            }
            scores[size] = ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            };

            size += 1;
            if size == scores.len() {
                return size;
            }
        }
        size
    }

    fn score_points_unfiltered(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
    ) -> Vec<ScoredPointOffset> {
        if self.is_stopped.load(Ordering::Relaxed) {
            return vec![];
        }
        let mut scores = vec![];
        for point_id in points {
            scores.push(ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            });
        }
        scores
    }

    fn check_vector(&self, point: PointOffsetType) -> bool {
        check_deleted_condition(point, self.vec_deleted, self.point_deleted)
    }

    fn score_point(&self, point: PointOffsetType) -> ScoreType {
        self.query_scorer.score_stored(point)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.query_scorer.score_internal(point_a, point_b)
    }

    fn peek_top_iter(
        &self,
        points: &mut dyn Iterator<Item = PointOffsetType>,
        top: usize,
    ) -> Vec<ScoredPointOffset> {
        let scores = points
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| ScoredPointOffset {
                idx: point_id,
                score: self.query_scorer.score_stored(point_id),
            });
        peek_top_largest_iterable(scores, top)
    }

    fn peek_top_all(&self, top: usize) -> Vec<ScoredPointOffset> {
        let scores = (0..self.point_deleted.len() as PointOffsetType)
            .take_while(|_| !self.is_stopped.load(Ordering::Relaxed))
            .filter(|point_id| self.check_vector(*point_id))
            .map(|point_id| {
                let point_id = point_id as PointOffsetType;
                ScoredPointOffset {
                    idx: point_id,
                    score: self.query_scorer.score_stored(point_id),
                }
            });
        peek_top_largest_iterable(scores, top)
    }
}

#[inline]
pub fn check_deleted_condition(
    point: PointOffsetType,
    vec_deleted: &BitSlice,
    point_deleted: &BitSlice,
) -> bool {
    // Deleted points propagate to vectors; check vector deletion for possible early return
    !vec_deleted
            .get(point as usize)
            .map(|x| *x)
            // Default to not deleted if our deleted flags failed grow
            .unwrap_or(false)
        // Additionally check point deletion for integrity if delete propagation to vector failed
        && !point_deleted
            .get(point as usize)
            .map(|x| *x)
            // Default to deleted if the point mapping was removed from the ID tracker
            .unwrap_or(true)
}
