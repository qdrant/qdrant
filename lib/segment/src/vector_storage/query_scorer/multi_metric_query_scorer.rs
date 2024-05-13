use std::borrow::Cow;
use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use super::score_multi;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVector, TypedMultiDenseVector, TypedMultiDenseVectorRef,
};
use crate::spaces::metric::Metric;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::MultiVectorStorage;

pub struct MultiMetricQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorage<TElement>,
> {
    vector_storage: &'a TVectorStorage,
    query: TypedMultiDenseVector<TElement>,
    metric: PhantomData<TMetric>,
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
    > MultiMetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    pub fn new(query: MultiDenseVector, vector_storage: &'a TVectorStorage) -> Self {
        let slices = query.multi_vectors();
        let preprocessed: DenseVector = slices
            .into_iter()
            .flat_map(|slice| TMetric::preprocess(slice.to_vec()))
            .collect();
        let preprocessed = MultiDenseVector::new(preprocessed, query.dim);
        Self {
            query: TElement::from_float_multivector(Cow::Owned(preprocessed)).into_owned(),
            vector_storage,
            metric: PhantomData,
        }
    }

    fn score_multi(
        &self,
        multi_dense_a: TypedMultiDenseVectorRef<TElement>,
        multi_dense_b: TypedMultiDenseVectorRef<TElement>,
    ) -> ScoreType {
        score_multi::<TElement, TMetric>(
            self.vector_storage.multi_vector_config(),
            multi_dense_a,
            multi_dense_b,
        )
    }
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
    > QueryScorer<TypedMultiDenseVector<TElement>>
    for MultiMetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.score_multi(
            TypedMultiDenseVectorRef::from(&self.query),
            self.vector_storage.get_multi(idx),
        )
    }

    #[inline]
    fn score(&self, v2: &TypedMultiDenseVector<TElement>) -> ScoreType {
        self.score_multi(
            TypedMultiDenseVectorRef::from(&self.query),
            TypedMultiDenseVectorRef::from(v2),
        )
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self.vector_storage.get_multi(point_a);
        let v2 = self.vector_storage.get_multi(point_b);
        self.score_multi(v1, v2)
    }
}
