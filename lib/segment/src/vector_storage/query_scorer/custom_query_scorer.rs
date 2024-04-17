use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, TypedDenseVector};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::DenseVectorStorage;

pub struct CustomQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
    TInputQuery: Query<DenseVector>,
    TStoredQuery: Query<TypedDenseVector<TElement>>,
> {
    vector_storage: &'a TVectorStorage,
    query: TStoredQuery,
    metric: PhantomData<TMetric>,
    _input_query: PhantomData<TInputQuery>,
    _element: PhantomData<TElement>,
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: DenseVectorStorage<TElement>,
        TInputQuery: Query<DenseVector> + TransformInto<TStoredQuery, DenseVector, TypedDenseVector<TElement>>,
        TStoredQuery: Query<TypedDenseVector<TElement>>,
    > CustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TInputQuery, TStoredQuery>
{
    pub fn new(query: TInputQuery, vector_storage: &'a TVectorStorage) -> Self {
        let query = query
            .transform(|vector| {
                let preprocessed_vector = TMetric::preprocess(vector);
                Ok(TElement::slice_from_float_cow(&preprocessed_vector).to_vec())
            })
            .unwrap();

        Self {
            query,
            vector_storage,
            metric: PhantomData,
            _input_query: PhantomData,
            _element: PhantomData,
        }
    }
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: DenseVectorStorage<TElement>,
        TInputQuery: Query<DenseVector>,
        TStoredQuery: Query<TypedDenseVector<TElement>>,
    > QueryScorer<[TElement]>
    for CustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TInputQuery, TStoredQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_dense(idx);
        self.score(stored)
    }

    #[inline]
    fn score(&self, against: &[TElement]) -> ScoreType {
        self.query
            .score_by(|example| TMetric::similarity(example, against))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }
}
