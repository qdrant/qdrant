use std::marker::PhantomData;

use common::types::{PointOffsetType, ScoreType};

use super::score_multi;
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVector, TypedMultiDenseVector, TypedMultiDenseVectorRef,
};
use crate::spaces::metric::Metric;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::MultiVectorStorage;

pub struct MultiCustomQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorage<TElement>,
    TQuery: Query<TypedMultiDenseVector<TElement>>,
    TInputQuery: Query<MultiDenseVector>,
> {
    vector_storage: &'a TVectorStorage,
    query: TQuery,
    input_query: PhantomData<TInputQuery>,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
        TQuery: Query<TypedMultiDenseVector<TElement>>,
        TInputQuery: Query<MultiDenseVector>
            + TransformInto<TQuery, MultiDenseVector, TypedMultiDenseVector<TElement>>,
    > MultiCustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    pub fn new(query: TInputQuery, vector_storage: &'a TVectorStorage) -> Self {
        let query = query
            .transform(|vector| {
                let slices = vector.multi_vectors();
                let preprocessed: DenseVector = slices
                    .into_iter()
                    .flat_map(|slice| TMetric::preprocess(slice.to_vec()))
                    .collect();
                let preprocessed = MultiDenseVector::new(preprocessed, vector.dim);
                let converted =
                    TElement::from_float_multivector(CowMultiVector::Owned(preprocessed))
                        .to_owned();
                Ok(converted)
            })
            .unwrap();

        Self {
            query,
            vector_storage,
            input_query: PhantomData,
            metric: PhantomData,
            element: PhantomData,
        }
    }
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
        TQuery: Query<TypedMultiDenseVector<TElement>>,
        TInputQuery: Query<MultiDenseVector>,
    > QueryScorer<TypedMultiDenseVector<TElement>>
    for MultiCustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_multi(idx);
        self.query.score_by(|example| {
            score_multi::<TElement, TMetric>(
                self.vector_storage.multi_vector_config(),
                TypedMultiDenseVectorRef::from(example),
                stored,
            )
        })
    }

    #[inline]
    fn score(&self, against: &TypedMultiDenseVector<TElement>) -> ScoreType {
        self.query.score_by(|example| {
            score_multi::<TElement, TMetric>(
                self.vector_storage.multi_vector_config(),
                TypedMultiDenseVectorRef::from(example),
                TypedMultiDenseVectorRef::from(against),
            )
        })
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }
}
