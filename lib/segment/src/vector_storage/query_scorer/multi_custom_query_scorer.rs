use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use super::score_multi;
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedMultiDenseVector, TypedMultiDenseVectorRef,
};
use crate::spaces::metric::Metric;
use crate::vector_storage::MultiVectorStorage;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;

pub struct MultiCustomQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorage<TElement>,
    TQuery: Query<TypedMultiDenseVector<TElement>>,
    TInputQuery: Query<MultiDenseVectorInternal>,
> {
    vector_storage: &'a TVectorStorage,
    query: TQuery,
    input_query: PhantomData<TInputQuery>,
    metric: PhantomData<TMetric>,
    element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorage<TElement>,
    TQuery: Query<TypedMultiDenseVector<TElement>>,
    TInputQuery: Query<MultiDenseVectorInternal>
        + TransformInto<TQuery, MultiDenseVectorInternal, TypedMultiDenseVector<TElement>>,
> MultiCustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    pub fn new(
        query: TInputQuery,
        vector_storage: &'a TVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        let mut dim = 0;
        let query = query
            .transform(|vector| {
                dim = vector.dim;
                let mut preprocessed = DenseVector::new();
                for slice in vector.multi_vectors() {
                    preprocessed.extend_from_slice(&TMetric::preprocess(slice.to_vec()));
                }
                let preprocessed = MultiDenseVectorInternal::new(preprocessed, vector.dim);
                let converted =
                    TElement::from_float_multivector(CowMultiVector::Owned(preprocessed))
                        .to_owned();
                Ok(converted)
            })
            .unwrap();

        hardware_counter.set_cpu_multiplier(dim * size_of::<TElement>());

        Self {
            query,
            vector_storage,
            input_query: PhantomData,
            metric: PhantomData,
            element: PhantomData,
            hardware_counter,
        }
    }
}

impl<
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorage<TElement>,
    TQuery: Query<TypedMultiDenseVector<TElement>>,
    TInputQuery: Query<MultiDenseVectorInternal>,
> MultiCustomQueryScorer<'_, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    #[inline]
    fn score_ref(&self, against: TypedMultiDenseVectorRef<TElement>) -> ScoreType {
        let cpu_counter = self.hardware_counter.cpu_counter();

        let against_vector_count = against.vectors_count();

        self.query.score_by(|example| {
            cpu_counter.incr_delta(example.vectors_count() * against_vector_count);

            score_multi::<TElement, TMetric>(
                self.vector_storage.multi_vector_config(),
                TypedMultiDenseVectorRef::from(example),
                against,
            )
        })
    }
}

impl<
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorage<TElement>,
    TQuery: Query<TypedMultiDenseVector<TElement>>,
    TInputQuery: Query<MultiDenseVectorInternal>,
> QueryScorer<TypedMultiDenseVector<TElement>>
    for MultiCustomQueryScorer<'_, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_multi(idx);
        self.score_ref(stored)
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert!(ids.len() <= VECTOR_READ_BATCH_SIZE);
        debug_assert_eq!(ids.len(), scores.len());

        let mut vectors = [TypedMultiDenseVectorRef {
            flattened_vectors: &[],
            dim: 0,
        }; VECTOR_READ_BATCH_SIZE];

        self.vector_storage
            .get_batch_multi(ids, &mut vectors[..ids.len()]);
        for idx in 0..ids.len() {
            scores[idx] = self.score_ref(vectors[idx]);
        }
    }

    #[inline]
    fn score(&self, against: &TypedMultiDenseVector<TElement>) -> ScoreType {
        self.score_ref(TypedMultiDenseVectorRef::from(against))
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }
}
