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
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::MultiVectorStorage;

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
    dimension: usize,
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
    pub fn new(query: TInputQuery, vector_storage: &'a TVectorStorage) -> Self {
        let mut dim = 0;
        let query = query
            .transform(|vector| {
                dim = vector.dim;
                let slices = vector.multi_vectors();
                let preprocessed: DenseVector = slices
                    .into_iter()
                    .flat_map(|slice| TMetric::preprocess(slice.to_vec()))
                    .collect();
                let preprocessed = MultiDenseVectorInternal::new(preprocessed, vector.dim);
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
            dimension: dim,
            hardware_counter: HardwareCounterCell::new(),
        }
    }
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
        TQuery: Query<TypedMultiDenseVector<TElement>>,
        TInputQuery: Query<MultiDenseVectorInternal>,
    > MultiCustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    fn hardware_counter_finalized(&self) -> HardwareCounterCell {
        let mut counter = self.hardware_counter.clone();

        // Calculate the dimension multiplier here to improve performance of measuring.
        counter
            .cpu_counter_mut()
            .multiplied_mut(self.dimension * size_of::<TElement>());

        counter
    }
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
        TQuery: Query<TypedMultiDenseVector<TElement>>,
        TInputQuery: Query<MultiDenseVectorInternal>,
    > QueryScorer<TypedMultiDenseVector<TElement>>
    for MultiCustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TQuery, TInputQuery>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_multi(idx);
        let cpu_counter = self.hardware_counter.cpu_counter();

        let stored_vector_count = stored.vectors_count();
        self.query.score_by(|example| {
            cpu_counter.incr_delta(example.vectors_count() * stored_vector_count);

            score_multi::<TElement, TMetric>(
                self.vector_storage.multi_vector_config(),
                TypedMultiDenseVectorRef::from(example),
                stored,
            )
        })
    }

    #[inline]
    fn score(&self, against: &TypedMultiDenseVector<TElement>) -> ScoreType {
        let cpu_counter = self.hardware_counter.cpu_counter();

        let against_vector_count = against.vectors_count();

        self.query.score_by(|example| {
            cpu_counter.incr_delta(example.vectors_count() * against_vector_count);

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

    fn hardware_counter(&self) -> HardwareCounterCell {
        self.hardware_counter_finalized()
    }
}
