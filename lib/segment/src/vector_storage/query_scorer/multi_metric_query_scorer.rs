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
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
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
    hardware_counter: HardwareCounterCell,
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
    > MultiMetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    pub fn new(query: &MultiDenseVectorInternal, vector_storage: &'a TVectorStorage) -> Self {
        let mut preprocessed = DenseVector::new();
        for slice in query.multi_vectors() {
            preprocessed.extend_from_slice(&TMetric::preprocess(slice.to_vec()));
        }
        let preprocessed = MultiDenseVectorInternal::new(preprocessed, query.dim);
        Self {
            query: TElement::from_float_multivector(CowMultiVector::Owned(preprocessed)).to_owned(),
            vector_storage,
            metric: PhantomData,
            hardware_counter: HardwareCounterCell::new(),
        }
    }

    fn score_multi(
        &self,
        multi_dense_a: TypedMultiDenseVectorRef<TElement>,
        multi_dense_b: TypedMultiDenseVectorRef<TElement>,
    ) -> ScoreType {
        self.hardware_counter
            .cpu_counter()
            // Calculate the amount of comparisons needed for multi vector scoring.
            .incr_delta(multi_dense_a.vectors_count() * multi_dense_b.vectors_count());

        score_multi::<TElement, TMetric>(
            self.vector_storage.multi_vector_config(),
            multi_dense_a,
            multi_dense_b,
        )
    }

    fn score_ref(&self, v2: TypedMultiDenseVectorRef<TElement>) -> ScoreType {
        self.score_multi(TypedMultiDenseVectorRef::from(&self.query), v2)
    }
}

impl<
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
    > MultiMetricQueryScorer<'_, TElement, TMetric, TVectorStorage>
{
    fn hardware_counter_finalized(&self) -> HardwareCounterCell {
        let mut counter = self.hardware_counter.take();

        // Calculate the dimension multiplier here to improve performance of measuring.
        counter
            .cpu_counter_mut()
            .multiplied_mut(self.query.dim * size_of::<TElement>());

        counter
    }
}

impl<
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: MultiVectorStorage<TElement>,
    > QueryScorer<TypedMultiDenseVector<TElement>>
    for MultiMetricQueryScorer<'_, TElement, TMetric, TVectorStorage>
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

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self.vector_storage.get_multi(point_a);
        let v2 = self.vector_storage.get_multi(point_b);
        self.score_multi(v1, v2)
    }

    fn take_hardware_counter(&self) -> HardwareCounterCell {
        self.hardware_counter_finalized()
    }
}
