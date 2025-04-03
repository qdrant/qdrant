use std::marker::PhantomData;
use std::mem::MaybeUninit;

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
use crate::vector_storage::query_scorer::QueryScorer;

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
    pub fn new(
        query: &MultiDenseVectorInternal,
        vector_storage: &'a TVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        let mut preprocessed = DenseVector::new();
        for slice in query.multi_vectors() {
            preprocessed.extend_from_slice(&TMetric::preprocess(slice.to_vec()));
        }
        let preprocessed = MultiDenseVectorInternal::new(preprocessed, query.dim);

        hardware_counter.set_cpu_multiplier(query.dim * size_of::<TElement>());

        if vector_storage.is_on_disk() {
            hardware_counter.set_vector_io_read_multiplier(query.dim * size_of::<TElement>());
        } else {
            hardware_counter.set_vector_io_read_multiplier(0);
        }

        Self {
            query: TElement::from_float_multivector(CowMultiVector::Owned(preprocessed)).to_owned(),
            vector_storage,
            metric: PhantomData,
            hardware_counter,
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
> QueryScorer<TypedMultiDenseVector<TElement>>
    for MultiMetricQueryScorer<'_, TElement, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_multi(idx);
        self.hardware_counter
            .vector_io_read()
            .incr_delta(stored.vectors_count());

        self.score_multi(TypedMultiDenseVectorRef::from(&self.query), stored)
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

        let mut vectors = [MaybeUninit::uninit(); VECTOR_READ_BATCH_SIZE];
        let vectors = self
            .vector_storage
            .get_batch_multi(ids, &mut vectors[..ids.len()]);

        let total_read = vectors.iter().map(|v| v.vectors_count()).sum();

        self.hardware_counter
            .vector_io_read()
            .incr_delta(total_read);

        for idx in 0..ids.len() {
            scores[idx] = self.score_ref(vectors[idx]);
        }
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self.vector_storage.get_multi(point_a);
        let v2 = self.vector_storage.get_multi(point_b);
        self.hardware_counter
            .vector_io_read()
            .incr_delta(v1.vectors_count() + v2.vectors_count());

        self.score_multi(v1, v2)
    }
}
