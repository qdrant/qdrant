use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedDenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::DenseVectorStorage;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct MetricQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
> {
    vector_storage: &'a TVectorStorage,
    query: TypedDenseVector<TElement>,
    metric: PhantomData<TMetric>,
    hardware_counter: HardwareCounterCell,
}

impl<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
> MetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    pub fn new(
        query: TypedDenseVector<VectorElementType>,
        vector_storage: &'a TVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self {
        let dim = query.len();
        let preprocessed_vector = TMetric::preprocess(query);

        hardware_counter.set_cpu_multiplier(dim * size_of::<TElement>());
        if vector_storage.is_on_disk() {
            hardware_counter.set_vector_io_read_multiplier(dim * size_of::<TElement>());
        } else {
            hardware_counter.set_vector_io_read_multiplier(0);
        }

        Self {
            query: TypedDenseVector::from(TElement::slice_from_float_cow(Cow::from(
                preprocessed_vector,
            ))),
            vector_storage,
            metric: PhantomData,
            hardware_counter,
        }
    }
}

impl<
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
> QueryScorer<[TElement]> for MetricQueryScorer<'_, TElement, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.hardware_counter.cpu_counter().incr();
        self.hardware_counter.vector_io_read().incr();
        TMetric::similarity(&self.query, self.vector_storage.get_dense(idx))
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert!(ids.len() <= VECTOR_READ_BATCH_SIZE);
        debug_assert_eq!(ids.len(), scores.len());

        let mut vectors = [MaybeUninit::uninit(); VECTOR_READ_BATCH_SIZE];

        let vectors = self
            .vector_storage
            .get_dense_batch(ids, &mut vectors[..ids.len()]);
        self.hardware_counter.cpu_counter().incr_delta(ids.len());
        self.hardware_counter.vector_io_read().incr_delta(ids.len());

        for idx in 0..ids.len() {
            scores[idx] = TMetric::similarity(&self.query, vectors[idx]);
        }
    }

    #[inline]
    fn score(&self, v2: &[TElement]) -> ScoreType {
        self.hardware_counter.cpu_counter().incr();
        TMetric::similarity(&self.query, v2)
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.hardware_counter.cpu_counter().incr();
        let v1 = self.vector_storage.get_dense(point_a);
        let v2 = self.vector_storage.get_dense(point_b);
        TMetric::similarity(v1, v2)
    }
}
