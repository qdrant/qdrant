use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedDenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::DenseVectorStorage;

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
    dim: usize,
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
    ) -> Self {
        let dim = query.len();
        let preprocessed_vector = TMetric::preprocess(query);
        Self {
            query: TypedDenseVector::from(TElement::slice_from_float_cow(Cow::from(
                preprocessed_vector,
            ))),
            vector_storage,
            metric: PhantomData,
            hardware_counter: HardwareCounterCell::new(),
            dim,
        }
    }

    fn hardware_counter_finalized(&self) -> HardwareCounterCell {
        let mut counter = self.hardware_counter.take();

        // Calculate the dimension multiplier here to improve performance of measuring.
        counter
            .cpu_counter_mut()
            .multiplied_mut(self.dim * size_of::<TElement>());

        counter
    }
}

impl<
        'a,
        TElement: PrimitiveVectorElement,
        TMetric: Metric<TElement>,
        TVectorStorage: DenseVectorStorage<TElement>,
    > QueryScorer<[TElement]> for MetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.hardware_counter.cpu_counter().incr();
        TMetric::similarity(&self.query, self.vector_storage.get_dense(idx))
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType]) -> Vec<ScoreType> {
        let consecutive_ids = ids.windows(2).all(|w| w[0] + 1 == w[1]);
        if consecutive_ids {
            // TODO optimize storage for consecutive ids
            ids.iter().map(|&id| self.score_stored(id)).collect()
        } else {
            ids.iter().map(|&id| self.score_stored(id)).collect()
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

    fn take_hardware_counter(&self) -> HardwareCounterCell {
        self.hardware_counter_finalized()
    }
}
