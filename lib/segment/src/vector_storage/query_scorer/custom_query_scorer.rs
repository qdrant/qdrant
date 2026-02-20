use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hardware_counter::HardwareCounterCell;
use common::typelevel::True;
use common::types::{PointOffsetType, ScoreType};
use zerocopy::FromBytes;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{DenseVector, TypedDenseVector};
use crate::spaces::metric::Metric;
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::query::{Query, TransformInto};
use crate::vector_storage::query_scorer::QueryScorer;
use crate::vector_storage::{DenseVectorStorage, Random};

pub struct CustomQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
    TStoredQuery: Query<TypedDenseVector<TElement>>,
> {
    vector_storage: &'a TVectorStorage,
    query: TStoredQuery,
    metric: PhantomData<TMetric>,
    _element: PhantomData<TElement>,
    hardware_counter: HardwareCounterCell,
}

impl<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
    TStoredQuery: Query<TypedDenseVector<TElement>>,
> CustomQueryScorer<'a, TElement, TMetric, TVectorStorage, TStoredQuery>
{
    pub fn new<TInputQuery>(
        query: TInputQuery,
        vector_storage: &'a TVectorStorage,
        mut hardware_counter: HardwareCounterCell,
    ) -> Self
    where
        TInputQuery: Query<DenseVector>
            + TransformInto<TStoredQuery, DenseVector, TypedDenseVector<TElement>>,
    {
        let mut dim = 0;
        let query = query
            .transform(|vector| {
                dim = vector.len();
                let preprocessed_vector = TMetric::preprocess(vector);
                Ok(TypedDenseVector::from(TElement::slice_from_float_cow(
                    Cow::from(preprocessed_vector),
                )))
            })
            .unwrap();

        hardware_counter.set_cpu_multiplier(dim * size_of::<TElement>());
        if vector_storage.is_on_disk() {
            hardware_counter.set_vector_io_read_multiplier(dim * size_of::<TElement>());
        } else {
            hardware_counter.set_vector_io_read_multiplier(0);
        }

        Self {
            query,
            vector_storage,
            metric: PhantomData,
            _element: PhantomData,
            hardware_counter,
        }
    }
}

impl<
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorage<TElement>,
    TStoredQuery: Query<TypedDenseVector<TElement>>,
> QueryScorer for CustomQueryScorer<'_, TElement, TMetric, TVectorStorage, TStoredQuery>
{
    type TVector = [TElement];

    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_dense::<Random>(idx);
        self.hardware_counter.vector_io_read().incr();

        self.score(&stored)
    }

    fn score_stored_batch(&self, ids: &[PointOffsetType], scores: &mut [ScoreType]) {
        debug_assert!(ids.len() <= VECTOR_READ_BATCH_SIZE);
        debug_assert_eq!(ids.len(), scores.len());

        self.hardware_counter.vector_io_read().incr_delta(ids.len());

        self.vector_storage
            .for_each_in_dense_batch(ids, |idx, vector| scores[idx] = self.score(vector));
    }

    #[inline]
    fn score(&self, against: &[TElement]) -> ScoreType {
        let cpu_counter = self.hardware_counter.cpu_counter();

        self.query.score_by(|example| {
            cpu_counter.incr();
            TMetric::similarity(example, against)
        })
    }

    fn score_internal(&self, _point_a: PointOffsetType, _point_b: PointOffsetType) -> ScoreType {
        unimplemented!("Custom scorer can compare against multiple vectors, not just one")
    }

    type SupportsBytes = True;
    fn score_bytes(&self, _enabled: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        self.score(<[TElement]>::ref_from_bytes(bytes).unwrap())
    }
}
