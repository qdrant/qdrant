use std::marker::PhantomData;

use common::counter::hw::HwScale;
use common::generic_consts::Random;
use common::typelevel::False;
use common::types::{PointOffsetType, ScoreType};

use super::score_multi;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowMultiVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    DenseVector, MultiDenseVectorInternal, TypedMultiDenseVector, TypedMultiDenseVectorRef,
};
use crate::spaces::metric::Metric;
use crate::vector_storage::MultiVectorStorageRead;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct MultiMetricQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorageRead<TElement>,
> {
    hw: HwScale,
    vector_storage: &'a TVectorStorage,
    query: TypedMultiDenseVector<TElement>,
    metric: PhantomData<TMetric>,
}

impl<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: MultiVectorStorageRead<TElement>,
> MultiMetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    pub fn new(query: &MultiDenseVectorInternal, vector_storage: &'a TVectorStorage) -> Self {
        let mut preprocessed = DenseVector::new();
        for slice in query.multi_vectors() {
            preprocessed.extend_from_slice(&TMetric::preprocess(slice.to_vec()));
        }
        let preprocessed = MultiDenseVectorInternal::new(preprocessed, query.dim);

        Self {
            hw: HwScale {
                cpu: query.dim * size_of::<TElement>(),
                vector_io_read: if vector_storage.is_cold() {
                    query.dim * size_of::<TElement>()
                } else {
                    0
                },
            },
            query: TElement::from_float_multivector(CowMultiVector::Owned(preprocessed)).to_owned(),
            vector_storage,
            metric: PhantomData,
        }
    }

    fn score_multi(
        &self,
        multi_dense_a: TypedMultiDenseVectorRef<TElement>,
        multi_dense_b: TypedMultiDenseVectorRef<TElement>,
    ) -> ScoreType {
        // Calculate the amount of comparisons needed for multi vector scoring.
        self.hw
            .cpu(multi_dense_a.vectors_count() * multi_dense_b.vectors_count());

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
    TVectorStorage: MultiVectorStorageRead<TElement>,
> QueryScorer for MultiMetricQueryScorer<'_, TElement, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        let stored = self.vector_storage.get_multi::<Random>(idx);
        self.hw.vector_io_read(stored.as_ref().vectors_count());

        self.score_multi(TypedMultiDenseVectorRef::from(&self.query), stored.as_ref())
    }

    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.vector_storage
            .for_each_in_batch_multi(ids, |idx, vector| {
                self.hw.vector_io_read(vector.vectors_count());
                scores[idx] = self.score_ref(vector);
            })
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let v1 = self.vector_storage.get_multi::<Random>(point_a);
        let v2 = self.vector_storage.get_multi::<Random>(point_b);
        self.hw
            .vector_io_read(v1.as_ref().vectors_count() + v2.as_ref().vectors_count());

        self.score_multi(v1.as_ref(), v2.as_ref())
    }

    type SupportsBytes = False;
    fn score_bytes(&self, enabled: Self::SupportsBytes, _: &[u8]) -> ScoreType {
        match enabled {}
    }
}
