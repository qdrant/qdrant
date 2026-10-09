use std::borrow::Cow;
use std::marker::PhantomData;

use common::counter::hw::HwScale;
use common::generic_consts::Random;
use common::typelevel::True;
use common::types::{PointOffsetType, ScoreType};
use zerocopy::FromBytes;

use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedDenseVector, VectorElementType};
use crate::spaces::metric::Metric;
use crate::vector_storage::DenseVectorStorageRead;
use crate::vector_storage::query_scorer::QueryScorer;

pub struct MetricQueryScorer<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorageRead<TElement>,
> {
    hw: HwScale,
    vector_storage: &'a TVectorStorage,
    query: TypedDenseVector<TElement>,
    metric: PhantomData<TMetric>,
}

impl<
    'a,
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorageRead<TElement>,
> MetricQueryScorer<'a, TElement, TMetric, TVectorStorage>
{
    pub fn new(
        query: TypedDenseVector<VectorElementType>,
        vector_storage: &'a TVectorStorage,
    ) -> Self {
        let dim = query.len();
        let preprocessed_vector = TMetric::preprocess(query);

        Self {
            hw: HwScale {
                cpu: dim * size_of::<TElement>(),
                vector_io_read: if vector_storage.is_cold() {
                    dim * size_of::<TElement>()
                } else {
                    0
                },
            },
            query: TypedDenseVector::from(TElement::slice_from_float_cow(Cow::from(
                preprocessed_vector,
            ))),
            vector_storage,
            metric: PhantomData,
        }
    }

    #[inline]
    fn score(&self, v2: &[TElement]) -> ScoreType {
        self.hw.cpu(1);
        TMetric::similarity(&self.query, v2)
    }
}

impl<
    TElement: PrimitiveVectorElement,
    TMetric: Metric<TElement>,
    TVectorStorage: DenseVectorStorageRead<TElement>,
> QueryScorer for MetricQueryScorer<'_, TElement, TMetric, TVectorStorage>
{
    #[inline]
    fn score_stored(&self, idx: PointOffsetType) -> ScoreType {
        self.hw.cpu(1);
        self.hw.vector_io_read(1);
        TMetric::similarity(&self.query, &self.vector_storage.get_dense::<Random>(idx))
    }

    #[inline]
    fn score_stored_batch(
        &self,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) -> OperationResult<()> {
        debug_assert_eq!(ids.len(), scores.len());

        self.hw.cpu(ids.len());
        self.hw.vector_io_read(ids.len());

        self.vector_storage
            .for_each_in_dense_batch(ids, |idx, vector| {
                scores[idx] = TMetric::similarity(&self.query, vector);
            })
    }

    fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.hw.cpu(1);
        let v1 = self.vector_storage.get_dense::<Random>(point_a);
        let v2 = self.vector_storage.get_dense::<Random>(point_b);
        TMetric::similarity(&v1, &v2)
    }

    type SupportsBytes = True;
    fn score_bytes(&self, _enabled: Self::SupportsBytes, bytes: &[u8]) -> ScoreType {
        self.score(<[TElement]>::ref_from_bytes(bytes).unwrap())
    }
}
