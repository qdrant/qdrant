use std::borrow::Cow;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::UserData;
use quantization::turboquant::EncodedQueryTQ;

use super::ReadOnlyImmutableTurboVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::DenseVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::turbo::shared;
use crate::vector_storage::turbo::turbo_vectors::TurboVectorBlob;
use crate::vector_storage::vector_storage_base::VectorStorageRead;
use crate::vector_storage::{DenseTQVectorStorageRead, TurboScoring};

impl<B: TurboVectorBlob> VectorStorageRead for ReadOnlyImmutableTurboVectorStorage<B> {
    fn size_of_available_vectors_in_bytes(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.available_vector_count(hw_counter) * self.quantized_vector_size()
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        shared::storage_datatype(&self.quantizer)
    }

    fn is_on_disk(&self) -> bool {
        self.on_disk
    }

    fn total_vector_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        self.storage.vectors_count(hw_counter)
    }

    fn get_vector<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> CowVector<'_> {
        shared::dequantize_vector(
            &self.quantizer,
            self.dim,
            &self.storage.get_vector_data(key, hw_counter),
        )
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
        hw_counter: &HardwareCounterCell,
    ) {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage
            .for_each_in_batch(
                &point_offsets,
                |idx, bytes| {
                    let vector = shared::dequantize_vector(&self.quantizer, self.dim, bytes);
                    callback(user_data[idx], point_offsets[idx], vector);
                },
                hw_counter,
            )
            .expect("read TQ vectors");
    }

    fn get_vector_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CowVector<'_>> {
        let bytes = self.storage.get_vector_data_opt(key, hw_counter)?;
        Some(shared::dequantize_vector(&self.quantizer, self.dim, &bytes))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted.count()
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }

    fn read_vector_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, Vec<u8>),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.read_dense_tq_bytes::<P, U>(keys, callback, hw_counter)
    }
}

impl<B: TurboVectorBlob> DenseTQVectorStorageRead for ReadOnlyImmutableTurboVectorStorage<B> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn quantized_vector_size(&self) -> usize {
        self.quantizer.quantized_size()
    }

    fn get_dense_tq<P: AccessPattern>(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]> {
        self.storage.get_vector_data(key, hw_counter)
    }

    fn for_each_in_dense_tq_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.for_each_in_batch(keys, f, hw_counter)
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage.for_each_in_batch(
            &point_offsets,
            |idx, bytes| {
                callback(user_data[idx], point_offsets[idx], bytes.to_vec());
            },
            hw_counter,
        )
    }

    fn get_dense_for_requantization(
        &self,
        key: PointOffsetType,
        keep_rotated: bool,
        hw_counter: &HardwareCounterCell,
    ) -> DenseVector {
        shared::dequantize_for_requantization(
            &self.quantizer,
            self.dim,
            &self.storage.get_vector_data(key, hw_counter),
            keep_rotated,
        )
    }
}

impl<B: TurboVectorBlob> TurboScoring for ReadOnlyImmutableTurboVectorStorage<B> {
    fn preprocess_query(&self, query: DenseVector) -> EncodedQueryTQ {
        shared::preprocess_query(&self.quantizer, self.distance, query)
    }

    fn score_query_bytes(&self, query: &EncodedQueryTQ, bytes: &[u8]) -> ScoreType {
        shared::score_query_bytes(&self.quantizer, self.distance, query, bytes)
    }

    fn score_query_batch(
        &self,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    ) {
        self.storage.score_query_batch(
            &self.quantizer,
            self.distance,
            query,
            ids,
            scores,
            hw_counter,
        );
    }

    fn score_internal_encoded(
        &self,
        point_a: PointOffsetType,
        point_b: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> ScoreType {
        let v1 = self.storage.get_vector_data(point_a, hw_counter);
        let v2 = self.storage.get_vector_data(point_b, hw_counter);
        shared::score_symmetric_bytes(&self.quantizer, self.distance, &v1, &v2)
    }

    fn get_quantized_vector(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]> {
        self.storage.get_vector_data(key, hw_counter)
    }
}
