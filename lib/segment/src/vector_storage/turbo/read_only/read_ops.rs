use std::borrow::Cow;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::{UniversalRead, UserData};
use quantization::turboquant::EncodedQueryTQ;
use quantization::{EncodedStorage, EncodedStorageWrite};

use super::super::shared;
use super::ReadOnlyChunkedTurboVectorStorage;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::DenseVector;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::vector_storage_base::VectorStorageRead;
use crate::vector_storage::{DenseTQVectorStorageRead, TurboScoring};

impl<S: UniversalRead> VectorStorageRead for ReadOnlyChunkedTurboVectorStorage<S> {
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
        self.storage.is_on_disk()
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

        self.storage.for_each_batch(
            &point_offsets,
            |idx, bytes| {
                let vector = shared::dequantize_vector(&self.quantizer, self.dim, &bytes);
                callback(user_data[idx], point_offsets[idx], vector);
            },
            hw_counter,
        );
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

impl<S: UniversalRead> DenseTQVectorStorageRead for ReadOnlyChunkedTurboVectorStorage<S> {
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
        mut f: F,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage
            .for_each_batch(keys, |idx, bytes| f(idx, &bytes), hw_counter);
        Ok(())
    }

    fn read_dense_tq_bytes<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, Vec<u8>),
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let (user_data, point_offsets): (Vec<U>, Vec<PointOffsetType>) = keys.into_iter().unzip();

        self.storage.for_each_batch(
            &point_offsets,
            |idx, bytes| {
                callback(user_data[idx], point_offsets[idx], bytes.to_vec());
            },
            hw_counter,
        );
        Ok(())
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

impl<S: UniversalRead> TurboScoring for ReadOnlyChunkedTurboVectorStorage<S> {
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
        shared::score_query_batch(
            &self.storage,
            &self.quantizer,
            self.distance,
            query,
            ids,
            scores,
            hw_counter,
        )
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
