use std::borrow::Cow;
use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::mmap::Flusher;
use common::types::{PointOffsetType, ScoreType};
use common::universal_io::UniversalRead;
use quantization::turboquant::EncodedQueryTQ;
use quantization::turboquant::quantization::TurboQuantizer;
use quantization::{EncodedStorage, EncodedStorageWrite};

use super::shared;
use crate::common::operation_error::OperationResult;
use crate::types::Distance;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

pub trait TurboVectorBlob {
    /// Backend the encoded bytes are read through.
    type File: UniversalRead;

    fn vectors_count(&self, hw_counter: &HardwareCounterCell) -> usize;

    fn get_vector_data(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]>;

    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()>;

    fn populate(&self, hw_counter: &HardwareCounterCell);

    fn clear_cache(&self) -> OperationResult<()>;

    fn get_vector_data_opt(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Cow<'_, [u8]>>;

    fn files(&self) -> Vec<PathBuf>;

    fn flusher(&self) -> Flusher;

    fn score_query_batch(
        &self,
        quantizer: &TurboQuantizer,
        distance: Distance,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    );
}

impl<S: UniversalRead> TurboVectorBlob for QuantizedStorage<S> {
    type File = S;

    fn vectors_count(&self, hw_counter: &HardwareCounterCell) -> usize {
        EncodedStorageWrite::vectors_count(self, hw_counter)
    }

    fn get_vector_data(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Cow<'_, [u8]> {
        EncodedStorage::get_vector_data(self, key, hw_counter)
    }

    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        QuantizedStorage::for_each_in_batch(self, keys, f, hw_counter)
    }

    fn populate(&self, hw_counter: &HardwareCounterCell) {
        QuantizedStorage::populate(self, hw_counter);
    }

    fn clear_cache(&self) -> OperationResult<()> {
        QuantizedStorage::clear_cache(self);
        Ok(())
    }

    fn get_vector_data_opt(
        &self,
        key: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> Option<Cow<'_, [u8]>> {
        EncodedStorage::get_vector_data_opt(self, key, hw_counter)
    }

    fn files(&self) -> Vec<PathBuf> {
        EncodedStorage::files(self)
    }

    fn flusher(&self) -> Flusher {
        EncodedStorageWrite::flusher(self)
    }

    fn score_query_batch(
        &self,
        quantizer: &TurboQuantizer,
        distance: Distance,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
        hw_counter: &HardwareCounterCell,
    ) {
        shared::score_query_batch(self, quantizer, distance, query, ids, scores, hw_counter);
    }
}
