use std::borrow::Cow;
use std::path::PathBuf;

use common::mmap::MmapFlusher;
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

    fn vectors_count(&self) -> usize;

    fn get_vector_data(&self, key: PointOffsetType) -> Cow<'_, [u8]>;

    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()>;

    fn populate(&self);

    fn clear_cache(&self) -> OperationResult<()>;

    fn get_vector_data_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>>;

    fn files(&self) -> Vec<PathBuf>;

    fn flusher(&self) -> MmapFlusher;

    fn score_query_batch(
        &self,
        quantizer: &TurboQuantizer,
        distance: Distance,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    );
}

impl<S: UniversalRead> TurboVectorBlob for QuantizedStorage<S> {
    type File = S;

    fn vectors_count(&self) -> usize {
        EncodedStorageWrite::vectors_count(self)
    }

    fn get_vector_data(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        EncodedStorage::get_vector_data(self, key)
    }

    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        f: F,
    ) -> OperationResult<()> {
        QuantizedStorage::for_each_in_batch(self, keys, f)
    }

    fn populate(&self) {
        QuantizedStorage::populate(self);
    }

    fn clear_cache(&self) -> OperationResult<()> {
        QuantizedStorage::clear_cache(self);
        Ok(())
    }

    fn get_vector_data_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        EncodedStorage::get_vector_data_opt(self, key)
    }

    fn files(&self) -> Vec<PathBuf> {
        EncodedStorage::files(self)
    }

    fn flusher(&self) -> MmapFlusher {
        EncodedStorageWrite::flusher(self)
    }

    fn score_query_batch(
        &self,
        quantizer: &TurboQuantizer,
        distance: Distance,
        query: &EncodedQueryTQ,
        ids: &[PointOffsetType],
        scores: &mut [ScoreType],
    ) {
        shared::score_query_batch(self, quantizer, distance, query, ids, scores);
    }
}
