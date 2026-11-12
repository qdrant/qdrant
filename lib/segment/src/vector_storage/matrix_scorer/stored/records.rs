use common::types::PointOffsetType;
use quantization::encoded_storage::EncodedStorage;
use quantization::encoded_vectors_u8::ScalarOperands;

use super::StoredForm;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::vectors::VectorElementType;
use crate::vector_storage::DenseVectorStorageRead;

/// The stored records of one segment's vectors, read undecoded, see [`super::stored_records`].
pub struct StoredRecords<'a> {
    form: StoredForm,
    source: Box<dyn RecordSource + 'a>,
}

impl<'a> StoredRecords<'a> {
    pub(super) fn new(form: StoredForm, source: Box<dyn RecordSource + 'a>) -> Self {
        Self { form, source }
    }

    /// The layout of every record, and how two of them score.
    pub fn form(&self) -> &StoredForm {
        &self.form
    }

    /// The number of records the storage holds, one per point, deleted points included: their
    /// records exist and ids index them. Every id below it can be read by
    /// [`Self::for_each_record`]. For original vectors, the storage's `total_vector_count`; for
    /// scalar quantization, the records in the quantized storage.
    pub fn len(&self) -> usize {
        self.source.len()
    }

    /// Whether the storage holds no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Calls `f(i, record)` with the stored bytes of `ids[i]`, undecoded. Each record is
    /// [`StoredForm::record_bytes`] long. Reads through the storage's batch reader, as the matrix
    /// operands do. What that leaves to the caller:
    ///
    /// - **Order:** the calls come in the order the reader delivers, which may differ from the
    ///   order of `ids` (io_uring completions, batched graph-inline reads). Place each record by
    ///   the position `i` it comes with.
    /// - **Errors:** on `Ok`, `f` was called once for each position of `ids`. On `Err`, the
    ///   records of the batches read before the failure may have been delivered already.
    /// - **Ids out of range** are the caller's error: some storages panic (the volatile ones),
    ///   others return `Err` (the immutable memory-mapped ones).
    /// - **Stopping:** `f` can neither fail nor stop the read. A caller that wants to stop or
    ///   cancel reads `ids` in chunks and checks between them.
    pub fn for_each_record(
        &self,
        ids: &[PointOffsetType],
        mut f: impl FnMut(usize, &[u8]),
    ) -> OperationResult<()> {
        self.source.for_each_record(ids, &mut f)
    }
}

/// Object-safe reader of one storage's records.
pub(super) trait RecordSource: Sync {
    /// See [`StoredRecords::len`].
    fn len(&self) -> usize;

    fn for_each_record(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[u8]),
    ) -> OperationResult<()>;
}

/// Original f32 vectors, each record the bytes of its vector.
pub(super) struct DenseRecords<'a, S>(pub &'a S);

impl<S> RecordSource for DenseRecords<'_, S>
where
    S: DenseVectorStorageRead<VectorElementType> + Sync,
{
    fn len(&self) -> usize {
        let Self(storage) = self;
        storage.total_vector_count()
    }

    fn for_each_record(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[u8]),
    ) -> OperationResult<()> {
        let Self(storage) = self;
        storage.for_each_in_dense_batch(ids, |i, vector| f(i, bytemuck::cast_slice(vector)))
    }
}

impl<TStorage> RecordSource for ScalarOperands<'_, TStorage>
where
    TStorage: EncodedStorage + Sync,
{
    fn len(&self) -> usize {
        self.record_count()
    }

    fn for_each_record(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[u8]),
    ) -> OperationResult<()> {
        ScalarOperands::for_each_record(self, ids, f).map_err(OperationError::from)
    }
}
