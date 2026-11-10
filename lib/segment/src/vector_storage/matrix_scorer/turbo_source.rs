use common::types::PointOffsetType;
use quantization::turboquant::matrix::TurboSource;

use crate::common::operation_error::OperationError;
use crate::vector_storage::DenseTQVectorStorageRead;

/// The encoded vectors of a TurboQuant datatype storage, for `TurboMatrixOperands`.
pub(super) struct TurboStorageSource<'a, S>(pub &'a S);

impl<S: DenseTQVectorStorageRead + Sync> TurboSource for TurboStorageSource<'_, S> {
    type Error = OperationError;

    fn for_each_batch(
        &self,
        ids: &[PointOffsetType],
        f: impl FnMut(usize, &[u8]),
    ) -> Result<(), OperationError> {
        self.0.for_each_in_dense_tq_batch(ids, f)
    }
}
