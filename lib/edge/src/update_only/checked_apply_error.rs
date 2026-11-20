//! Why [`UpdateOnlyEdgeShard::apply_batch_checked`] wrote nothing.
//!
//! [`UpdateOnlyEdgeShard::apply_batch_checked`]: crate::UpdateOnlyEdgeShard::apply_batch_checked

use segment::common::operation_error::OperationError;

/// A checked batch that stopped before its first write. It comes back next
/// to the writer, which still describes the durable state and can apply the
/// next batch.
#[derive(Debug)]
pub enum CheckedApplyError<E> {
    /// The caller's check refused a resolved point.
    Refused(E),
    /// The batch failed before anything was written: an unsupported
    /// operation, a vector update of a point no segment holds, a failed read,
    /// a metering error.
    NotWritten(OperationError),
}
