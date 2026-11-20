use segment::common::operation_error::OperationError;

/// Why a checked apply wrote nothing.
#[derive(Debug)]
pub enum CheckedApplyError<E> {
    /// The check refused a point.
    Refused(E),
    /// The batch failed before its first write.
    NotWritten(OperationError),
}
