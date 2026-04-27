use segment::common::operation_error::OperationError;

/// The error type returned from fallible `EdgeShard` operations and
/// `UpdateOperation` constructors.
///
/// All error variants carry a human-readable `message` describing the cause.
/// On the Swift side this surfaces as a throwing error; on the Kotlin side as
/// a checked exception.
///
/// ## Example
///
/// ```swift
/// do {
///     let shard = try EdgeShard.load(path: dataDir, config: nil)
/// } catch let error as EdgeError {
///     print("Failed to load shard: \(error)")
/// }
/// ```
///
/// ```kotlin
/// try {
///     val shard = EdgeShard.load(path = dataDir, config = null)
/// } catch (e: EdgeException.OperationException) {
///     println("Failed to load shard: ${e.message}")
/// }
/// ```
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum EdgeError {
    /// A recoverable operation failure (I/O error, invalid input, missing
    /// configuration, corrupted segment, etc.).
    #[error("{message}")]
    OperationError { message: String },
}

impl From<OperationError> for EdgeError {
    fn from(err: OperationError) -> Self {
        EdgeError::OperationError {
            message: err.to_string(),
        }
    }
}

pub type Result<T, E = EdgeError> = std::result::Result<T, E>;
