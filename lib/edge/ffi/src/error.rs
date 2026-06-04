use segment::common::operation_error::OperationError;
use segment::json_path::JsonPath;

/// The error type returned from fallible `EdgeShard` operations and
/// `UpdateOperation` constructors.
///
/// All error variants carry a human-readable `reason` describing the cause.
/// On the Swift side this surfaces as a throwing error; on the Kotlin side as
/// a checked exception.
///
/// NOTE: the field is named `reason`, not `message`, on purpose. UniFFI
/// generates the Kotlin error as a subclass of `kotlin.Exception` (→ `Throwable`),
/// which already declares a `message` property; a variant field literally named
/// `message` collides with it ("hides member of supertype 'Throwable'") and
/// fails to compile. `reason` sidesteps that.
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
///     println("Failed to load shard: ${e.reason}")
/// }
/// ```
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum EdgeError {
    /// A recoverable operation failure (I/O error, invalid input, missing
    /// configuration, corrupted segment, etc.).
    #[error("{reason}")]
    OperationError { reason: String },
}

impl From<OperationError> for EdgeError {
    fn from(err: OperationError) -> Self {
        EdgeError::OperationError {
            reason: err.to_string(),
        }
    }
}

impl EdgeError {
    /// Build an `OperationError` from a borrowed message — used by fallible
    /// FFI-boundary conversions for invalid host input (bad UUID, out-of-range
    /// coordinate, malformed payload key).
    pub(crate) fn invalid_argument(reason: impl Into<String>) -> Self {
        EdgeError::OperationError {
            reason: reason.into(),
        }
    }
}

/// Parse a payload key into a `JsonPath`, returning a typed error for invalid keys.
///
/// Centralizes the (identical) parse-or-EdgeError logic used across
/// filter/query/update. `JsonPath`'s `FromStr::Err` is `()`, so the upstream
/// error carries no additional information; the formatted message is as
/// informative as possible.
pub(crate) fn parse_json_path(key: &str) -> Result<JsonPath, EdgeError> {
    key.parse().map_err(|_: ()| {
        EdgeError::invalid_argument(format!(
            "invalid payload key {key:?}: not a valid JSON path"
        ))
    })
}

pub type Result<T, E = EdgeError> = std::result::Result<T, E>;
