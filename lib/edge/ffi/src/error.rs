use segment::common::operation_error::OperationError;
use segment::json_path::JsonPath;

/// The error type returned from fallible `EdgeShard` operations and
/// `UpdateOperation` constructors.
///
/// Three branchable variants let Swift/Kotlin hosts pattern-match on the error
/// category:
///
/// - `ShardClosed` — the shard has been unloaded; reopen it via `load`.
/// - `InvalidArgument` — host-supplied input was invalid; fix it and retry.
/// - `OperationError` — any other engine failure (I/O, missing index, etc.).
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
/// } catch EdgeError.shardClosed {
///     print("Shard is closed — reopen it first")
/// } catch let EdgeError.invalidArgument(reason) {
///     print("Bad input: \(reason)")
/// } catch let error as EdgeError {
///     print("Engine error: \(error)")
/// }
/// ```
///
/// ```kotlin
/// try {
///     val shard = EdgeShard.load(path = dataDir, config = null)
/// } catch (e: EdgeException.ShardClosed) {
///     println("Shard is closed — reopen it first")
/// } catch (e: EdgeException.InvalidArgument) {
///     println("Bad input: ${e.reason}")
/// } catch (e: EdgeException.OperationException) {
///     println("Engine error: ${e.reason}")
/// }
/// ```
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum EdgeError {
    /// The shard has been unloaded/disposed; reopen it via `load` to continue.
    #[error("shard is closed")]
    ShardClosed,

    /// Host-supplied input was invalid (bad UUID, out-of-range coordinate,
    /// malformed payload key/JSON, contradictory match filter, …). Fix the
    /// input and retry; retrying unchanged will fail again.
    #[error("invalid argument: {reason}")]
    InvalidArgument { reason: String },

    /// Any other failure from the engine (I/O, missing index, dimension
    /// mismatch, resource exhaustion, internal error, …).
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
    /// Build an `InvalidArgument` from a borrowed message — used by fallible
    /// FFI-boundary conversions for invalid host input (bad UUID, out-of-range
    /// coordinate, malformed payload key).
    pub(crate) fn invalid_argument(reason: impl Into<String>) -> Self {
        EdgeError::InvalidArgument {
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

/// Clamp a host-supplied u64 count to usize, saturating instead of truncating.
/// On 64-bit this is a no-op; on 32-bit it avoids silent wrap. Callers that
/// want a hard upper bound should validate before calling.
pub(crate) fn clamp_usize(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

pub type Result<T, E = EdgeError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::clamp_usize;

    #[test]
    fn clamp_usize_small_value() {
        assert_eq!(clamp_usize(5), 5);
    }

    #[test]
    fn clamp_usize_max_saturates() {
        assert_eq!(clamp_usize(u64::MAX), usize::MAX);
    }
}
