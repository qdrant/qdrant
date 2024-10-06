use std::path::Path;

use common::service_error::Context as _;

use crate::common::operation_error::OperationResult;

pub fn strip_prefix<'a>(path: &'a Path, prefix: &Path) -> OperationResult<&'a Path> {
    Ok(path
        .strip_prefix(prefix)
        .with_context(|| format!("failed to strip {prefix:?} prefix from {path:?}"))?)
}
