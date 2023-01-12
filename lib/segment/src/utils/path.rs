use std::path::Path;

use crate::entry::entry_point::{OperationError, OperationResult};

pub fn strip_prefix<'a>(path: &'a Path, prefix: &Path) -> OperationResult<&'a Path> {
    path.strip_prefix(prefix).map_err(|err| {
        OperationError::service_error(format!(
            "failed to strip {prefix:?} prefix from {path:?}: {err}"
        ))
    })
}
