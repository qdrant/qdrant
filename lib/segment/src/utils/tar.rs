use std::path::Path;
use std::{fmt, io};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::utils;

/// Append `file` to the archive under `dest_dir` directory at `file`'s path relative to `base`.
///
/// E.g.:
/// - if `base` is `/some/directory/`
/// - `file` is `/some/directory/file/path`
/// - and `dest_dir` is `/inside/the/archive/`
/// - then the `file` will be added to the archive at path `/inside/the/archive/file/path`
pub fn append_file_relative_to_base(
    builder: &mut tar::Builder<impl io::Write>,
    base: &Path,
    file: &Path,
    dest_dir: &Path,
) -> OperationResult<()> {
    let name =
        utils::path::strip_prefix(file, base).map_err(|err| failed_to_append_error(file, err))?;

    append_file(builder, file, &dest_dir.join(name))
}

pub fn append_file(
    builder: &mut tar::Builder<impl io::Write>,
    file: &Path,
    dest: &Path,
) -> OperationResult<()> {
    builder
        .append_path_with_name(file, dest)
        .map_err(|err| failed_to_append_error(file, err))
}

/// Create "failed to append `<path>` to the archive" error.
pub fn failed_to_append_error(path: &Path, err: impl fmt::Display) -> OperationError {
    OperationError::service_error(format!(
        "failed to append {path:?} path to the archive: {err}"
    ))
}
