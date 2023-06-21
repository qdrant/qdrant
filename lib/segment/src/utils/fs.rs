use std::path::{Path, PathBuf};
use std::{fmt, fs};

use crate::entry::entry_point::{OperationError, OperationResult};

/// Move all files and directories from the `dir` directory to the `dest_dir` directory.
///
/// - `<dir>/child/directory` will be merged with `<dest-dir>/child/directory` if one already exists
/// - `<dir>/some/file` will overwrite `<dest-dir>/some/file` if one already exists
pub fn move_all(dir: &Path, dest_dir: &Path) -> OperationResult<()> {
    assert_is_dir(dir)?;
    assert_is_dir(dest_dir)?;

    move_all_impl(dir, dir, dest_dir).map_err(|err| failed_to_move_error(dir, dest_dir, err))
}

fn move_all_impl(base: &Path, dir: &Path, dest_dir: &Path) -> OperationResult<()> {
    let entries = dir.read_dir().map_err(|err| {
        if base != dir {
            failed_to_read_dir_error(dir, err)
        } else {
            err.into()
        }
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| {
            if base != dir {
                failed_to_read_dir_error(dir, err)
            } else {
                err.into()
            }
        })?;

        let path = entry.path();

        let name = path
            .file_name()
            .ok_or_else(|| failed_to_move_error(&path, dest_dir, "source path ends with .."))?;

        let dest_path = dest_dir.join(name);

        if path.is_dir() && dest_path.exists() {
            move_all_impl(base, &path, &dest_path)?;
        } else {
            if let Some(dir) = dest_path.parent() {
                if !dir.exists() {
                    fs::create_dir_all(dir).map_err(|err| {
                        failed_to_move_error(
                            &path,
                            &dest_path,
                            format!("failed to create {dir:?} directory: {err}"),
                        )
                    })?;
                }
            }

            fs::rename(&path, &dest_path)
                .map_err(|err| failed_to_move_error(&path, &dest_path, err))?;
        }
    }

    Ok(())
}

fn assert_is_dir(dir: &Path) -> OperationResult<()> {
    if dir.is_dir() {
        Ok(())
    } else {
        Err(not_a_dir_error(dir))
    }
}

fn not_a_dir_error(dir: &Path) -> OperationError {
    OperationError::service_error(format!(
        "path {dir:?} is not a directory (or does not exist)"
    ))
}

fn failed_to_read_dir_error(dir: &Path, err: impl fmt::Display) -> OperationError {
    OperationError::service_error(format!("failed to read {dir:?} directory: {err}"))
}

fn failed_to_move_error(path: &Path, dest: &Path, err: impl fmt::Display) -> OperationError {
    OperationError::service_error(format!("failed to move {path:?} to {dest:?}: {err}"))
}

/// Finds the first symlink in the directory tree and returns its path.
pub fn find_symlink(directory: &Path) -> Option<PathBuf> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(_) => return None,
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let path = entry.path();

        if path.is_dir() {
            if let Some(path) = find_symlink(&path) {
                return Some(path);
            }
        } else if path.is_symlink() {
            return Some(path);
        }
    }

    None
}
