use std::io;
use std::path::Path;

use fs_err as fs;

use crate::common::operation_error::{OperationError, OperationResult};

/// Validates an unpacked snapshot directory, ensuring it contains only regular files and directories.
///
/// Similar to [`validate_snapshot_archive`], but works on already unpacked directories.
/// Rejects symlinks and other special file types.
pub fn validate_unpacked_snapshot(path: &Path) -> OperationResult<()> {
    if !path.is_dir() {
        return Err(OperationError::validation_error(format!(
            "snapshot path {} is not a directory",
            path.display(),
        )));
    }

    validate_unpacked_snapshot_recursive(path, path)
}

fn validate_unpacked_snapshot_recursive(root: &Path, current: &Path) -> OperationResult<()> {
    let entries = fs::read_dir(current).map_err(|err| {
        OperationError::service_error(format!(
            "failed to read snapshot directory {}: {err}",
            current.display()
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| {
            log::error!(
                "Failed to read snapshot directory entry in {}: {err}",
                current.display()
            );

            OperationError::service_error(format!(
                "failed to read snapshot directory {}",
                current.display(),
            ))
        })?;

        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| {
            OperationError::service_error(format!(
                "failed to get file type for {}: {err}",
                path.display()
            ))
        })?;

        if file_type.is_dir() {
            validate_unpacked_snapshot_recursive(root, &path)?;
        } else if file_type.is_file() {
            // Regular file, OK
        } else if file_type.is_symlink() {
            return Err(OperationError::validation_error(format!(
                "malformed snapshot directory {}: contains symlink at {}",
                root.display(),
                path.display(),
            )));
        } else {
            // Other special file types (block device, char device, fifo, socket)
            return Err(OperationError::validation_error(format!(
                "malformed snapshot directory {}: contains special file at {}",
                root.display(),
                path.display(),
            )));
        }
    }

    Ok(())
}

pub fn open_snapshot_archive_with_validation(
    path: &Path,
) -> OperationResult<tar::Archive<impl io::Read + io::Seek>> {
    validate_snapshot_archive(path)?;
    open_snapshot_archive(path)
}

pub fn validate_snapshot_archive(path: &Path) -> OperationResult<()> {
    let mut ar = open_snapshot_archive(path)?;

    let entries = ar.entries_with_seek().map_err(|err| {
        OperationError::service_error(format!(
            "failed to read snapshot archive {}: {err}",
            path.display()
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|err| {
            log::error!("Failed to read snapshot archive {}: {err}", path.display());

            // Deliberately mask underlying error from API users, because it can expose arbitrary file contents
            OperationError::service_error(format!(
                "failed to read snapshot archive {}",
                path.display(),
            ))
        })?;

        match entry.header().entry_type() {
            tar::EntryType::Directory | tar::EntryType::Regular | tar::EntryType::GNUSparse => (),
            entry_type => {
                return Err(OperationError::validation_error(format!(
                    "malformed snapshot archive {}: archive contains {entry_type:?} entry",
                    path.display(),
                )));
            }
        }
    }

    Ok(())
}

pub fn open_snapshot_archive(
    path: &Path,
) -> OperationResult<tar::Archive<impl io::Read + io::Seek>> {
    let file = fs::File::open(path).map_err(|err| {
        OperationError::service_error(format!(
            "failed to open snapshot archive {}: {err}",
            path.display()
        ))
    })?;

    let mut ar = tar::Archive::new(io::BufReader::new(file));
    ar.set_overwrite(false);

    Ok(ar)
}
