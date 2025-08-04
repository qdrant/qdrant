use std::path::Path;
use std::{fs, io};

use crate::common::operation_error::{OperationError, OperationResult};

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
    ar.set_sync(true);

    Ok(ar)
}
