use std::fs::File;
use std::path::Path;

use tar::Archive;

use crate::common::operation_error::{OperationError, OperationResult};

pub fn open_snapshot_archive_with_validation<P: AsRef<Path>>(
    snapshot_path: P,
) -> OperationResult<Archive<File>> {
    let path = snapshot_path.as_ref();
    {
        let archive_file = File::open(path).map_err(|err| {
            OperationError::service_error(format!(
                "failed to open segment snapshot archive {path:?}: {err}"
            ))
        })?;
        let mut ar = Archive::new(archive_file);

        for entry in ar.entries_with_seek()? {
            let entry_type = entry?.header().entry_type();
            if !matches!(
                entry_type,
                tar::EntryType::Regular | tar::EntryType::Directory,
            ) {
                return Err(OperationError::ValidationError {
                    description: format!(
                        "Malformed snapshot, tar archive contains {entry_type:?} entry",
                    ),
                });
            }
        }
    }

    let archive_file = File::open(path).map_err(|err| {
        OperationError::service_error(format!(
            "failed to open segment snapshot archive {path:?}: {err}"
        ))
    })?;

    let mut ar = Archive::new(archive_file);
    ar.set_overwrite(false);

    Ok(ar)
}
