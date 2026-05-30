use std::fs::File;
use std::path::Path;

use segment::common::validate_snapshot_archive::open_snapshot_archive_with_validation;
use tar::Archive;

use crate::operations::types::CollectionResult;

pub fn validate_open_snapshot_archive<P: AsRef<Path>>(
    archive_path: P,
) -> CollectionResult<Archive<File>> {
    Ok(open_snapshot_archive_with_validation(archive_path)?)
}
