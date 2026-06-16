use std::path::{Path, PathBuf};

pub const DELETED_FILE_NAME: &str = "id_tracker.deleted";

pub(crate) fn deleted_path(base: &Path) -> PathBuf {
    base.join(DELETED_FILE_NAME)
}
