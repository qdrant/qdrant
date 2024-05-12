use std::path::Path;

use io::file_operations::{FileOperationResult, FileStorageError};
use io::storage_version::StorageVersion;

pub struct SparseVectorIndexVersion;

impl StorageVersion for SparseVectorIndexVersion {
    fn current_raw() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

pub fn migrate(path: &Path) -> FileOperationResult<()> {
    let app_version = SparseVectorIndexVersion::current();
    match SparseVectorIndexVersion::load(path)? {
        Some(version) if version == app_version => (),
        Some(version) => {
            return Err(FileStorageError::generic(format!(
                "Version mismatch: expected {app_version}, found {version}"
            )))
        }
        None => migrate_from_v1(path)?,
    }
    Ok(())
}

fn migrate_from_v1(path: &Path) -> FileOperationResult<()> {
    // Disable migration for now.
    SparseVectorIndexVersion::save(path)?;

    /*
    log::info!("Migrating {path:?}");

    let index_v1 = InvertedIndexMmap::index_file_path_v1(path);
    let index_v2 = InvertedIndexMmap::index_file_path(path);

    // 1. Migrate index to a new file.
    AtomicFile::new(index_v2, OverwriteBehavior::AllowOverwrite)
        .write(|f| std::io::copy(&mut File::open(&index_v1)?, f))?;

    // 2. Save version info. Having this file signals that migration is complete.
    SparseVectorIndexVersion::save(path)?;

    // 3. Remove old index file.
    std::fs::remove_file(&index_v1)?;
    */

    Ok(())
}
