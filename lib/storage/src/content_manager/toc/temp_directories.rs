use std::path::{Path, PathBuf};

use collection::operations::types::{CollectionError, CollectionResult};
use tempfile::TempDir;

use crate::content_manager::toc::TableOfContent;

const TEMP_SUBDIR_NAME: &str = "tmp";
const FILE_UPLOAD_SUBDIR_NAME: &str = "upload";

/// Functions for managing temporary storages of TOC.
///
/// The directory structure is as follows:
///
/// ./snapshots
///           └── tmp
///               └── (tempdirs)
/// ./optional_temp_path (if specified)
///           └── tmp
///               └── upload
///               └── (tempdirs)
/// ./storage
///           └── tmp
///               └── (tempdirs)
///
/// optional_temp_path can be used instead of `snapshots/tmp` or `storage/tmp`
/// to speed up processing.
///
/// Assume all temp directories are located on different filesystems, so
/// the choice between them should be made from the performance considerations.
///
/// Subdirectories are required for simpler cleanup on the start of the process.
impl TableOfContent {
    pub fn temp_path(&self) -> Option<&str> {
        self.storage_config.temp_path.as_deref()
    }

    fn get_snapshots_temp_path(&self) -> PathBuf {
        self.snapshot_manager.temp_path()
    }

    fn get_storage_temp_path(&self) -> PathBuf {
        Path::new(self.storage_path()).join(TEMP_SUBDIR_NAME)
    }

    fn get_optional_temp_path(&self) -> Option<PathBuf> {
        self.temp_path()
            .map(|path| Path::new(path).join(TEMP_SUBDIR_NAME))
    }

    /// Get temporary storage path inside the `snapshots` directory.
    pub fn snapshots_temp_path(&self) -> CollectionResult<PathBuf> {
        let path = self.get_snapshots_temp_path();

        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| {
                CollectionError::service_error(format!(
                    "Failed to create snapshots temp directory at {}: {:?}",
                    path.display(),
                    e,
                ))
            })?;
        }
        Ok(path)
    }

    /// Get temporary storage path inside the `storage` directory.
    pub fn storage_temp_path(&self) -> CollectionResult<PathBuf> {
        let path = self.get_storage_temp_path();

        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| {
                CollectionError::service_error(format!(
                    "Failed to create storage temp directory at {}: {:?}",
                    path.display(),
                    e,
                ))
            })?;
        }
        Ok(path)
    }

    /// Get temporary storage path inside the `optional_temp_path` directory.
    pub fn optional_temp_path(&self) -> CollectionResult<Option<PathBuf>> {
        if let Some(path) = self.get_optional_temp_path() {
            if !path.exists() {
                std::fs::create_dir_all(&path).map_err(|e| {
                    CollectionError::service_error(format!(
                        "Failed to create optional temp directory at {}: {:?}",
                        path.display(),
                        e,
                    ))
                })?;
            }
            Ok(Some(path))
        } else {
            Ok(None)
        }
    }

    /// Get directory for snapshots-related temporary files.
    /// If the optional_temp_path is specified, it will be used instead of snapshots_temp_path.
    pub fn optional_temp_or_snapshot_temp_path(&self) -> CollectionResult<PathBuf> {
        match self.optional_temp_path() {
            Ok(Some(path)) => Ok(path),
            Ok(None) => self.snapshots_temp_path(),
            Err(err) => Err(err),
        }
    }

    /// Get directory for storage-related temporary files.
    /// If the optional_temp_path is specified, it will be used instead of storage_temp_path.
    pub fn optional_temp_or_storage_temp_path(&self) -> CollectionResult<PathBuf> {
        match self.optional_temp_path() {
            Ok(Some(path)) => Ok(path),
            Ok(None) => self.storage_temp_path(),
            Err(err) => Err(err),
        }
    }

    pub fn upload_dir(&self) -> CollectionResult<PathBuf> {
        let tmp_storage_dir = match self.optional_temp_path() {
            Ok(Some(path)) => path,
            Ok(None) => self.snapshots_temp_path()?,
            Err(err) => return Err(err),
        };

        let upload_dir = tmp_storage_dir.join(FILE_UPLOAD_SUBDIR_NAME);

        if !upload_dir.exists() {
            std::fs::create_dir_all(&upload_dir).map_err(|e| {
                CollectionError::service_error(format!(
                    "Failed to create upload directory at {}: {:?}",
                    upload_dir.display(),
                    e,
                ))
            })?;
        }
        Ok(upload_dir)
    }

    pub fn snapshots_download_tempdir(&self) -> CollectionResult<TempDir> {
        let tmp_storage_dir = match self.optional_temp_path() {
            Ok(Some(path)) => path,
            Ok(None) => self.snapshots_temp_path()?,
            Err(err) => return Err(err),
        };

        let download_tempdir = tempfile::Builder::new()
            .prefix("download-")
            .tempdir_in(tmp_storage_dir)?;

        Ok(download_tempdir)
    }

    pub fn clear_all_tmp_directories(&self) -> CollectionResult<()> {
        let snapshots_temp_path = self.get_snapshots_temp_path();
        let storage_temp_path = self.get_storage_temp_path();
        let optional_temp_path = self.get_optional_temp_path();

        if snapshots_temp_path.exists() {
            std::fs::remove_dir_all(&snapshots_temp_path).map_err(|e| {
                CollectionError::service_error(format!(
                    "Failed to remove snapshots temp directory at {}: {:?}",
                    snapshots_temp_path.display(),
                    e,
                ))
            })?;
        }

        if storage_temp_path.exists() {
            std::fs::remove_dir_all(&storage_temp_path).map_err(|e| {
                CollectionError::service_error(format!(
                    "Failed to remove storage temp directory at {}: {:?}",
                    storage_temp_path.display(),
                    e,
                ))
            })?;
        }

        if let Some(path) = optional_temp_path {
            if path.exists() {
                std::fs::remove_dir_all(&path).map_err(|e| {
                    CollectionError::service_error(format!(
                        "Failed to remove optional temp directory at {}: {:?}",
                        path.display(),
                        e,
                    ))
                })?;
            }
        }

        Ok(())
    }
}
