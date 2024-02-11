use std::path::{Path, PathBuf};

use super::SnapshotManager;
use crate::error::SnapshotManagerError;

impl SnapshotManager {
    pub(super) fn snapshots_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub(super) fn use_base(&self, path: impl AsRef<Path>) -> Result<PathBuf, std::io::Error> {
        Ok(self.path.canonicalize()?.join(path))
    }

    pub fn temp_path(&self) -> PathBuf {
        self.snapshots_path().join("tmp")
    }

    pub(super) async fn snapshot_exists(
        &self,
        snapshot_path: impl AsRef<Path>,
    ) -> Result<bool, SnapshotManagerError> {
        Ok(self.use_base(snapshot_path)?.exists())
    }

    /// Removes the specified snapshot and checksum files.
    pub(super) async fn remove_snapshot(
        &self,
        snapshot_path: impl AsRef<Path>,
    ) -> Result<(), SnapshotManagerError> {
        log::info!("Deleting snapshot {:?}", snapshot_path.as_ref());

        let snapshot_path = self.use_base(snapshot_path)?;
        let checksum_path = self.checksum_path(&snapshot_path);

        let (delete_snapshot, delete_checksum) = tokio::join!(
            tokio::fs::remove_file(snapshot_path),
            tokio::fs::remove_file(checksum_path),
        );
        delete_snapshot?;

        // We might not have a checksum file for the snapshot, ignore deletion errors in that case
        if let Err(err) = delete_checksum {
            log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
        }

        Ok(())
    }

    pub(super) async fn list_snapshots_in_directory(
        &self,
        directory: impl Into<PathBuf>,
    ) -> Result<Vec<String>, SnapshotManagerError> {
        let directory: PathBuf = directory.into();

        if !directory.exists() {
            return Ok(vec![]);
        }

        let mut entries = tokio::fs::read_dir(directory).await?;
        let mut snapshots = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if !path.is_dir() && path.extension().map_or(false, |ext| ext == "snapshot") {
                if let Some(file_name) = path.file_name() {
                    snapshots.push(file_name.to_string_lossy().to_string());
                }
            }
        }

        Ok(snapshots)
    }
}
