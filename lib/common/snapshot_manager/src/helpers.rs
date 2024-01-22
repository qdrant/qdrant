use std::path::{self, PathBuf};

use path_clean::PathClean;
use s3::error::S3Error;
use s3::Bucket;

use super::file::SnapshotFile;
use super::SnapshotManager;
use crate::error::SnapshotManagerError;

impl SnapshotManager {
    pub(super) fn using_s3(&self) -> bool {
        self.0.config_s3.is_some()
    }

    pub(super) fn snapshots_path(&self) -> PathBuf {
        self.0.path.clone()
    }

    pub(super) fn s3_bucket(&self) -> Option<&Bucket> {
        self.0.bucket.as_ref()
    }

    /// Turns any dirty path into an S3-compatible path.
    pub(super) fn s3ify_path(
        &self,
        path: impl Into<PathBuf>,
    ) -> Result<String, SnapshotManagerError> {
        let path: PathBuf = path.into();
        let snapshots_canon = self.snapshots_path().canonicalize()?;
        let path = snapshots_canon.join(path).clean();
        let path =
            path.strip_prefix(snapshots_canon)
                .map_err(|_| SnapshotManagerError::BadRequest {
                    description: format!("Provided file path is invalid."),
                })?;
        let p = path.to_string_lossy().to_string();
        if !path.has_root() {
            Ok(format!("{}{p}", std::path::MAIN_SEPARATOR_STR))
        } else {
            Ok(p)
        }
    }

    pub fn temp_path(&self) -> PathBuf {
        self.snapshots_path().join("tmp")
    }

    pub(super) async fn snapshot_exists(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<bool, SnapshotManagerError> {
        if let Some(bucket) = self.s3_bucket() {
            let path = self.s3ify_path(snapshot.get_path(self.snapshots_path()))?;
            match bucket.head_object(path).await {
                Ok((_, code)) => {
                    if code == 404 {
                        Ok(false)
                    } else if code >= 200 && code < 300 {
                        Ok(true)
                    } else {
                        Err(S3Error::Http(code, "".to_string()).into())
                    }
                }
                Err(e) => match e {
                    S3Error::Http(code, _) => {
                        if code == 404 {
                            Ok(false)
                        } else {
                            Err(e.into())
                        }
                    }
                    _ => Err(e.into()),
                },
            }
        } else {
            Ok(snapshot.get_path(self.snapshots_path()).exists())
        }
    }

    /// Removes the specified snapshot and checksum files.
    pub(super) async fn remove_snapshot(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(), SnapshotManagerError> {
        if let Some(collection) = &snapshot.collection() {
            log::info!(
                "Deleting collection {:?} snapshot {:?}",
                collection,
                snapshot.name()
            );
        } else {
            log::info!("Deleting full storage snapshot {:?}", snapshot.name());
        }

        if let Some(bucket) = self.s3_bucket() {
            let base = self.snapshots_path();
            let snapshot_path = self.s3ify_path(snapshot.get_path(&base))?;
            let checksum_path = self.s3ify_path(snapshot.get_checksum_path(&base))?;

            let (delete_snapshot, delete_checksum) = tokio::join!(
                bucket.delete_object(snapshot_path),
                bucket.delete_object(checksum_path),
            );
            delete_snapshot?;

            // We might not have a checksum file for the snapshot, ignore deletion errors in that case
            if let Err(err) = delete_checksum {
                log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
            }
        } else {
            let base = self.snapshots_path();
            let snapshot_path = snapshot.get_path(&base);
            let checksum_path = snapshot.get_checksum_path(base);

            let (delete_snapshot, delete_checksum) = tokio::join!(
                tokio::fs::remove_file(snapshot_path),
                tokio::fs::remove_file(checksum_path),
            );
            delete_snapshot?;

            // We might not have a checksum file for the snapshot, ignore deletion errors in that case
            if let Err(err) = delete_checksum {
                log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
            }
        }

        Ok(())
    }

    pub(super) async fn list_snapshots_in_directory(
        &self,
        directory: impl Into<PathBuf>,
    ) -> Result<Vec<String>, SnapshotManagerError> {
        let directory: PathBuf = directory.into();

        if let Some(bucket) = self.s3_bucket() {
            let path = self.s3ify_path(directory)?;
            let list = bucket
                .list(
                    format!("{}{}", &path[1..], path::MAIN_SEPARATOR_STR), 
                    Some(path::MAIN_SEPARATOR_STR.to_string())
                )
                .await?[0]
                .contents
                .iter()
                .map(|x| PathBuf::from(&x.key))
                .filter(|x| {
                    x
                        .extension()
                        .map_or(false, |ext| ext == "snapshot")
                })
                .map(|x| x.file_name().unwrap().to_string_lossy().to_string())
                .collect();
            Ok(list)
        } else {
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
}
