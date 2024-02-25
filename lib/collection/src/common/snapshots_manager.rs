use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::operations::snapshot_ops::{
    get_checksum_path, get_snapshot_description, SnapshotDescription,
};
use crate::operations::types::CollectionResult;

#[derive(Clone, Deserialize, Debug)]
pub struct LocalFileSystemConfig {
    pub snapshots_path: String,
}

#[derive(Clone, Deserialize, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub snapshots_path: String,
}

impl LocalFileSystemConfig {
    pub fn new(snapshots_path: String) -> Self {
        Self { snapshots_path }
    }
}

#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    async fn delete_snapshot(&self, snapshot_name: &Path) -> CollectionResult<bool>;
    async fn list_snapshots(&self, directory: &Path) -> CollectionResult<Vec<SnapshotDescription>>;
}

#[async_trait]
impl SnapshotStorage for LocalFileSystemConfig {
    async fn delete_snapshot(&self, snapshot_path: &Path) -> CollectionResult<bool> {
        let checksum_path = get_checksum_path(snapshot_path);
        let (delete_snapshot, delete_checksum) = tokio::join!(
            tokio::fs::remove_file(snapshot_path),
            tokio::fs::remove_file(checksum_path),
        );
        delete_snapshot?;

        // We might not have a checksum file for the snapshot, ignore deletion errors in that case
        if let Err(err) = delete_checksum {
            log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
        }

        Ok(true)
    }

    async fn list_snapshots(&self, directory: &Path) -> CollectionResult<Vec<SnapshotDescription>> {
        let mut entries = tokio::fs::read_dir(directory).await?;
        let mut snapshots = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if !path.is_dir() && path.extension().map_or(false, |ext| ext == "snapshot") {
                snapshots.push(get_snapshot_description(&path).await?);
            }
        }

        Ok(snapshots)
    }
}

#[async_trait]
impl SnapshotStorage for S3Config {
    async fn delete_snapshot(&self, _snapshot_path: &Path) -> CollectionResult<bool> {
        unimplemented!()
    }
    async fn list_snapshots(
        &self,
        _directory: &Path,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        unimplemented!()
    }
}
