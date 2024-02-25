use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;

use crate::common::file_utils::move_file;
use crate::common::sha_256::hash_file;
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
    async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
        shard: bool,
    ) -> CollectionResult<SnapshotDescription>;
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

    async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
        shard: bool,
    ) -> CollectionResult<SnapshotDescription> {
        // Move snapshot to permanent location.
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, because copy is not atomic.
        // So we copy to the final location with a temporary name and then rename atomically.
        let snapshot_path_tmp_move = target_path.with_extension("tmp");
        // Ensure that the temporary file is deleted on error
        let _temp_path = TempPath::from_path(&snapshot_path_tmp_move);
        // compute and store the file's checksum before the final snapshot file is saved
        // to avoid making snapshot available without checksum
        let checksum_path = get_checksum_path(target_path);
        let checksum = hash_file(source_path).await?;
        let checksum_file = TempPath::from_path(&checksum_path);
        let mut file = tokio::fs::File::create(checksum_path.as_path()).await?;
        file.write_all(checksum.as_bytes()).await?;

        if target_path != source_path {
            if !shard {
                tokio::fs::copy(&source_path, &target_path).await?;
            }
            let snapshot_file = tempfile::TempPath::from_path(target_path);
            // `tempfile::NamedTempFile::persist` does not work if destination file is on another
            // file-system, so we have to move the file explicitly
            move_file(&source_path, &target_path).await?;

            snapshot_file.keep()?;
        }

        checksum_file.keep()?;
        get_snapshot_description(target_path).await
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

    async fn store_file(
        &self,
        _source_path: &Path,
        _target_path: &Path,
        _shard: bool,
    ) -> CollectionResult<SnapshotDescription> {
        unimplemented!()
    }
}
