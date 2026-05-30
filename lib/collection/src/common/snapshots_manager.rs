use std::path::Path;

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
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[allow(dead_code)]
pub struct SnapshotStorageS3 {
    s3_config: S3Config,
}

pub struct SnapshotStorageLocalFS;

pub enum SnapshotStorageManager {
    LocalFS(SnapshotStorageLocalFS),
    S3(SnapshotStorageS3),
}

impl SnapshotStorageManager {
    pub fn new(s3_config: Option<S3Config>) -> Self {
        if let Some(s3_config) = s3_config {
            SnapshotStorageManager::S3(SnapshotStorageS3 { s3_config })
        } else {
            SnapshotStorageManager::LocalFS(SnapshotStorageLocalFS)
        }
    }

    pub async fn delete_snapshot(&self, snapshot_name: &Path) -> CollectionResult<bool> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.delete_snapshot(snapshot_name).await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl.delete_snapshot(snapshot_name).await
            }
        }
    }
    pub async fn list_snapshots(
        &self,
        directory: &Path,
    ) -> CollectionResult<Vec<SnapshotDescription>> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.list_snapshots(directory).await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl.list_snapshots(directory).await
            }
        }
    }
    pub async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.store_file(source_path, target_path).await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl.store_file(source_path, target_path).await
            }
        }
    }

    pub async fn get_stored_file(
        &self,
        storage_path: &Path,
        local_path: &Path,
    ) -> CollectionResult<()> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.get_stored_file(storage_path, local_path).await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl.get_stored_file(storage_path, local_path).await
            }
        }
    }
}

impl SnapshotStorageLocalFS {
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
    ) -> CollectionResult<SnapshotDescription> {
        // Steps:
        //
        // 1. Make sure that the target directory exists.
        // 2. Compute the checksum of the source file.
        // 3. Generate temporary file name, which should be used on the same file system as the target directory.
        // 4. Move or copy the source file to the temporary file. (move might not be possible if the source and target are on different file systems)
        // 5. Move the temporary file to the target file. (move is atomic, copy is not)

        if let Some(target_dir) = target_path.parent() {
            if !target_dir.exists() {
                std::fs::create_dir_all(target_dir)?;
            }
        }

        // Move snapshot to permanent location.
        // We can't move right away, because snapshot folder can be on another mounting point.
        // We can't copy to the target location directly, because copy is not atomic.
        // So we copy to the final location with a temporary name and then rename atomically.
        let target_path_tmp_move = target_path.with_extension("tmp");
        // Ensure that the temporary file is deleted on error
        let _temp_path = TempPath::from_path(&target_path_tmp_move);

        // compute and store the file's checksum before the final snapshot file is saved
        // to avoid making snapshot available without checksum
        let checksum_path = get_checksum_path(target_path);
        let checksum = hash_file(source_path).await?;
        let checksum_file = TempPath::from_path(&checksum_path);
        let mut file = tokio::fs::File::create(checksum_path.as_path()).await?;
        file.write_all(checksum.as_bytes()).await?;

        if target_path != source_path {
            move_file(&source_path, &target_path_tmp_move).await?;
            tokio::fs::rename(&target_path_tmp_move, &target_path).await?;
        }

        checksum_file.keep()?;
        get_snapshot_description(target_path).await
    }

    async fn get_stored_file(
        &self,
        storage_path: &Path,
        local_path: &Path,
    ) -> CollectionResult<()> {
        if let Some(target_dir) = local_path.parent() {
            if !target_dir.exists() {
                std::fs::create_dir_all(target_dir)?;
            }
        }

        if storage_path != local_path {
            move_file(&storage_path, &local_path).await?;
        }
        Ok(())
    }
}

impl SnapshotStorageS3 {
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
    ) -> CollectionResult<SnapshotDescription> {
        unimplemented!()
    }

    async fn get_stored_file(
        &self,
        _storage_path: &Path,
        _local_path: &Path,
    ) -> CollectionResult<()> {
        unimplemented!()
    }
}
