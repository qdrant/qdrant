use std::path::{Path, PathBuf};
use std::sync::Arc;

use actix_web::HttpRequest;
use object_store::aws::AmazonS3Builder;
use serde::Deserialize;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;

use super::snapshot_stream::{SnapShotStreamCloudStrage, SnapShotStreamLocalFS, SnapshotStream};
use crate::common::file_utils::move_file;
use crate::common::sha_256::hash_file;
use crate::operations::snapshot_ops::{
    get_checksum_path, get_snapshot_description, SnapshotDescription,
};
use crate::operations::snapshot_storage_ops::{self};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::LockedShardHolder;

#[derive(Clone, Deserialize, Debug, Default)]
pub struct SnapShotsConfig {
    pub snapshots_storage: SnapshotsStorageConfig,
    pub s3_config: Option<S3Config>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub enum SnapshotsStorageConfig {
    #[default]
    Local,
    S3,
}

#[derive(Clone, Deserialize, Debug, Default)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub endpoint_url: Option<String>,
}

#[allow(dead_code)]
pub struct SnapshotStorageCloud {
    client: Box<dyn object_store::ObjectStore>,
}

pub struct SnapshotStorageLocalFS;

pub enum SnapshotStorageManager {
    LocalFS(SnapshotStorageLocalFS),
    // Assuming that we can have common operations for all cloud storages
    S3(SnapshotStorageCloud),
    // <TODO> : Implement other cloud storage
    // GCS(SnapshotStorageCloud),
    // AZURE(SnapshotStorageCloud),
}

impl SnapshotStorageManager {
    pub fn new(snapshots_config: SnapShotsConfig) -> CollectionResult<Self> {
        match snapshots_config.clone().snapshots_storage {
            SnapshotsStorageConfig::Local => {
                Ok(SnapshotStorageManager::LocalFS(SnapshotStorageLocalFS))
            }
            SnapshotsStorageConfig::S3 => {
                let mut builder = AmazonS3Builder::new();
                if let Some(s3_config) = &snapshots_config.s3_config {
                    builder = builder.with_bucket_name(&s3_config.bucket);

                    if let Some(access_key) = &s3_config.access_key {
                        builder = builder.with_access_key_id(access_key);
                    }
                    if let Some(secret_key) = &s3_config.secret_key {
                        builder = builder.with_secret_access_key(secret_key);
                    }
                    if let Some(region) = &s3_config.region {
                        builder = builder.with_region(region);
                    }
                    if let Some(endpoint_url) = &s3_config.endpoint_url {
                        builder = builder.with_endpoint(endpoint_url);
                        if endpoint_url.starts_with("http://") {
                            builder = builder.with_allow_http(true);
                        }
                    }
                }
                let client: Box<dyn object_store::ObjectStore> =
                    Box::new(builder.build().map_err(|e| {
                        CollectionError::service_error(format!("Failed to create S3 client: {}", e))
                    })?);

                Ok(SnapshotStorageManager::S3(SnapshotStorageCloud { client }))
            }
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

    pub async fn get_snapshot_path(
        &self,
        snapshots_path: &Path,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl
                    .get_snapshot_path(snapshots_path, snapshot_name)
                    .await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl
                    .get_snapshot_path(snapshots_path, snapshot_name)
                    .await
            }
        }
    }

    pub async fn get_full_snapshot_path(
        &self,
        snapshots_path: &str,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl
                    .get_full_snapshot_path(snapshots_path, snapshot_name)
                    .await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl
                    .get_full_snapshot_path(snapshots_path, snapshot_name)
                    .await
            }
        }
    }

    pub async fn get_shard_snapshot_path(
        &self,
        shards_holder: Arc<LockedShardHolder>,
        shard_id: ShardId,
        snapshots_path: &Path,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl
                    .get_shard_snapshot_path(
                        shards_holder,
                        shard_id,
                        snapshots_path,
                        snapshot_file_name,
                    )
                    .await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl
                    .get_shard_snapshot_path(
                        shards_holder,
                        shard_id,
                        snapshots_path,
                        snapshot_file_name,
                    )
                    .await
            }
        }
    }

    pub async fn get_snapshot_stream(
        self,
        req: HttpRequest,
        snapshot_path: &Path,
    ) -> CollectionResult<SnapshotStream> {
        match self {
            SnapshotStorageManager::LocalFS(storage_impl) => {
                storage_impl.get_snapshot_stream(req, snapshot_path).await
            }
            SnapshotStorageManager::S3(storage_impl) => {
                storage_impl.get_snapshot_stream(snapshot_path).await
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

    /// Get absolute file path for a full snapshot by name
    ///
    /// This enforces the file to be inside the snapshots directory
    async fn get_full_snapshot_path(
        &self,
        snapshots_path: &str,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        let absolute_snapshot_dir = Path::new(snapshots_path).canonicalize().map_err(|_| {
            CollectionError::not_found(format!("Snapshot directory: {snapshots_path}"))
        })?;

        let absolute_snapshot_path = absolute_snapshot_dir
            .join(snapshot_name)
            .canonicalize()
            .map_err(|_| CollectionError::not_found(format!("Snapshot {snapshot_name}")))?;

        if !absolute_snapshot_path.starts_with(absolute_snapshot_dir) {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        if !absolute_snapshot_path.is_file() {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        Ok(absolute_snapshot_path)
    }

    /// Get absolute file path for a collection snapshot by name
    ///
    /// This enforces the file to be inside the snapshots directory
    async fn get_snapshot_path(
        &self,
        snapshots_path: &Path,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        let absolute_snapshot_dir = snapshots_path.canonicalize().map_err(|_| {
            CollectionError::not_found(format!("Snapshot directory: {}", snapshots_path.display()))
        })?;

        let absolute_snapshot_path = absolute_snapshot_dir
            .join(snapshot_name)
            .canonicalize()
            .map_err(|_| CollectionError::not_found(format!("Snapshot {snapshot_name}")))?;

        if !absolute_snapshot_path.starts_with(absolute_snapshot_dir) {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        if !absolute_snapshot_path.is_file() {
            return Err(CollectionError::not_found(format!(
                "Snapshot {snapshot_name}"
            )));
        }

        Ok(absolute_snapshot_path)
    }

    async fn get_shard_snapshot_path(
        &self,
        shards_holder: Arc<LockedShardHolder>,
        shard_id: ShardId,
        snapshots_path: &Path,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        shards_holder
            .read()
            .await
            .get_shard_snapshot_path(snapshots_path, shard_id, snapshot_file_name)
            .await
    }

    async fn get_snapshot_stream(
        &self,
        req: HttpRequest,
        snapshot_path: &Path,
    ) -> CollectionResult<SnapshotStream> {
        Ok(SnapshotStream::LocalFS(SnapShotStreamLocalFS {
            req,
            snapshot_path: snapshot_path.to_path_buf(),
        }))
    }
}

impl SnapshotStorageCloud {
    async fn delete_snapshot(&self, snapshot_path: &Path) -> CollectionResult<bool> {
        snapshot_storage_ops::delete_snapshot(&self.client, snapshot_path).await
    }

    async fn list_snapshots(&self, directory: &Path) -> CollectionResult<Vec<SnapshotDescription>> {
        snapshot_storage_ops::list_snapshot_descriptions(&self.client, directory).await
    }

    async fn store_file(
        &self,
        source_path: &Path,
        target_path: &Path,
    ) -> CollectionResult<SnapshotDescription> {
        snapshot_storage_ops::multipart_upload(&self.client, source_path, target_path).await?;
        snapshot_storage_ops::get_snapshot_description(&self.client, target_path).await
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
            // download snapshot from cloud storage to local path
            snapshot_storage_ops::download_snapshot(&self.client, storage_path, local_path).await?;
        }
        Ok(())
    }

    async fn get_snapshot_path(
        &self,
        snapshots_path: &Path,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        let absolute_snapshot_dir = snapshots_path;
        let absolute_snapshot_path = absolute_snapshot_dir.join(snapshot_name);
        Ok(absolute_snapshot_path)
    }

    async fn get_full_snapshot_path(
        &self,
        snapshots_path: &str,
        snapshot_name: &str,
    ) -> CollectionResult<PathBuf> {
        let absolute_snapshot_dir = PathBuf::from(snapshots_path);
        let absolute_snapshot_path = absolute_snapshot_dir.join(snapshot_name);
        Ok(absolute_snapshot_path)
    }

    async fn get_shard_snapshot_path(
        &self,
        _shards_holder: Arc<LockedShardHolder>,
        shard_id: u32,
        snapshots_path: &Path,
        snapshot_file_name: impl AsRef<Path>,
    ) -> CollectionResult<PathBuf> {
        Ok(snapshots_path
            .join(format!("shards/{shard_id}"))
            .join(snapshot_file_name))
    }

    pub async fn get_snapshot_stream(
        &self,
        snapshot_path: &Path,
    ) -> CollectionResult<SnapshotStream> {
        let snapshot_path = snapshot_storage_ops::trim_dot_slash(snapshot_path)?;
        let download = self.client.get(&snapshot_path).await.map_err(|e| {
            CollectionError::service_error(format!("Failed to get {}: {}", snapshot_path, e))
        })?;
        Ok(SnapshotStream::CloudStorage(SnapShotStreamCloudStrage {
            streamer: Box::pin(download.into_stream()),
        }))
    }
}
