use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use api::grpc::conversions::naive_date_time_to_proto;
use chrono::NaiveDateTime;
use error::SnapshotManagerError;
use path_clean::PathClean;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};
use url::Url;
use uuid::Uuid;

use self::file::SnapshotFile;

pub mod error;
pub mod file;
mod helpers;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct SnapshotDescription {
    pub name: String,
    pub creation_time: Option<NaiveDateTime>,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

impl From<SnapshotDescription> for api::grpc::qdrant::SnapshotDescription {
    fn from(value: SnapshotDescription) -> Self {
        Self {
            name: value.name,
            creation_time: value.creation_time.map(naive_date_time_to_proto),
            size: value.size as i64,
            checksum: value.checksum,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotManagerInner {
    path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct SnapshotManager(Arc<SnapshotManagerInner>);

impl SnapshotManager {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        SnapshotManager(Arc::new(SnapshotManagerInner { path: path.into() }))
    }

    pub async fn do_delete_snapshot(
        &self,
        snapshot: &SnapshotFile,
        wait: bool,
    ) -> Result<bool, SnapshotManagerError> {
        let _self = self.clone();
        let snapshot = snapshot.clone();
        let task = tokio::spawn(async move { _self._do_delete_snapshot(&snapshot).await });

        if wait {
            task.await??;
        }

        Ok(true)
    }

    async fn _do_delete_snapshot(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(), SnapshotManagerError> {
        if !self.snapshot_exists(snapshot).await? {
            return Err(SnapshotManagerError::NotFound {
                description: if let Some(collection) = &snapshot.collection() {
                    format!(
                        "Collection {:?} snapshot {} not found",
                        collection,
                        snapshot.name()
                    )
                } else {
                    format!("Full storage snapshot {} not found", snapshot.name())
                },
            });
        }

        self.remove_snapshot(snapshot).await?;

        Ok(())
    }

    pub async fn get_snapshot_checksum(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<String, SnapshotManagerError> {
        let path = snapshot.get_checksum_path(self.snapshots_path());

        Ok(tokio::fs::read_to_string(&path).await?)
    }

    pub async fn get_snapshot_description(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<SnapshotDescription, SnapshotManagerError> {
        let path = snapshot.get_path(self.snapshots_path());
        let checksum = self.get_snapshot_checksum(snapshot).await.ok();

        let meta = tokio::fs::metadata(path).await?;
        let creation_time = meta.created().ok().and_then(|created_time| {
            created_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|duration| {
                    NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0).unwrap()
                })
        });

        Ok(SnapshotDescription {
            name: snapshot.name(),
            creation_time,
            size: meta.len(),
            checksum,
        })
    }

    pub async fn do_list_full_snapshots(
        &self,
    ) -> Result<Vec<SnapshotDescription>, SnapshotManagerError> {
        let directory = self.snapshots_path();
        let filenames = self.list_snapshots_in_directory(directory).await?;

        let mut out: Vec<SnapshotDescription> = Vec::with_capacity(filenames.len());

        for name in filenames {
            let snapshot = SnapshotFile::new_full(name);
            let desc = self.get_snapshot_description(&snapshot).await?;
            out.push(desc);
        }

        Ok(out)
    }

    pub async fn do_list_collection_snapshots(
        &self,
        collection: &str,
    ) -> Result<Vec<SnapshotDescription>, SnapshotManagerError> {
        let directory = self.snapshots_path().join(collection);

        let filenames = self.list_snapshots_in_directory(directory).await?;

        let mut out: Vec<SnapshotDescription> = Vec::with_capacity(filenames.len());

        for name in filenames {
            let snapshot = SnapshotFile::new_collection(name, collection);
            let desc = self.get_snapshot_description(&snapshot).await?;
            out.push(desc);
        }

        Ok(out)
    }

    pub async fn do_list_shard_snapshots(
        &self,
        collection: &str,
        shard: u32,
    ) -> Result<Vec<SnapshotDescription>, SnapshotManagerError> {
        let base = self.snapshots_path();
        let directory = SnapshotFile::new_shard("", collection, shard).get_directory(base);

        let filenames = self.list_snapshots_in_directory(directory).await?;

        let mut out: Vec<SnapshotDescription> = Vec::with_capacity(filenames.len());

        for name in filenames {
            let snapshot = SnapshotFile::new_shard(name, collection, shard);
            let desc = self.get_snapshot_description(&snapshot).await?;
            out.push(desc);
        }

        Ok(out)
    }

    pub fn ensure_snapshots_path(&self, collection_name: &str) -> Result<(), SnapshotManagerError> {
        let snapshots_path = self.snapshots_path().join(collection_name);

        std::fs::create_dir_all(snapshots_path).map_err(|err| {
            SnapshotManagerError::service_error(format!(
                "Can't create directory for snapshots {collection_name}. Error: {err}"
            ))
        })?;

        Ok(())
    }

    pub async fn save_snapshot(
        &self,
        snapshot: &SnapshotFile,
        snapshot_file: TempPath,
        checksum_file: TempPath,
    ) -> Result<(), SnapshotManagerError> {
        // Snapshot files are ready now, move them into the right place
        let snapshot_temp = snapshot_file.keep()?;
        let checksum_temp = checksum_file.keep()?;

        let base = self.snapshots_path();

        let snapshot_path = snapshot.get_path(&base);
        let checksum_path = snapshot.get_checksum_path(base);

        let dir = snapshot_path.parent().unwrap();
        if !dir.exists() {
            tokio::fs::create_dir_all(dir).await?;
        }

        tokio::fs::rename(snapshot_temp, snapshot_path).await?;
        tokio::fs::rename(checksum_temp, checksum_path).await?;

        Ok(())
    }

    pub async fn get_snapshot_path(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(PathBuf, Option<TempPath>), SnapshotManagerError> {
        if snapshot.is_oop() {
            return Ok((snapshot.get_path(self.snapshots_path()), None));
        }

        let path = snapshot.get_path(self.snapshots_path());

        if !path.exists() {
            Err(SnapshotManagerError::NotFound {
                description: format!("Snapshot {} not found", snapshot.name()),
            })
        } else {
            Ok((path, None))
        }
    }

    pub fn get_snapshot_path_sync(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(PathBuf, Option<TempPath>), SnapshotManagerError> {
        if snapshot.is_oop() {
            return Ok((snapshot.get_path(self.snapshots_path()), None));
        }

        Ok((snapshot.get_path(self.snapshots_path()), None))
    }

    pub async fn get_snapshot_file(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(File, Option<TempPath>), SnapshotManagerError> {
        let (path, temp) = self.get_snapshot_path(snapshot).await?;
        Ok((File::open(path)?, temp))
    }

    pub fn get_snapshot_file_sync(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(File, Option<TempPath>), SnapshotManagerError> {
        let (path, temp) = self.get_snapshot_path_sync(snapshot)?;
        Ok((File::open(path)?, temp))
    }

    pub async fn do_save_uploaded_snapshot(
        &self,
        collection: &str,
        name: Option<String>,
        file: NamedTempFile<File>,
    ) -> Result<Url, SnapshotManagerError> {
        let name = name.unwrap_or_else(|| Uuid::new_v4().to_string());

        let snapshot = SnapshotFile::new_collection(name, collection);

        let path = snapshot
            .get_path(self.snapshots_path().canonicalize()?)
            .clean();

        let (_, temp_path) = file.keep()?;

        tokio::fs::create_dir_all(path.parent().unwrap()).await?;

        if tokio::fs::rename(&temp_path, &path).await.is_err() {
            tokio::fs::copy(&temp_path, &path).await?;
            tokio::fs::remove_file(temp_path).await?;
        }

        Url::from_file_path(&path).map_err(|_| {
            SnapshotManagerError::service_error(format!(
                "Failed to convert path to URL: {}",
                path.display(),
            ))
        })
    }
}
