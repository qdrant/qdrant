use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use api::grpc::conversions::naive_date_time_to_proto;
use chrono::NaiveDateTime;
use error::SnapshotManagerError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};
use url::Url;
use uuid::Uuid;

pub mod error;
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
pub struct SnapshotManager {
    path: PathBuf,
}

impl SnapshotManager {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        SnapshotManager { path: path.into() }
    }

    pub fn scope(&self, path: impl AsRef<Path>) -> Self {
        SnapshotManager {
            path: self.path.join(path.as_ref()),
        }
    }

    pub fn checksum_path(&self, snapshot_path: impl AsRef<Path>) -> PathBuf {
        PathBuf::from(format!("{}.checksum", snapshot_path.as_ref().display()))
    }

    pub async fn do_delete_snapshot(
        &self,
        snapshot: impl AsRef<Path>,
        wait: bool,
    ) -> Result<bool, SnapshotManagerError> {
        let _self = self.clone();
        let snapshot = self.use_base(snapshot);
        let task = tokio::spawn(async move { _self._do_delete_snapshot(snapshot).await });

        if wait {
            task.await??;
        }

        Ok(true)
    }

    async fn _do_delete_snapshot(
        &self,
        snapshot_path: PathBuf,
    ) -> Result<(), SnapshotManagerError> {
        if !self.snapshot_exists(&snapshot_path).await? {
            return Err(SnapshotManagerError::NotFound {
                description: format!("Snapshot {:?} not found", snapshot_path),
            });
        }

        self.remove_snapshot(snapshot_path).await?;

        Ok(())
    }

    pub async fn get_snapshot_checksum(
        &self,
        snapshot: impl AsRef<Path>,
    ) -> Result<String, SnapshotManagerError> {
        let path = self.checksum_path(self.use_base(snapshot));

        Ok(tokio::fs::read_to_string(&path).await?)
    }

    pub async fn get_snapshot_description(
        &self,
        snapshot: impl AsRef<Path>,
    ) -> Result<SnapshotDescription, SnapshotManagerError> {
        let path = self.use_base(snapshot);
        let checksum = self.get_snapshot_checksum(&path).await.ok();

        let meta = tokio::fs::metadata(&path).await?;
        let creation_time = meta.created().ok().and_then(|created_time| {
            created_time
                .duration_since(SystemTime::UNIX_EPOCH)
                .ok()
                .map(|duration| {
                    NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0).unwrap()
                })
        });

        Ok(SnapshotDescription {
            name: path
                .file_name()
                .map(|x| x.to_string_lossy().to_string())
                .unwrap_or_else(|| String::with_capacity(0)),
            creation_time,
            size: meta.len(),
            checksum,
        })
    }

    pub async fn do_list_snapshots(
        &self,
    ) -> Result<Vec<SnapshotDescription>, SnapshotManagerError> {
        let directory = self.snapshots_path();
        let filenames = self.list_snapshots_in_directory(directory).await?;

        let mut out: Vec<SnapshotDescription> = Vec::with_capacity(filenames.len());

        for name in filenames {
            let desc = self.get_snapshot_description(&PathBuf::from(name)).await?;
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
        snapshot_file: TempPath,
        checksum_file: TempPath,
    ) -> Result<PathBuf, SnapshotManagerError> {
        // Snapshot files are ready now, move them into the right place
        let snapshot_temp = snapshot_file.keep()?;
        let checksum_temp = checksum_file.keep()?;

        let snapshot = PathBuf::from(snapshot_temp.file_name().ok_or(
            SnapshotManagerError::BadInput {
                description: "Received snapshot_file without filename?".to_string(),
            },
        )?);

        let snapshot_path = self.use_base(&snapshot);
        let checksum_path = self.checksum_path(&snapshot_path);

        let dir = snapshot_path.parent().unwrap();
        if !dir.exists() {
            tokio::fs::create_dir_all(dir).await?;
        }

        tokio::fs::rename(snapshot_temp, snapshot_path).await?;
        tokio::fs::rename(checksum_temp, checksum_path).await?;

        Ok(snapshot)
    }

    pub fn get_snapshot_path(
        &self,
        snapshot: impl AsRef<Path>,
    ) -> Result<(PathBuf, Option<TempPath>), SnapshotManagerError> {
        let path = self.use_base(&snapshot);

        if !path.exists() {
            Err(SnapshotManagerError::NotFound {
                description: format!("Snapshot {} not found", snapshot.as_ref().display()),
            })
        } else {
            Ok((path, None))
        }
    }

    pub async fn get_snapshot_file(
        &self,
        snapshot: impl AsRef<Path>,
    ) -> Result<(File, Option<TempPath>), SnapshotManagerError> {
        let (path, temp) = self.get_snapshot_path(snapshot)?;
        Ok((File::open(path)?, temp))
    }

    pub async fn do_save_uploaded_snapshot(
        &self,
        name: Option<String>,
        file: NamedTempFile<File>,
    ) -> Result<Url, SnapshotManagerError> {
        let name = name.unwrap_or_else(|| Uuid::new_v4().to_string());

        let path = self.use_base(name);

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
