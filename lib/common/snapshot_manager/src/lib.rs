use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use ::s3::creds::Credentials;
use ::s3::{Bucket, Region};
use api::grpc::conversions::date_time_to_proto;
use chrono::NaiveDateTime;
use error::SnapshotManagerError;
use path_clean::PathClean;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempPath};
use tokio::fs::OpenOptions;
use url::Url;
use uuid::Uuid;
use validator::Validate;

//use crate::{types::SnapshotsS3Config, dispatcher::Dispatcher, content_manager::toc::FULL_SNAPSHOT_FILE_NAME};
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
            creation_time: value.creation_time.map(date_time_to_proto),
            size: value.size as i64,
            checksum: value.checksum,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum SnapshotsS3Service {
    AWS { region: String },
    R2 { account_id: String },
    Custom { region: String, endpoint: String },
}

impl From<SnapshotsS3Service> for Region {
    fn from(value: SnapshotsS3Service) -> Self {
        match value {
            SnapshotsS3Service::AWS { region } => {
                region.parse().expect("Invalid AWS region specified.")
            }
            SnapshotsS3Service::R2 { account_id } => Region::R2 { account_id },
            SnapshotsS3Service::Custom { region, endpoint } => Region::Custom { region, endpoint },
        }
    }
}

/// Configuration for storing snapshots on S3. Specifying region and bucket will automatically enable this functionality.
#[derive(Clone, Debug, Deserialize, Validate)]
pub struct SnapshotsS3Config {
    #[validate(length(min = 1))]
    pub bucket: String,
    pub service: SnapshotsS3Service,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SnapshotManagerInner {
    path: PathBuf,
    config_s3: Option<SnapshotsS3Config>,
    bucket: Option<Bucket>,
}

#[derive(Clone, Debug)]
pub struct SnapshotManager(Arc<SnapshotManagerInner>);

impl SnapshotManager {
    pub fn new(path: impl Into<PathBuf>, config_s3: Option<SnapshotsS3Config>) -> Self {
        let bucket: Option<Bucket> = config_s3.as_ref().map(|config_s3| {
            Bucket::new(
                &config_s3.bucket,
                config_s3.service.clone().into(),
                Credentials::new(
                    config_s3.access_key.as_deref(),
                    config_s3.secret_key.as_deref(),
                    None,
                    None,
                    None,
                )
                .expect("Failed to create S3 credentials. Have you configured them correctly?"),
            )
            .unwrap()
        });

        let path = path.into();

        if config_s3.is_none() && !path.exists() {
            create_dir_all(&path).expect("Can't create Snapshots directory");
        }

        SnapshotManager(Arc::new(SnapshotManagerInner {
            path,
            config_s3,
            bucket,
        }))
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

        if let Some(bucket) = self.s3_bucket() {
            let path = self.s3ify_path(path)?;
            let obj = bucket.get_object(path).await?;
            Ok(obj.as_str()?.to_string())
        } else {
            Ok(tokio::fs::read_to_string(&path).await?)
        }
    }

    pub async fn get_snapshot_description(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<SnapshotDescription, SnapshotManagerError> {
        let path = snapshot.get_path(self.snapshots_path());
        let checksum = self.get_snapshot_checksum(snapshot).await.ok();

        let (creation_time, size) = if let Some(bucket) = self.s3_bucket() {
            let path = self.s3ify_path(path)?;
            let (head, code) = bucket.head_object(path).await?;

            if code == 404 {
                return Err(SnapshotManagerError::NotFound {
                    description: format!("Snapshot {:?} not found", snapshot.name()),
                });
            }

            let creation_time = head
                .last_modified
                .and_then(|x| NaiveDateTime::parse_from_str(&x, "&a, %d %b %Y %H:%M:%S %Z").ok());

            (creation_time, head.content_length.unwrap_or(0) as u64)
        } else {
            let meta = tokio::fs::metadata(path).await?;
            let creation_time = meta.created().ok().and_then(|created_time| {
                created_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .ok()
                    .map(|duration| {
                        NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0).unwrap()
                    })
            });

            (creation_time, meta.len())
        };

        Ok(SnapshotDescription {
            name: snapshot.name(),
            creation_time,
            size,
            checksum,
        })
    }

    pub async fn is_path_on_s3(&self, path: &Path) -> Result<bool, SnapshotManagerError> {
        if self.using_s3() {
            let canon = path.join(self.snapshots_path().canonicalize()?).clean();
            if canon.exists() {
                Ok(false)
            } else {
                Ok(canon.starts_with(self.snapshots_path().canonicalize()?)
                    && !canon.starts_with(self.temp_path().canonicalize()?))
            }
        } else {
            Ok(false)
        }
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
        if !self.using_s3() {
            let snapshots_path = self.snapshots_path().join(collection_name);

            std::fs::create_dir_all(snapshots_path).map_err(|err| {
                SnapshotManagerError::service_error(format!(
                    "Can't create directory for snapshots {collection_name}. Error: {err}"
                ))
            })?;
        }

        Ok(())
    }

    pub async fn save_snapshot(
        &self,
        snapshot: &SnapshotFile,
        snapshot_file: TempPath,
        checksum_file: TempPath,
    ) -> Result<(), SnapshotManagerError> {
        if let Some(bucket) = self.s3_bucket() {
            // Sync snapshot files to S3 and drop them
            let snapshot_path = self.s3ify_path(snapshot.get_path(self.snapshots_path()))?;
            let checksum_path =
                self.s3ify_path(snapshot.get_checksum_path(self.snapshots_path()))?;

            let mut snapshot_stream = tokio::fs::File::open(snapshot_file.to_path_buf()).await?;
            let checksum = tokio::fs::read(checksum_file.to_path_buf()).await?;

            bucket.put_object(checksum_path, &checksum).await?;
            bucket
                .put_object_stream(&mut snapshot_stream, snapshot_path)
                .await?;

            let _ = snapshot_file.close();
            let _ = checksum_file.close();
        } else {
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
        }

        Ok(())
    }

    pub async fn get_snapshot_path(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(PathBuf, Option<TempPath>), SnapshotManagerError> {
        if snapshot.is_oop() {
            return Ok((snapshot.get_path(self.snapshots_path()), None));
        }

        if let Some(bucket) = self.s3_bucket() {
            let f = tempfile::Builder::new().tempfile_in(self.temp_path())?;
            let path = self.s3ify_path(snapshot.get_path(self.snapshots_path()))?;
            let mut file = OpenOptions::new().write(true).open(f.path()).await?;

            bucket.get_object_to_writer(path, &mut file).await?;

            let f = f.into_temp_path();

            Ok((f.to_path_buf(), Some(f)))
        } else {
            Ok((snapshot.get_path(self.snapshots_path()), None))
        }
    }

    pub fn get_snapshot_path_sync(
        &self,
        snapshot: &SnapshotFile,
    ) -> Result<(PathBuf, Option<TempPath>), SnapshotManagerError> {
        if snapshot.is_oop() {
            return Ok((snapshot.get_path(self.snapshots_path()), None));
        }

        if let Some(bucket) = self.s3_bucket() {
            let f = tempfile::Builder::new().tempfile_in(self.temp_path())?;
            let path = self.s3ify_path(snapshot.get_path(self.snapshots_path()))?;
            let mut file = std::fs::OpenOptions::new().write(true).open(f.path())?;

            file.write_all(bucket.get_object_blocking(path)?.as_slice())?;

            let f = f.into_temp_path();

            Ok((f.to_path_buf(), Some(f)))
        } else {
            Ok((snapshot.get_path(self.snapshots_path()), None))
        }
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

        if let Some(bucket) = self.s3_bucket() {
            let mut f = tokio::fs::File::open(file.path()).await?;
            let path = self.s3ify_path(&path)?;
            bucket.put_object_stream(&mut f, path).await?;
        } else {
            let (_, temp_path) = file.keep()?;

            tokio::fs::create_dir_all(path.parent().unwrap()).await?;

            if tokio::fs::rename(&temp_path, &path).await.is_err() {
                tokio::fs::copy(&temp_path, &path).await?;
                tokio::fs::remove_file(temp_path).await?;
            }
        }

        Url::from_file_path(&path).map_err(|_| {
            SnapshotManagerError::service_error(format!(
                "Failed to convert path to URL: {}",
                path.display(),
            ))
        })
    }
}
