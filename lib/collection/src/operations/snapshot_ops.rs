use std::path::{Path, PathBuf};
use std::time::SystemTime;

use api::grpc::conversions::date_time_to_proto;
use chrono::NaiveDateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;
use validator::Validate;

use crate::operations::types::CollectionResult;

/// Defines source of truth for snapshot recovery:
/// `NoSync` means - restore snapshot without *any* additional synchronization.
/// `Snapshot` means - prefer snapshot data over the current state.
/// `Replica` means - prefer existing data over the snapshot.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPriority {
    NoSync,
    Snapshot,
    #[default]
    Replica,
    // `ShardTransfer` is for internal use only, and should not be exposed/used in public API
    #[serde(skip)]
    ShardTransfer,
}

impl TryFrom<i32> for SnapshotPriority {
    type Error = tonic::Status;

    fn try_from(snapshot_priority: i32) -> Result<Self, Self::Error> {
        api::grpc::qdrant::ShardSnapshotPriority::try_from(snapshot_priority)
            .map(Into::into)
            .map_err(|_| tonic::Status::invalid_argument("Malformed shard snapshot priority"))
    }
}

impl From<api::grpc::qdrant::ShardSnapshotPriority> for SnapshotPriority {
    fn from(snapshot_priority: api::grpc::qdrant::ShardSnapshotPriority) -> Self {
        match snapshot_priority {
            api::grpc::qdrant::ShardSnapshotPriority::NoSync => Self::NoSync,
            api::grpc::qdrant::ShardSnapshotPriority::Snapshot => Self::Snapshot,
            api::grpc::qdrant::ShardSnapshotPriority::Replica => Self::Replica,
            api::grpc::qdrant::ShardSnapshotPriority::ShardTransfer => Self::ShardTransfer,
        }
    }
}

impl From<SnapshotPriority> for api::grpc::qdrant::ShardSnapshotPriority {
    fn from(snapshot_priority: SnapshotPriority) -> Self {
        match snapshot_priority {
            SnapshotPriority::NoSync => Self::NoSync,
            SnapshotPriority::Snapshot => Self::Snapshot,
            SnapshotPriority::Replica => Self::Replica,
            SnapshotPriority::ShardTransfer => Self::ShardTransfer,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate, Clone)]
pub struct SnapshotRecover {
    /// Examples:
    /// - URL `http://localhost:8080/collections/my_collection/snapshots/my_snapshot`
    /// - Local path `file:///qdrant/snapshots/test_collection-2022-08-04-10-49-10.snapshot`
    pub location: Url,

    /// Defines which data should be used as a source of truth if there are other replicas in the cluster.
    /// If set to `Snapshot`, the snapshot will be used as a source of truth, and the current state will be overwritten.
    /// If set to `Replica`, the current state will be used as a source of truth, and after recovery if will be synchronized with the snapshot.
    #[serde(default)]
    pub priority: Option<SnapshotPriority>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct SnapshotDescription {
    pub name: String,
    pub creation_time: Option<NaiveDateTime>,
    pub size: u64,
}

impl From<SnapshotDescription> for api::grpc::qdrant::SnapshotDescription {
    fn from(value: SnapshotDescription) -> Self {
        Self {
            name: value.name,
            creation_time: value.creation_time.map(date_time_to_proto),
            size: value.size as i64,
        }
    }
}

pub async fn get_snapshot_description(path: &Path) -> CollectionResult<SnapshotDescription> {
    let name = path.file_name().unwrap().to_str().unwrap();
    let file_meta = tokio::fs::metadata(&path).await?;
    let creation_time = file_meta.created().ok().and_then(|created_time| {
        created_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .map(|duration| {
                NaiveDateTime::from_timestamp_opt(duration.as_secs() as i64, 0).unwrap()
            })
    });
    let size = file_meta.len();
    Ok(SnapshotDescription {
        name: name.to_string(),
        creation_time,
        size,
    })
}

pub async fn list_snapshots_in_directory(
    directory: &Path,
) -> CollectionResult<Vec<SnapshotDescription>> {
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct ShardSnapshotRecover {
    pub location: ShardSnapshotLocation,

    #[serde(default)]
    pub priority: Option<SnapshotPriority>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ShardSnapshotLocation {
    Url(Url),
    Path(PathBuf),
}

impl TryFrom<Option<api::grpc::qdrant::ShardSnapshotLocation>> for ShardSnapshotLocation {
    type Error = tonic::Status;

    fn try_from(
        snapshot_location: Option<api::grpc::qdrant::ShardSnapshotLocation>,
    ) -> Result<Self, Self::Error> {
        let Some(snapshot_location) = snapshot_location else {
            return Err(tonic::Status::invalid_argument(
                "Malformed shard snapshot location",
            ));
        };

        snapshot_location.try_into()
    }
}

impl TryFrom<api::grpc::qdrant::ShardSnapshotLocation> for ShardSnapshotLocation {
    type Error = tonic::Status;

    fn try_from(location: api::grpc::qdrant::ShardSnapshotLocation) -> Result<Self, Self::Error> {
        use api::grpc::qdrant::shard_snapshot_location;

        let Some(location) = location.location else {
            return Err(tonic::Status::invalid_argument(
                "Malformed shard snapshot location",
            ));
        };

        let location = match location {
            shard_snapshot_location::Location::Url(url) => {
                let url = Url::parse(&url).map_err(|err| {
                    tonic::Status::invalid_argument(format!(
                        "Invalid shard snapshot URL {url}: {err}",
                    ))
                })?;

                Self::Url(url)
            }

            shard_snapshot_location::Location::Path(path) => {
                let path = PathBuf::from(path);
                Self::Path(path)
            }
        };

        Ok(location)
    }
}
