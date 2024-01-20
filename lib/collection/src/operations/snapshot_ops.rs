use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;
use validator::Validate;

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
        api::grpc::qdrant::ShardSnapshotPriority::from_i32(snapshot_priority)
            .map(Into::into)
            .ok_or_else(|| tonic::Status::invalid_argument("Malformed shard snapshot priority"))
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
