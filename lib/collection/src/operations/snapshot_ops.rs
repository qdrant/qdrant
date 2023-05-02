use std::path::Path;
use std::time::SystemTime;

use api::grpc::conversions::date_time_to_proto;
use chrono::NaiveDateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;
use validator::Validate;

use crate::operations::types::CollectionResult;

/// Defines source of truth for snapshot recovery
/// `Snapshot` means - prefer snapshot data over the current state
/// `Replica` means - prefer existing data over the snapshot
#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotPriority {
    Snapshot,
    #[default]
    Replica,
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
            .map(|duration| NaiveDateTime::from_timestamp(duration.as_secs() as i64, 0))
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
    let mut snapshots = Vec::new();
    let mut entries = tokio::fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        let entry = entry;
        let path = entry.path();
        if !path.is_dir() && path.extension().map(|s| s == "snapshot").unwrap_or(false) {
            snapshots.push(get_snapshot_description(&path).await?);
        }
    }
    Ok(snapshots)
}
