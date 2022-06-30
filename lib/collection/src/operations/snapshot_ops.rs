use crate::CollectionResult;
use chrono::NaiveDateTime;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::SystemTime;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct SnapshotDescription {
    pub name: String,
    pub creation_time: NaiveDateTime,
    pub size: u64,
}

pub async fn get_snapshot_description(path: &Path) -> CollectionResult<SnapshotDescription> {
    let name = path.file_name().unwrap().to_str().unwrap();
    let file_meta = tokio::fs::metadata(&path).await?;
    let creation_time = file_meta
        .created()?
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();
    let size = file_meta.len();
    Ok(SnapshotDescription {
        name: name.to_string(),
        creation_time: NaiveDateTime::from_timestamp(creation_time as i64, 0),
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
