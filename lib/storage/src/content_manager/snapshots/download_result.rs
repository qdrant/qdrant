use shard::snapshots::snapshot_data::SnapshotData;

pub struct DownloadResult {
    pub snapshot: SnapshotData,
    /// Sha256 hash of the downloaded snapshot file, if computed
    pub hash: Option<String>,
}
