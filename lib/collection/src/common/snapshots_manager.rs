use std::path::Path;

use async_trait::async_trait;
use serde::Deserialize;

use crate::operations::types::CollectionResult;

#[derive(Clone, Deserialize, Debug)]
pub struct LocalFileSystemConfig {
    pub snapshots_path: String,
}

#[derive(Clone, Deserialize, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub snapshots_path: String,
}

impl LocalFileSystemConfig {
    pub fn new(snapshots_path: String) -> Self {
        Self { snapshots_path }
    }
}

#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    async fn delete_snapshot(&self, snapshot_name: &Path) -> CollectionResult<bool>;
}

#[async_trait]
impl SnapshotStorage for LocalFileSystemConfig {
    async fn delete_snapshot(&self, _snapshot_path: &Path) -> CollectionResult<bool> {
        unimplemented!()
    }
}

#[async_trait]
impl SnapshotStorage for S3Config {
    async fn delete_snapshot(&self, _snapshot_path: &Path) -> CollectionResult<bool> {
        unimplemented!()
    }
}
