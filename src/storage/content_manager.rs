use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::error;

use super::collection_meta_ops::{CollectionMetaOperations, CreateCollectionOperation, UpdateCollectionOperation};
use super::consensus::Consensus;
use super::consensus_ops::ConsensusOperations;
use super::shard_distribution::ShardDistribution;
use super::SnapshotDescription;
use super::StorageError;
use super::TOPOLOGY_SNAPSHOT_FILENAME;
use crate::collection::Collection;
use crate::shard::ShardConfig;

pub struct ContentManager {
    storage_path: PathBuf,
    state: Arc<tokio::sync::RwLock<super::StorageState>>,
    consensus: Arc<tokio::sync::RwLock<Consensus>>,
}

impl ContentManager {
    pub fn new(
        storage_path: PathBuf,
        state: Arc<tokio::sync::RwLock<super::StorageState>>,
        consensus_state: Arc<tokio::sync::RwLock<super::ConsensusState>>,
    ) -> Self {
        let consensus = Arc::new(tokio::sync::RwLock::new(Consensus::new(consensus_state)));
        ContentManager {
            storage_path,
            state,
            consensus,
        }
    }

    pub async fn apply_create_collection(
        &self,
        operation: CreateCollectionOperation,
    ) -> Result<(), StorageError> {
        let mut consensus_guard = self.consensus.write().await;
        consensus_guard.apply_create_collection(operation).await?;
        Ok(())
    }

    pub async fn apply_update_collection(
        &self,
        operation: UpdateCollectionOperation,
    ) -> Result<(), StorageError> {
        let mut consensus_guard = self.consensus.write().await;
        consensus_guard.apply_update_collection(operation).await?;
        Ok(())
    }

    pub async fn apply_snapshot(
        &self,
        snapshot_description: &SnapshotDescription,
    ) -> Result<(), StorageError> {
        let snapshot_path = self.storage_path.join(&snapshot_description.name);
        let snapshot_data = fs::read(snapshot_path)?;
        let snapshot: super::ConsensusStateSnapshot = serde_json::from_slice(&snapshot_data)?;

        let mut consensus_state = self.state.read().await.consensus_state.write().await;
        consensus_state.apply_snapshot(snapshot);

        let topology_snapshot_path = self.storage_path.join(TOPOLOGY_SNAPSHOT_FILENAME);
        let topology_snapshot_data = serde_json::to_vec(snapshot_description)?;
        let mut topology_snapshot_file = File::create(topology_snapshot_path).await?;
        topology_snapshot_file.write_all(&topology_snapshot_data).await?;

        Ok(())
    }

    pub async fn create_snapshot(&self) -> Result<SnapshotDescription, StorageError> {
        let snapshot_name = format!("snapshot-{}", chrono::Utc::now().timestamp_millis());
        let snapshot_path = self.storage_path.join(&snapshot_name);
        let consensus_state = self.state.read().await.consensus_state.read().await;
        let snapshot_data = serde_json::to_vec(&*consensus_state)?;
        fs::write(snapshot_path, snapshot_data)?;

        let snapshot_description = SnapshotDescription {
            name: snapshot_name,
            created_at: chrono::Utc::now(),
        };

        let topology_snapshot_path = self.storage_path.join(TOPOLOGY_SNAPSHOT_FILENAME);
        let topology_snapshot_data = serde_json::to_vec(&snapshot_description)?;
        let mut topology_snapshot_file = File::create(topology_snapshot_path).await?;
        topology_snapshot_file.write_all(&topology_snapshot_data).await?;

        Ok(snapshot_description)
    }

    pub async fn delete_snapshot(&self, snapshot_name: &str) -> Result<(), StorageError> {
        let snapshot_path = self.storage_path.join(snapshot_name);
        fs::remove_file(snapshot_path)?;

        let topology_snapshot_path = self.storage_path.join(TOPOLOGY_SNAPSHOT_FILENAME);
        if topology_snapshot_path.exists() {
            let topology_snapshot_data = fs::read(topology_snapshot_path)?;
            let mut topology_snapshot: SnapshotDescription = serde_json::from_slice(&topology_snapshot_data)?;
            if topology_snapshot.name == snapshot_name {
                fs::remove_file(topology_snapshot_path)?;
            }
        }

        Ok(())
    }

    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotDescription>, StorageError> {
        let mut snapshots = Vec::new();

        for entry in fs::read_dir(&self.storage_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().map(|ext| ext == "json").unwrap_or(false) {
                let snapshot_data = fs::read(path)?;
                let snapshot: SnapshotDescription = serde_json::from_slice(&snapshot_data)?;
                snapshots.push(snapshot);
            }
        }

        Ok(snapshots)
    }

    pub async fn shutdown(&self) -> Result<(), StorageError> {
        let mut consensus_guard = self.consensus.write().await;
        consensus_guard.shutdown().await?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CollectionSnapshot {
    pub name: String,
    pub shards: u32,
}

fn list_collection_snapshots(storage_path: &Path) -> Result<Vec<CollectionSnapshot>, StorageError> {
    let mut snapshots = Vec::new();

    for entry in fs::read_dir(storage_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let collection_name = path.file_name().unwrap().to_str().unwrap().to_string();
            let shard_config = ShardConfig::load(&path)?;
            snapshots.push(CollectionSnapshot {
                name: collection_name,
                shards: shard_config.shards,
            });
        }
    }

    Ok(snapshots)
}

#[derive(Debug, Error)]
pub enum ContentManagerError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Invalid collection update: {0}")]
    InvalidCollectionUpdate(String),

    #[error("Invalid collection create: {0}")]
    InvalidCollectionCreate(String),

    #[error("Invalid shard distribution: {0}")]
    InvalidShardDistribution(String),

    #[error("Invalid consensus state: {0}")]
    InvalidConsensusState(String),

    #[error("Other error: {0}")]
    Other(String),
}

impl From<ContentManagerError> for tonic::Status {
    fn from(err: ContentManagerError) -> Self {
        tonic::Status::internal(format!("{:?}", err))
    }
}

impl From<ContentManagerError> for StorageError {
    fn from(err: ContentManagerError) -> Self {
        match err {
            ContentManagerError::Io(e) => StorageError::Io(e),
            ContentManagerError::Json(e) => StorageError::Json(e),
            ContentManagerError::CollectionNotFound(e) => StorageError::CollectionNotFound(e),
            ContentManagerError::InvalidCollectionUpdate(e) => StorageError::InvalidCollectionUpdate(e),
            ContentManagerError::InvalidCollectionCreate(e) => StorageError::InvalidCollectionCreate(e),
            ContentManagerError::InvalidShardDistribution(e) => StorageError::InvalidShardDistribution(e),
            ContentManagerError::InvalidConsensusState(e) => StorageError::InvalidConsensusState(e),
            ContentManagerError::Other(e) => StorageError::Other(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;
    use crate::collection::operations::types::VectorParams;
    use crate::shard::ShardConfig;

    #[test]
    fn test_list_collection_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path();

        // Create collection directories
        let collection1_path = storage_path.join("collection1");
        let collection2_path = storage_path.join("collection2");
        fs::create_dir_all(&collection1_path).unwrap();
        fs::create_dir_all(&collection2_path).unwrap();

        // Save shard configs
        let shard_config1 = ShardConfig {
            vector_size: 100,
            index: Some(VectorParams::new("hnsw", 128, 64, 10, 100)),
            payload_storage: None,
        };
        shard_config1.save(&collection1_path).unwrap();

        let shard_config2 = ShardConfig {
            vector_size: 200,
            index: Some(VectorParams::new("hnsw", 256, 128, 20, 200)),
            payload_storage: None,
        };
        shard_config2.save(&collection2_path).unwrap();

        // List collection snapshots
        let snapshots = list_collection_snapshots(storage_path).unwrap();
        assert_eq!(snapshots.len(), 2);

        assert_eq!(snapshots[0].name, "collection1");
        assert_eq!(snapshots[0].shards, 1);

        assert_eq!(snapshots[1].name, "collection2");
        assert_eq!(snapshots[1].shards, 1);
    }
}
