use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::Status;

use super::consensus_ops::ConsensusOperations;
use super::shard_distribution::ShardDistribution;
use super::StorageError;
use crate::collection::Collection;
use crate::collection::operations::types::VectorParams;
use crate::config::collection::CollectionConfig;
use crate::shard::ShardConfig;

pub type ConsensusStateRef = Arc<RwLock<ConsensusState>>;
pub type ConsensusStateSnapshotRef = Arc<RwLock<ConsensusStateSnapshot>>;

#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub wal: super::wal::SerdeWal<ConsensusOperations>,
    pub shard_distribution: ShardDistribution,
}

impl ConsensusState {
    pub fn new(wal: super::wal::SerdeWal<ConsensusOperations>, shard_distribution: ShardDistribution) -> Self {
        ConsensusState {
            wal,
            shard_distribution,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: ConsensusStateSnapshot) {
        self.shard_distribution = snapshot.shard_distribution;
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsensusStateSnapshot {
    pub shard_distribution: ShardDistribution,
}

pub struct Consensus {
    state: ConsensusStateRef,
}

impl Consensus {
    pub fn new(state: ConsensusStateRef) -> Self {
        Consensus {
            state,
        }
    }

    pub async fn apply_create_collection(&mut self, operation: super::collection_meta_ops::CreateCollectionOperation) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        state.shard_distribution.add_collection(operation.name.clone(), operation.shards);
        state.wal.write(&ConsensusOperations::CreateCollection(operation)).await?;
        Ok(())
    }

    pub async fn apply_update_collection(&mut self, operation: super::collection_meta_ops::UpdateCollectionOperation) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        state.wal.write(&ConsensusOperations::UpdateCollection(operation)).await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        state.wal.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;
    use crate::storage::wal::Wal;

    #[tokio::test]
    async fn test_consensus_apply_create_collection() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("wal");
        let wal = Wal::create(&wal_path).unwrap();
        let shard_distribution = ShardDistribution::default();
        let state = Arc::new(RwLock::new(ConsensusState::new(wal, shard_distribution)));
        let mut consensus = Consensus::new(state.clone());

        let create_operation = super::collection_meta_ops::CreateCollectionOperation {
            name: "test_collection".to_string(),
            vector_size: 100,
            index: None,
            shards: 1,
            payload_storage: None,
        };
        consensus.apply_create_collection(create_operation).await.unwrap();

        let state = state.read().await;
        assert_eq!(state.shard_distribution.collections().len(), 1);
        assert_eq!(state.shard_distribution.collections()["test_collection"], 1);
    }

    #[tokio::test]
    async fn test_consensus_apply_update_collection() {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("wal");
        let wal = Wal::create(&wal_path).unwrap();
        let shard_distribution = ShardDistribution::default();
        let state = Arc::new(RwLock::new(ConsensusState::new(wal, shard_distribution)));
        let mut consensus = Consensus::new(state.clone());

        let update_operation = super::collection_meta_ops::UpdateCollectionOperation {
            name: "test_collection".to_string(),
            new_index: Some(VectorParams::new("hnsw", 128, 64, 10, 100)),
            new_payload_storage: None,
        };
        consensus.apply_update_collection(update_operation).await.unwrap();

        let state = state.read().await;
        // Check that the WAL contains the update operation
        let entries = state.wal.read_all(true).await.unwrap();
        assert_eq!(entries.len(), 1);
        match &entries[0].1 {
            ConsensusOperations::UpdateCollection(op) => {
                assert_eq!(op.name, "test_collection");
                assert_eq!(op.new_index.unwrap().vector_size, 128);
                assert_eq!(op.new_index.unwrap().distance, "euclidean");
                assert_eq!(op.new_index.unwrap().hnsw_ef_construct, 64);
                assert_eq!(op.new_index.unwrap().hnsw_full_scan_threshold, 10);
                assert_eq!(op.new_index.unwrap().hnsw_max_links, 100);
            }
            _ => panic!("Unexpected operation in WAL"),
        }
    }
}
