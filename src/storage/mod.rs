use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::error;

use super::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollectionOperation, UpdateCollectionOperation,
};
use super::content_manager::consensus::consensus_wal::ConsensusOpWal;
use super::content_manager::consensus_ops::ConsensusOperations;
use super::content_manager::graph::Graph;
use super::content_manager::shard_distribution::ShardDistribution;
use super::content_manager::SnapshotDescription;
use super::content_manager::TOPOLOGY_SNAPSHOT_FILENAME;
use super::optimizers::Optimizer;
use super::telemetry::CollectionTelemetry;
use super::wal::Wal;
use crate::collection::Collection;
use crate::config::storage::StorageConfig;
use crate::config::Config;
use crate::optimizers::OptimizerConfig;
use crate::shard::ShardConfig;
use crate::update_handler::UpdateHandler;

mod content_manager;
mod optimizers;
mod telemetry;
mod wal;

pub use content_manager::collection_meta_ops::CollectionMetaOperations;
pub use content_manager::consensus::ConsensusState;
pub use content_manager::consensus::ConsensusStateRef;
pub use content_manager::consensus::ConsensusStateSnapshot;
pub use content_manager::consensus::ConsensusStateSnapshotRef;
pub use content_manager::consensus_ops::ConsensusOperations;
pub use content_manager::shard_distribution::ShardDistribution;
pub use content_manager::SnapshotDescription;
pub use content_manager::TOPOLOGY_SNAPSHOT_FILENAME;
pub use optimizers::Optimizer;
pub use optimizers::OptimizerConfig;
pub use telemetry::CollectionTelemetry;
pub use wal::Wal;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Lock error: {0}")]
    Lock(#[from] parking_lot::lock_api::RwLockError),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Snapshot error: {0}")]
    Snapshot(String),

    #[error("Invalid snapshot state: {0}")]
    InvalidSnapshotState(String),

    #[error("Invalid snapshot version: {0}")]
    InvalidSnapshotVersion(String),

    #[error("Invalid snapshot data: {0}")]
    InvalidSnapshotData(String),

    #[error("Invalid collection update: {0}")]
    InvalidCollectionUpdate(String),

    #[error("Invalid collection create: {0}")]
    InvalidCollectionCreate(String),

    #[error("Invalid collection config: {0}")]
    InvalidCollectionConfig(String),

    #[error("Invalid shard distribution: {0}")]
    InvalidShardDistribution(String),

    #[error("Invalid consensus state: {0}")]
    InvalidConsensusState(String),

    #[error("Invalid collection state: {0}")]
    InvalidCollectionState(String),

    #[error("Invalid update handler state: {0}")]
    InvalidUpdateHandlerState(String),

    #[error("Invalid optimizer state: {0}")]
    InvalidOptimizerState(String),

    #[error("Invalid telemetry state: {0}")]
    InvalidTelemetryState(String),

    #[error("Invalid WAL state: {0}")]
    InvalidWalState(String),

    #[error("Invalid content manager state: {0}")]
    InvalidContentManagerState(String),

    #[error("Invalid storage state: {0}")]
    InvalidStorageState(String),

    #[error("Invalid config: {0}")]
    InvalidConfig(String),

    #[error("Invalid shard config: {0}")]
    InvalidShardConfig(String),

    #[error("Invalid optimizer config: {0}")]
    InvalidOptimizerConfig(String),

    #[error("Invalid storage config: {0}")]
    InvalidStorageConfig(String),

    #[error("Other error: {0}")]
    Other(String),
}

impl From<StorageError> for tonic::Status {
    fn from(err: StorageError) -> Self {
        tonic::Status::internal(format!("{:?}", err))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageState {
    pub collections: Vec<Collection>,
    pub update_handlers: Vec<Arc<RwLock<UpdateHandler>>>,
    pub optimizers: Vec<Arc<Optimizer>>,
    pub shard_distribution: ShardDistribution,
    pub consensus_state: Arc<RwLock<ConsensusState>>,
    pub collection_telemetries: Vec<Arc<CollectionTelemetry>>,
}

impl StorageState {
    pub fn new(
        collections: Vec<Collection>,
        update_handlers: Vec<Arc<RwLock<UpdateHandler>>>,
        optimizers: Vec<Arc<Optimizer>>,
        shard_distribution: ShardDistribution,
        consensus_state: Arc<RwLock<ConsensusState>>,
        collection_telemetries: Vec<Arc<CollectionTelemetry>>,
    ) -> Self {
        StorageState {
            collections,
            update_handlers,
            optimizers,
            shard_distribution,
            consensus_state,
            collection_telemetries,
        }
    }

    pub fn get_collection(&self, name: &str) -> Result<Arc<Collection>, StorageError> {
        self.collections
           .iter()
           .find(|c| c.name == name)
           .cloned()
           .ok_or_else(|| StorageError::CollectionNotFound(name.to_string()))
    }

    pub fn get_update_handler(
        &self,
        collection_name: &str,
    ) -> Result<Arc<RwLock<UpdateHandler>>, StorageError> {
        self.update_handlers
           .iter()
           .find(|handler| handler.read().collection.name == collection_name)
           .cloned()
           .ok_or_else(|| StorageError::CollectionNotFound(collection_name.to_string()))
    }

    pub fn get_optimizer(&self, collection_name: &str) -> Result<Arc<Optimizer>, StorageError> {
        self.optimizers
           .iter()
           .find(|optimizer| optimizer.collection.name == collection_name)
           .cloned()
           .ok_or_else(|| StorageError::CollectionNotFound(collection_name.to_string()))
    }

    pub fn get_collection_telemetry(
        &self,
        collection_name: &str,
    ) -> Result<Arc<CollectionTelemetry>, StorageError> {
        self.collection_telemetries
           .iter()
           .find(|telemetry| telemetry.collection_name == collection_name)
           .cloned()
           .ok_or_else(|| StorageError::CollectionNotFound(collection_name.to_string()))
    }
}

pub struct Storage {
    pub config: StorageConfig,
    pub state: Arc<RwLock<StorageState>>,
    pub content_manager: Arc<content_manager::ContentManager>,
}

impl Storage {
    pub async fn open(config: StorageConfig) -> Result<Self, StorageError> {
        let storage_path = Path::new(&config.storage_path);
        fs::create_dir_all(storage_path)?;

        let consensus_wal_path = storage_path.join("consensus_wal");
        let consensus_wal = ConsensusOpWal::new(&consensus_wal_path);

        let mut snapshot_description = None;
        let mut shard_distribution = ShardDistribution::default();
        let mut collections = Vec::new();
        let mut update_handlers = Vec::new();
        let mut optimizers = Vec::new();
        let mut collection_telemetries = Vec::new();

        let topology_snapshot_path = storage_path.join(TOPOLOGY_SNAPSHOT_FILENAME);
        if topology_snapshot_path.exists() {
            let topology_snapshot_data = fs::read(topology_snapshot_path)?;
            let topology_snapshot: SnapshotDescription =
                serde_json::from_slice(&topology_snapshot_data)?;
            snapshot_description = Some(topology_snapshot);
        }

        let collection_snapshots = content_manager::list_collection_snapshots(storage_path)?;
        for collection_snapshot in collection_snapshots {
            let collection_path = storage_path.join(&collection_snapshot.name);
            let collection_config = ShardConfig::load(&collection_path)?;
            let collection_wal = Wal::open(collection_path.join("wal"))?;
            let collection_meta_ops = CollectionMetaOperations::load(&collection_path)?;
            let collection_telemetry = CollectionTelemetry::load(&collection_path)?;
            let collection = Collection::new(
                collection_snapshot.name.clone(),
                collection_config,
                collection_wal,
                collection_meta_ops,
            );
            let update_handler = UpdateHandler::new(collection.clone());
            let optimizer = Optimizer::new(collection.clone(), OptimizerConfig::default());

            collections.push(collection);
            update_handlers.push(Arc::new(RwLock::new(update_handler)));
            optimizers.push(Arc::new(optimizer));
            collection_telemetries.push(Arc::new(collection_telemetry));

            shard_distribution.add_collection(collection_snapshot.name, collection_snapshot.shards);
        }

        let consensus_state = Arc::new(RwLock::new(ConsensusState::new(
            consensus_wal,
            shard_distribution.clone(),
        )));

        let state = Arc::new(RwLock::new(StorageState::new(
            collections,
            update_handlers,
            optimizers,
            shard_distribution,
            consensus_state.clone(),
            collection_telemetries,
        )));

        let content_manager = Arc::new(content_manager::ContentManager::new(
            storage_path.to_path_buf(),
            state.clone(),
            consensus_state,
        ));

        let storage = Storage {
            config,
            state,
            content_manager,
        };

        if let Some(snapshot_description) = snapshot_description {
            storage.apply_snapshot(&snapshot_description).await?;
        }

        Ok(storage)
    }

    pub async fn create_collection(
        &self,
        operation: CreateCollectionOperation,
    ) -> Result<(), StorageError> {
        let collection_name = operation.name.clone();
        let collection_path = Path::new(&self.config.storage_path).join(&collection_name);
        fs::create_dir_all(&collection_path)?;

        let shard_config = ShardConfig::new(
            operation.vector_size,
            operation.index,
            operation.payload_storage,
        );
        shard_config.save(&collection_path)?;

        let wal_path = collection_path.join("wal");
        Wal::create(&wal_path)?;

        let collection_wal = Wal::open(wal_path)?;
        let collection_meta_ops = CollectionMetaOperations::new();
        let collection_telemetry = CollectionTelemetry::new(&collection_path)?;
        let collection = Collection::new(
            collection_name.clone(),
            shard_config,
            collection_wal,
            collection_meta_ops,
        );
        let update_handler = UpdateHandler::new(collection.clone());
        let optimizer = Optimizer::new(collection.clone(), OptimizerConfig::default());

        let mut state = self.state.write();
        state.collections.push(collection);
        state.update_handlers.push(Arc::new(RwLock::new(update_handler)));
        state.optimizers.push(Arc::new(optimizer));
        state.collection_telemetries.push(Arc::new(collection_telemetry));
        state.shard_distribution.add_collection(collection_name, operation.shards);

        self.content_manager
           .apply_create_collection(operation)
           .await?;

        Ok(())
    }

    pub async fn update_collection(
        &self,
        operation: UpdateCollectionOperation,
    ) -> Result<(), StorageError> {
        let collection_name = operation.name.clone();
        let collection_path = Path::new(&self.config.storage_path).join(&collection_name);

        let mut state = self.state.write();
        let collection = state.get_collection(&collection_name)?;
        let shard_config = collection.config;

        if let Some(new_index) = operation.new_index {
            shard_config.index = new_index;
        }

        if let Some(new_payload_storage) = operation.new_payload_storage {
            shard_config.payload_storage = new_payload_storage;
        }

        shard_config.save(&collection_path)?;

        self.content_manager
           .apply_update_collection(operation)
           .await?;

        Ok(())
    }

    pub async fn apply_snapshot(
        &self,
        snapshot_description: &SnapshotDescription,
    ) -> Result<(), StorageError> {
        let snapshot_path = Path::new(&self.config.storage_path).join(&snapshot_description.name);
        let snapshot_data = fs::read(snapshot_path)?;
        let snapshot: ConsensusStateSnapshot = serde_json::from_slice(&snapshot_data)?;

        let mut consensus_state = self.state.write().consensus_state.write();
        consensus_state.apply_snapshot(snapshot);

        let topology_snapshot_path = Path::new(&self.config.storage_path).join(TOPOLOGY_SNAPSHOT_FILENAME);
        let topology_snapshot_data = serde_json::to_vec(snapshot_description)?;
        let mut topology_snapshot_file = File::create(topology_snapshot_path).await?;
        topology_snapshot_file.write_all(&topology_snapshot_data).await?;

        Ok(())
    }

    pub async fn create_snapshot(&self) -> Result<SnapshotDescription, StorageError> {
        let snapshot_name = format!("snapshot-{}", chrono::Utc::now().timestamp_millis());
        let snapshot_path = Path::new(&self.config.storage_path).join(&snapshot_name);
        let consensus_state = self.state.read().consensus_state.read();
        let snapshot_data = serde_json::to_vec(&*consensus_state)?;
        fs::write(snapshot_path, snapshot_data)?;

        let snapshot_description = SnapshotDescription {
            name: snapshot_name,
            created_at: chrono::Utc::now(),
        };

        let topology_snapshot_path = Path::new(&self.config.storage_path).join(TOPOLOGY_SNAPSHOT_FILENAME);
        let topology_snapshot_data = serde_json::to_vec(&snapshot_description)?;
        let mut topology_snapshot_file = File::create(topology_snapshot_path).await?;
        topology_snapshot_file.write_all(&topology_snapshot_data).await?;

        Ok(snapshot_description)
    }

    pub async fn delete_snapshot(&self, snapshot_name: &str) -> Result<(), StorageError> {
        let snapshot_path = Path::new(&self.config.storage_path).join(snapshot_name);
        fs::remove_file(snapshot_path)?;

        let topology_snapshot_path = Path::new(&self.config.storage_path).join(TOPOLOGY_SNAPSHOT_FILENAME);
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
        let storage_path = Path::new(&self.config.storage_path);
        let mut snapshots = Vec::new();

        for entry in fs::read_dir(storage_path)? {
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
}

impl Drop for Storage {
    fn drop(&mut self) {
        if let Err(e) = self.content_manager.shutdown() {
            error!("Failed to gracefully shutdown content manager: {}", e);
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
    use crate::config::storage::StorageConfig;
    use crate::config::Config;
    use crate::shard::ShardConfig;

    #[tokio::test]
    async fn test_storage_open() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("storage");
        let storage_config = StorageConfig {
            storage_path: storage_path.to_str().unwrap().to_string(),
        };

        // Open storage with empty path
        let storage = Storage::open(storage_config.clone()).await.unwrap();
        assert_eq!(storage.state.read().collections.len(), 0);
        assert_eq!(storage.state.read().update_handlers.len(), 0);
        assert_eq!(storage.state.read().optimizers.len(), 0);
        assert_eq!(storage.state.read().shard_distribution.collections().len(), 0);
        assert_eq!(storage.state.read().collection_telemetries.len(), 0);

        // Create collection
        let collection_name = "test_collection".to_string();
        let create_collection_operation = CreateCollectionOperation {
            name: collection_name.clone(),
            vector_size: 100,
            index: None,
            shards: 1,
            payload_storage: None,
        };
        storage.create_collection(create_collection_operation).await.unwrap();

        // Check collection created
        let state = storage.state.read();
        assert_eq!(state.collections.len(), 1);
        assert_eq!(state.update_handlers.len(), 1);
        assert_eq!(state.optimizers.len(), 1);
        assert_eq!(state.shard_distribution.collections().len(), 1);
        assert_eq!(state.collection_telemetries.len(), 1);

        let collection = state.get_collection(&collection_name).unwrap();
        assert_eq!(collection.name, collection_name);
        assert_eq!(collection.config.vector_size, 100);
        assert_eq!(collection.config.index, None);
        assert_eq!(collection.config.payload_storage, None);

        let update_handler = state.get_update_handler(&collection_name).unwrap();
        assert_eq!(update_handler.read().collection.name, collection_name);

        let optimizer = state.get_optimizer(&collection_name).unwrap();
        assert_eq!(optimizer.collection.name, collection_name);

        let collection_telemetry = state.get_collection_telemetry(&collection_name).unwrap();
        assert_eq!(collection_telemetry.collection_name, collection_name);

        // Update collection
        let update_collection_operation = UpdateCollectionOperation {
            name: collection_name.clone(),
            new_index: Some(VectorParams::new("hnsw", 128, 64, 10, 100)),
            new_payload_storage: None,
        };
        storage.update_collection(update_collection_operation).await.unwrap();

        // Check collection updated
        let updated_collection = state.get_collection(&collection_name).unwrap();
        assert_eq!(updated_collection.config.index.unwrap().vector_size, 128);
        assert_eq!(updated_collection.config.index.unwrap().distance, "euclidean");
        assert_eq!(updated_collection.config.index.unwrap().hnsw_ef_construct, 64);
        assert_eq!(updated_collection.config.index.unwrap().hnsw_full_scan_threshold, 10);
        assert_eq!(updated_collection.config.index.unwrap().hnsw_max_links, 100);

        // Create snapshot
        let snapshot_description = storage.create_snapshot().await.unwrap();
        assert!(snapshot_description.name.starts_with("snapshot-"));
        assert!(snapshot_description.created_at <= chrono::Utc::now());

        // Check snapshot created
        let snapshots = storage.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, snapshot_description.name);
        assert_eq!(snapshots[0].created_at, snapshot_description.created_at);

        // Delete snapshot
        storage.delete_snapshot(&snapshot_description.name).await.unwrap();

        // Check snapshot deleted
        let snapshots = storage.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 0);

        // Check storage state persisted
        drop(storage);
        let reopened_storage = Storage::open(storage_config).await.unwrap();
        let reopened_state = reopened_storage.state.read();
        assert_eq!(reopened_state.collections.len(), 1);
        assert_eq!(reopened_state.update_handlers.len(), 1);
        assert_eq!(reopened_state.optimizers.len(), 1);
        assert_eq!(reopened_state.shard_distribution.collections().len(), 1);
        assert_eq!(reopened_state.collection_telemetries.len(), 1);

        let reopened_collection = reopened_state.get_collection(&collection_name).unwrap();
        assert_eq!(reopened_collection.name, collection_name);
        assert_eq!(reopened_collection.config.vector_size, 100);
        assert_eq!(reopened_collection.config.index.unwrap().vector_size, 128);
        assert_eq!(reopened_collection.config.index.unwrap().distance, "euclidean");
        assert_eq!(reopened_collection.config.index.unwrap().hnsw_ef_construct, 64);
        assert_eq!(reopened_collection.config.index.unwrap().hnsw_full_scan_threshold, 10);
        assert_eq!(reopened_collection.config.index.unwrap().hnsw_max_links, 100);
    }

    #[test]
    fn test_shard_config_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let shard_config_path = temp_dir.path().join("shard_config.json");

        let shard_config = ShardConfig {
            vector_size: 100,
            index: Some(VectorParams::new("hnsw", 128, 64, 10, 100)),
            payload_storage: None,
        };

        // Save shard config
        shard_config.save(&shard_config_path).unwrap();

        // Check shard config file created
        assert!(shard_config_path.exists());

        // Load shard config
        let loaded_shard_config = ShardConfig::load(&shard_config_path).unwrap();

        // Check loaded shard config
        assert_eq!(loaded_shard_config.vector_size, 100);
        assert_eq!(loaded_shard_config.index.unwrap().vector_size, 128);
        assert_eq!(loaded_shard_config.index.unwrap().distance, "euclidean");
        assert_eq!(loaded_shard_config.index.unwrap().hnsw_ef_construct, 64);
        assert_eq!(loaded_shard_config.index.unwrap().hnsw_full_scan_threshold, 10);
        assert_eq!(loaded_shard_config.index.unwrap().hnsw_max_links, 100);
    }

    #[test]
    fn test_collection_telemetry_save_load() {
        let temp_dir = TempDir::new().unwrap();
        let collection_path = temp_dir.path().join("collection");
        fs::create_dir_all(&collection_path).unwrap();

        let collection_telemetry = CollectionTelemetry::new(&collection_path).unwrap();

        // Save collection telemetry
        collection_telemetry.save().unwrap();

        // Check collection telemetry file created
        let collection_telemetry_path = collection_path.join("telemetry.json");
        assert!(collection_telemetry_path.exists());

        // Load collection telemetry
        let loaded_collection_telemetry = CollectionTelemetry::load(&collection_path).unwrap();

        // Check loaded collection telemetry
        assert_eq!(loaded_collection_telemetry.collection_name, collection_path.file_name().unwrap().to_str().unwrap());
    }
}
