use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, remove_dir_all};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use std::sync::Arc;

use sled::transaction::UnabortableTransactionError;
use sled::{Config, Db};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use collection::config::{CollectionConfig, CollectionParams};
use collection::operations::config_diff::DiffConfig;
use collection::operations::types::{
    PointRequest, RecommendRequest, Record, ScrollRequest, ScrollResult, SearchRequest,
    UpdateResult,
};
use collection::operations::CollectionUpdateOperations;
use collection::Collection;
use segment::types::ScoredPoint;

use crate::content_manager::collections_ops::{Checker, Collections};
use crate::content_manager::errors::StorageError;
use crate::content_manager::storage_ops::{
    AliasOperations, ChangeAliasesOperation, CreateAlias, CreateAliasOperation, CreateCollection,
    DeleteAlias, DeleteAliasOperation, RenameAlias, RenameAliasOperation, StorageOperations,
    UpdateCollection,
};
use crate::types::StorageConfig;
use collection::collection_manager::collection_managers::CollectionSearcher;
use collection::collection_manager::simple_collection_searcher::SimpleCollectionSearcher;
use std::ops::Deref;

/// Since sled is used for reading only during the initialization, large read cache is not required
#[allow(clippy::identity_op)]
const SLED_CACHE_SIZE: u64 = 1 * 1024 * 1024; // 1 mb

const COLLECTIONS_DIR: &str = "collections";

/// The main object of the service. It holds all objects, required for proper functioning.
/// In most cases only one `TableOfContent` is enough for service. It is created only once during
/// the launch of the service.
pub struct TableOfContent {
    collections: Arc<RwLock<Collections>>,
    storage_config: StorageConfig,
    search_runtime: Runtime,
    alias_persistence: Db,
    segment_searcher: Box<dyn CollectionSearcher + Sync + Send>,
}

impl TableOfContent {
    pub fn new(storage_config: &StorageConfig, search_runtime: Runtime) -> Self {
        let collections_path = Path::new(&storage_config.storage_path).join(&COLLECTIONS_DIR);

        create_dir_all(&collections_path).expect("Can't create Collections directory");

        let collection_paths =
            read_dir(&collections_path).expect("Can't read Collections directory");

        let mut collections: HashMap<String, Arc<Collection>> = Default::default();

        for entry in collection_paths {
            let collection_path = entry
                .expect("Can't access of one of the collection files")
                .path();
            let collection_name = collection_path
                .file_name()
                .expect("Can't resolve a filename of one of the collection files")
                .to_str()
                .expect("A filename of one of the collection files is not a valid UTF-8")
                .to_string();

            let collection = Collection::load(collection_name.clone(), &collection_path);

            collections.insert(collection_name, Arc::new(collection));
        }

        let alias_path = Path::new(&storage_config.storage_path).join("aliases.sled");

        let alias_persistence = Config::new()
            .cache_capacity(SLED_CACHE_SIZE)
            .path(alias_path)
            .open()
            .expect("Can't open database by the provided config");

        TableOfContent {
            collections: Arc::new(RwLock::new(collections)),
            storage_config: storage_config.clone(),
            search_runtime,
            alias_persistence,
            segment_searcher: Box::new(SimpleCollectionSearcher::new()),
        }
    }

    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        Path::new(&self.storage_config.storage_path)
            .join(&COLLECTIONS_DIR)
            .join(collection_name)
    }

    fn create_collection_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let path = self.get_collection_path(collection_name);

        create_dir_all(&path).map_err(|err| StorageError::ServiceError {
            description: format!(
                "Can't create directory for collection {}. Error: {}",
                collection_name, err
            ),
        })?;

        Ok(path)
    }

    /// Finds the original name of the collection
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection or alias to resolve
    ///
    /// # Result
    ///
    /// If the collection exists - return its name
    /// If alias exists - returns the original collection name
    /// If neither exists - returns [`StorageError`]
    async fn resolve_name(&self, collection_name: &str) -> Result<String, StorageError> {
        let alias_collection_name = self.alias_persistence.get(collection_name.as_bytes())?;

        let resolved_name = match alias_collection_name {
            None => collection_name.to_string(),
            Some(resolved_alias) => from_utf8(&resolved_alias).unwrap().to_string(),
        };
        self.collections
            .read()
            .await
            .validate_collection_exists(&resolved_name)
            .await?;
        Ok(resolved_name)
    }

    pub async fn create_collection(
        &self,
        collection_name: &str,
        operation: CreateCollection,
    ) -> Result<bool, StorageError> {
        let CreateCollection {
            vector_size,
            distance,
            hnsw_config: hnsw_config_diff,
            wal_config: wal_config_diff,
            optimizers_config: optimizers_config_diff,
        } = operation;

        self.collections
            .read()
            .await
            .validate_collection_not_exists(collection_name)
            .await?;

        let collection_path = self.create_collection_path(collection_name)?;

        let collection_params = CollectionParams {
            vector_size,
            distance,
        };
        let wal_config = match wal_config_diff {
            None => self.storage_config.wal.clone(),
            Some(diff) => diff.update(&self.storage_config.wal)?,
        };

        let optimizers_config = match optimizers_config_diff {
            None => self.storage_config.optimizers.clone(),
            Some(diff) => diff.update(&self.storage_config.optimizers)?,
        };

        let hnsw_config = match hnsw_config_diff {
            None => self.storage_config.hnsw_index,
            Some(diff) => diff.update(&self.storage_config.hnsw_index)?,
        };

        let collection = Collection::new(
            collection_name.to_string(),
            Path::new(&collection_path),
            &CollectionConfig {
                wal_config,
                params: collection_params,
                optimizer_config: optimizers_config,
                hnsw_config,
            },
        )?;

        let mut write_collections = self.collections.write().await;
        write_collections
            .validate_collection_not_exists(collection_name)
            .await?;
        write_collections.insert(collection_name.to_string(), Arc::new(collection));
        Ok(true)
    }

    pub async fn update_collection(
        &self,
        collection_name: &str,
        operation: UpdateCollection,
    ) -> Result<bool, StorageError> {
        match operation.optimizers_config {
            None => {}
            Some(new_optimizers_config) => {
                let collection = self.get_collection(collection_name).await?;
                collection
                    .update_optimizer_params(new_optimizers_config)
                    .await?
            }
        }
        Ok(true)
    }

    pub async fn delete_collection(&self, collection_name: &str) -> Result<bool, StorageError> {
        if let Some(removed) = self.collections.write().await.remove(collection_name) {
            tokio::task::spawn_blocking(move || drop(removed)).await?;
            let path = self.get_collection_path(collection_name);
            remove_dir_all(path).map_err(|err| StorageError::ServiceError {
                description: format!(
                    "Can't delete collection {}, error: {}",
                    collection_name, err
                ),
            })?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn update_aliases(
        &self,
        operation: ChangeAliasesOperation,
    ) -> Result<bool, StorageError> {
        // Lock all collections for alias changes
        // Prevent search on partially switched collections
        let collection_lock = self.collections.write().await;
        for action in operation.actions {
            match action {
                AliasOperations::CreateAlias(CreateAliasOperation {
                    create_alias:
                        CreateAlias {
                            collection_name,
                            alias_name,
                        },
                }) => {
                    collection_lock
                        .validate_collection_exists(&collection_name)
                        .await?;
                    collection_lock
                        .validate_collection_not_exists(&alias_name)
                        .await?;

                    self.alias_persistence
                        .insert(alias_name.as_bytes(), collection_name.as_bytes())?;
                }
                AliasOperations::DeleteAlias(DeleteAliasOperation {
                    delete_alias: DeleteAlias { alias_name },
                }) => {
                    self.alias_persistence.remove(alias_name.as_bytes())?;
                }
                AliasOperations::RenameAlias(RenameAliasOperation {
                    rename_alias:
                        RenameAlias {
                            old_alias_name,
                            new_alias_name,
                        },
                }) => {
                    if !self
                        .alias_persistence
                        .contains_key(old_alias_name.as_bytes())?
                    {
                        return Err(StorageError::NotFound {
                            description: format!("Alias {} does not exists!", old_alias_name),
                        });
                    }

                    let transaction_res = self.alias_persistence.transaction(|tx_db| {
                        let collection = tx_db
                            .remove(old_alias_name.as_bytes())?
                            .ok_or(UnabortableTransactionError::Conflict)?;

                        tx_db.insert(new_alias_name.as_bytes(), collection)?;
                        Ok(())
                    });
                    transaction_res?;
                }
            };
        }
        self.alias_persistence.flush()?;
        Ok(true)
    }

    pub async fn perform_collection_operation(
        &self,
        operation: StorageOperations,
    ) -> Result<bool, StorageError> {
        match operation {
            StorageOperations::CreateCollection(operation) => {
                self.create_collection(&operation.name, operation.create_collection)
                    .await
            }
            StorageOperations::UpdateCollection(operation) => {
                self.update_collection(&operation.name, operation.update_collection)
                    .await
            }
            StorageOperations::DeleteCollection(operation) => {
                self.delete_collection(&operation.0).await
            }
            StorageOperations::ChangeAliases(operation) => self.update_aliases(operation).await,
        }
    }

    pub async fn get_collection(
        &self,
        collection_name: &str,
    ) -> Result<Arc<Collection>, StorageError> {
        let read_collection = self.collections.read().await;
        let real_collection_name = self.resolve_name(collection_name).await?;
        // resolve_name already checked collection existence, unwrap is safe here
        Ok(read_collection.get(&real_collection_name).unwrap().clone())
    }

    /// Recommend points using positive and negative example from the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - for what collection do we recommend
    /// * `request` - [`RecommendRequest`]
    ///
    /// # Result
    ///
    /// Points with recommendation score
    pub async fn recommend(
        &self,
        collection_name: &str,
        request: RecommendRequest,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .recommend_by(
                request,
                self.segment_searcher.deref(),
                self.search_runtime.handle(),
            )
            .await
            .map_err(|err| err.into())
    }

    /// Search for the closest points using vector similarity with given restrictions defined
    /// in the request
    ///
    /// # Arguments
    ///
    /// * `collection_name` - in what collection do we search
    /// * `request` - [`SearchRequest`]
    ///
    /// # Result
    ///
    /// Points with search score
    pub async fn search(
        &self,
        collection_name: &str,
        request: SearchRequest,
    ) -> Result<Vec<ScoredPoint>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .search(
                request,
                self.segment_searcher.as_ref(),
                self.search_runtime.handle(),
            )
            .await
            .map_err(|err| err.into())
    }

    /// Return specific points by IDs
    ///
    /// # Arguments
    ///
    /// * `collection_name` - select from this collection
    /// * `request` - [`PointRequest`]
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn retrieve(
        &self,
        collection_name: &str,
        request: PointRequest,
    ) -> Result<Vec<Record>, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .retrieve(request, self.segment_searcher.as_ref())
            .await
            .map_err(|err| err.into())
    }

    /// List of all collections
    pub async fn all_collections(&self) -> Vec<String> {
        self.collections.read().await.keys().cloned().collect()
    }

    /// List of all aliases for a given collection
    pub fn collection_aliases(&self, collection_name: &str) -> Result<Vec<String>, StorageError> {
        let mut result = vec![];
        for pair in self.alias_persistence.iter() {
            let (alias_bt, target_collection_bt) = pair?;
            let alias = from_utf8(&alias_bt).unwrap().to_string();
            let target_collection = from_utf8(&target_collection_bt).unwrap();
            if collection_name == target_collection {
                result.push(alias);
            }
        }
        Ok(result)
    }

    /// Paginate over all stored points with given filtering conditions
    ///
    /// # Arguments
    ///
    /// * `collection_name` - which collection to use
    /// * `request` - [`ScrollRequest`]
    ///
    /// # Result
    ///
    /// List of points with specified information included
    pub async fn scroll(
        &self,
        collection_name: &str,
        request: ScrollRequest,
    ) -> Result<ScrollResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .scroll_by(request, self.segment_searcher.deref())
            .await
            .map_err(|err| err.into())
    }

    pub async fn update(
        &self,
        collection_name: &str,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> Result<UpdateResult, StorageError> {
        let collection = self.get_collection(collection_name).await?;
        collection
            .update(operation, wait)
            .await
            .map_err(|err| err.into())
    }
}
