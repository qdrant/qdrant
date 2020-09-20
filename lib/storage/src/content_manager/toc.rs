use std::sync::{Arc, RwLock};
use collection::collection::Collection;
use std::collections::HashMap;
use wal::WalOptions;
use tokio::runtime::Runtime;
use tokio::runtime;
use num_cpus;
use std::cmp::max;
use segment::types::SegmentConfig;
use std::path::{Path, PathBuf};
use std::fs::{create_dir_all, remove_dir_all};
use collection::collection_builder::optimizers_builder::build_optimizers;
use collection::collection_builder::collection_builder::build_collection;
use crate::content_manager::errors::StorageError;
use crate::content_manager::storage_ops::{AliasOperations, StorageOps};
use crate::types::StorageConfig;
use sled::Db;
use sled::transaction::UnabortableTransactionError;
use std::str::{from_utf8_unchecked, from_utf8};


struct TableOfContent {
    collections: Arc<RwLock<HashMap<String, Arc<Collection>>>>,
    storage_config: StorageConfig,
    search_runtime: Runtime,
    optimization_runtime: Runtime,
    alias_persistence: Db,
}


impl TableOfContent {
    pub fn new(
        collections: HashMap<String, Arc<Collection>>,
        storage_config: &StorageConfig,
    ) -> Self {
        let mut search_threads = storage_config.performance.max_search_threads;

        if search_threads == 0 {
            let num_cpu = num_cpus::get();
            search_threads = max(1, num_cpu - 1);
        }

        let search_runtime: Runtime = runtime::Builder::new()
            .threaded_scheduler()
            .max_threads(search_threads)
            .build().unwrap();

        let mut optimization_threads = storage_config.performance.max_optimize_threads;
        if optimization_threads == 0 {
            optimization_threads = 1;
        }

        let optimization_runtime: Runtime = runtime::Builder::new()
            .threaded_scheduler()
            .max_threads(optimization_threads)
            .build().unwrap();


        let alias_path = Path::new(&storage_config.storage_path)
            .join("aliases.sled");

        let alias_persistence = sled::open(alias_path.as_path()).unwrap();


        TableOfContent {
            collections: Arc::new(RwLock::new(collections)),
            storage_config: storage_config.clone(),
            search_runtime,
            optimization_runtime,
            alias_persistence,
        }
    }

    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        Path::new(&self.storage_config.storage_path)
            .join("collections")
            .join(collection_name)
    }

    fn create_collection_path(&self, collection_name: &str) -> Result<PathBuf, StorageError> {
        let path = self.get_collection_path(collection_name);

        create_dir_all(&path)
            .or_else(|err| Err(StorageError::ServiceError {
                description: format!("Can't create directory for collection {}. Error: {}", collection_name, err)
            }))?;

        Ok(path)
    }

    fn validate_collection_name(&self, collection_name: &str) -> Result<(), StorageError> {
        if self.collections.read().unwrap().contains_key(collection_name) {
            return Err(StorageError::BadInput {
                description: format!("Collection {} already exists!", collection_name)
            });
        }

        Ok(())
    }


    pub fn perform_collection_operation(&self, operation: StorageOps) -> Result<bool, StorageError> {
        match operation {
            StorageOps::CreateCollection {
                collection_name,
                dim,
                distance,
                index
            } => {
                self.validate_collection_name(&collection_name)?;

                let wal_options = WalOptions {
                    segment_capacity: self.storage_config.wal.wal_capacity_mb * 1024 * 1024,
                    segment_queue_len: self.storage_config.wal.wal_segments_ahead,
                };

                let collection_path = self.create_collection_path(&collection_name)?;


                let segment_config = SegmentConfig {
                    vector_size: dim,
                    index: index.unwrap_or(Default::default()),
                    distance,
                };

                let optimizers = build_optimizers(
                    &collection_path,
                    segment_config.clone(),
                    self.storage_config.optimizers.deleted_threshold,
                    self.storage_config.optimizers.vacuum_min_vector_number,
                    self.storage_config.optimizers.max_segment_number,
                );

                let segment = build_collection(
                    Path::new(&collection_path),
                    &wal_options,
                    &segment_config,
                    self.search_runtime.handle().clone(),
                    self.optimization_runtime.handle().clone(),
                    optimizers,
                )?;

                let mut write_collections = self.collections.write().unwrap();
                write_collections.insert(collection_name, Arc::new(segment));
                Ok(true)
            }
            StorageOps::DeleteCollection { collection_name } => {
                let removed = self.collections.write().unwrap().remove(&collection_name).is_some();
                if removed {
                    let path = self.get_collection_path(&collection_name);
                    remove_dir_all(path).or_else(
                        |err| Err(StorageError::ServiceError {
                            description: format!("Can't delete collection {}, error: {}", collection_name, err)
                        }))?;
                }
                Ok(removed)
            }
            StorageOps::ChangeAliases { actions } => {
                for action in actions {
                    match action {
                        AliasOperations::CreateAlias { collection_name, alias_name } => {
                            self.validate_collection_name(&collection_name)?;
                            self.alias_persistence.insert(alias_name.as_bytes(), collection_name.as_bytes())?;
                        }
                        AliasOperations::DeleteAlias { alias_name } => {
                            self.alias_persistence.remove(alias_name.as_bytes())?;
                        }
                        AliasOperations::RenameAlias { old_alias_name, new_alias_name } => {
                            if !self.alias_persistence.contains_key(old_alias_name.as_bytes())? {
                                return Err(StorageError::NotFound { description: format!("Alias {} does not exists!", old_alias_name) });
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
        }
    }

    fn resolve_name(&self, collection_name: String) -> Result<String, StorageError> {
        let alias_collection_name = self.alias_persistence
            .get(collection_name.as_bytes())?;

        let resolved_name = match alias_collection_name {
            None => collection_name,
            Some(resolved_alias) => {
                from_utf8(&resolved_alias).unwrap().to_string()
            }
        };

        self.validate_collection_name(&resolved_name)?;

        Ok(resolved_name)
    }

    pub fn get_collection(&self, collection_name: String) -> Result<Arc<Collection>, StorageError> {
        let read_collection = self.collections.read().unwrap();
        let real_collection_name = self.resolve_name(collection_name)?;
        Ok(read_collection.get(&real_collection_name).unwrap().clone())
    }
}