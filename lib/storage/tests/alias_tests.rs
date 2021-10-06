use collection::collection_builder::optimizers_builder::OptimizersConfig;
use storage::content_manager::toc::TableOfContent;
use storage::types::{PerformanceConfig, StorageConfig};
use tempdir::TempDir;
use tokio::runtime::Runtime;

#[cfg(test)]
mod tests {
    use super::*;
    use segment::types::Distance;
    use storage::content_manager::storage_ops::{AliasOperations, StorageOperations};

    #[test]
    fn test_alias_operation() {
        let storage_dir = TempDir::new("storage").unwrap();

        let config = StorageConfig {
            storage_path: storage_dir.path().to_str().unwrap().to_string(),
            optimizers: OptimizersConfig {
                deleted_threshold: 0.5,
                vacuum_min_vector_number: 100,
                max_segment_number: 2,
                memmap_threshold: 100,
                indexing_threshold: 100,
                payload_indexing_threshold: 100,
                flush_interval_sec: 2,
            },
            wal: Default::default(),
            performance: PerformanceConfig {
                max_search_threads: 1,
            },
            hnsw_index: Default::default(),
        };

        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();

        let toc = TableOfContent::new(&config, runtime);

        handle
            .block_on(
                toc.perform_collection_operation(StorageOperations::CreateCollection {
                    name: "test".to_string(),
                    vector_size: 10,
                    distance: Distance::Cosine,
                    hnsw_config: None,
                    wal_config: None,
                    optimizers_config: None,
                }),
            )
            .unwrap();

        handle
            .block_on(
                toc.perform_collection_operation(StorageOperations::ChangeAliases {
                    actions: vec![AliasOperations::CreateAlias {
                        collection_name: "test".to_string(),
                        alias_name: "test_alias".to_string(),
                    }],
                }),
            )
            .unwrap();

        handle
            .block_on(
                toc.perform_collection_operation(StorageOperations::ChangeAliases {
                    actions: vec![
                        AliasOperations::CreateAlias {
                            collection_name: "test".to_string(),
                            alias_name: "test_alias2".to_string(),
                        },
                        AliasOperations::DeleteAlias {
                            alias_name: "test_alias".to_string(),
                        },
                        AliasOperations::RenameAlias {
                            old_alias_name: "test_alias2".to_string(),
                            new_alias_name: "test_alias3".to_string(),
                        },
                    ],
                }),
            )
            .unwrap();

        handle.block_on(toc.get_collection(&"test_alias3")).unwrap();
    }
}
