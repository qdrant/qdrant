#[cfg(all(test))]
mod tests {
    use collection::optimizers_builder::OptimizersConfig;
    use segment::types::Distance;
    use storage::{
        content_manager::{
            collection_meta_ops::{
                ChangeAliasesOperation, CollectionMetaOperations, CreateAlias, CreateCollection,
                CreateCollectionOperation, DeleteAlias, RenameAlias,
            },
            toc::TableOfContent,
        },
        types::{PerformanceConfig, StorageConfig},
    };
    use tempdir::TempDir;
    use tokio::runtime::Runtime;

    #[test]
    fn test_alias_operation() {
        let storage_dir = TempDir::new("storage").unwrap();

        let config = StorageConfig {
            storage_path: storage_dir.path().to_str().unwrap().to_string(),
            on_disk_payload: false,
            optimizers: OptimizersConfig {
                deleted_threshold: 0.5,
                vacuum_min_vector_number: 100,
                default_segment_number: 2,
                max_segment_size: 100_000,
                memmap_threshold: 100,
                indexing_threshold: 100,
                payload_indexing_threshold: 100,
                flush_interval_sec: 2,
                max_optimization_threads: 2,
            },
            wal: Default::default(),
            performance: PerformanceConfig {
                max_search_threads: 1,
            },
            hnsw_index: Default::default(),
        };

        let runtime = Runtime::new().unwrap();
        let handle = runtime.handle().clone();

        let toc = TableOfContent::new(&config, runtime, None);

        handle
            .block_on(toc.submit_collection_operation(
                CollectionMetaOperations::CreateCollection(CreateCollectionOperation {
                    collection_name: "test".to_string(),
                    create_collection: CreateCollection {
                        vector_size: 10,
                        distance: Distance::Cosine,
                        hnsw_config: None,
                        wal_config: None,
                        optimizers_config: None,
                        shard_number: 1,
                        on_disk_payload: None,
                    },
                }),
                None,
            ))
            .unwrap();

        handle
            .block_on(toc.submit_collection_operation(
                CollectionMetaOperations::ChangeAliases(ChangeAliasesOperation {
                    actions: vec![CreateAlias {
                            collection_name: "test".to_string(),
                            alias_name: "test_alias".to_string(),
                        }
                        .into()],
                }),
                None,
            ))
            .unwrap();

        handle
            .block_on(toc.submit_collection_operation(
                CollectionMetaOperations::ChangeAliases(ChangeAliasesOperation {
                    actions: vec![
                            CreateAlias {
                                collection_name: "test".to_string(),
                                alias_name: "test_alias2".to_string(),
                            }
                            .into(),
                            DeleteAlias {
                                alias_name: "test_alias".to_string(),
                            }
                            .into(),
                            RenameAlias {
                                old_alias_name: "test_alias2".to_string(),
                                new_alias_name: "test_alias3".to_string(),
                            }
                            .into(),
                        ],
                }),
                None,
            ))
            .unwrap();

        handle.block_on(toc.get_collection("test_alias3")).unwrap();
    }
}
