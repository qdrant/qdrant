pub mod download;
pub mod recover;

use std::collections::HashMap;
use std::path::PathBuf;

use collection::common::sha_256::hash_file;
use serde::{Deserialize, Serialize};
use snapshot_manager::error::SnapshotManagerError;
use snapshot_manager::SnapshotDescription;
use tar::Builder as TarBuilder;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;

use super::errors::StorageError;
use crate::content_manager::toc::FULL_SNAPSHOT_FILE_NAME;
use crate::dispatcher::Dispatcher;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// Map collection name to snapshot file name
    pub collections_mapping: HashMap<String, String>,
    /// Aliases for collections `<alias>:<collection_name>`
    #[serde(default)]
    pub collections_aliases: HashMap<String, String>,
}

pub async fn do_create_full_snapshot(
    dispatcher: &Dispatcher,
    wait: bool,
) -> Result<Option<SnapshotDescription>, StorageError> {
    let dispatcher = dispatcher.clone();
    let task = tokio::spawn(async move { _do_create_full_snapshot(&dispatcher).await });
    if wait {
        Ok(Some(task.await??))
    } else {
        Ok(None)
    }
}

async fn _do_create_full_snapshot(
    dispatcher: &Dispatcher,
) -> Result<SnapshotDescription, StorageError> {
    let dispatcher = dispatcher.clone();

    let base: PathBuf = dispatcher.snapshots_temp_path()?;

    let all_collections = dispatcher.all_collections().await;
    let mut created_snapshots: Vec<(String, TempPath, TempPath)> = vec![];
    for collection_name in &all_collections {
        let (_, snapshot_file, checksum_file) =
            dispatcher.create_temp_snapshot(collection_name).await?;
        created_snapshots.push((collection_name.to_owned(), snapshot_file, checksum_file));
    }
    let current_time = chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S").to_string();

    let snapshot_name = format!("{FULL_SNAPSHOT_FILE_NAME}-{current_time}.snapshot");

    let collection_name_to_snapshot_path: HashMap<_, _> = created_snapshots
        .iter()
        .map(|x| (x.0.clone(), x.1.to_path_buf().to_string_lossy().to_string()))
        .collect();

    let mut alias_mapping: HashMap<String, String> = Default::default();
    for collection_name in &all_collections {
        for alias in dispatcher.collection_aliases(collection_name).await? {
            alias_mapping.insert(alias.to_string(), collection_name.to_string());
        }
    }

    let config_path = base.join(format!("config-{current_time}.json"));

    {
        let snapshot_config = SnapshotConfig {
            collections_mapping: collection_name_to_snapshot_path,
            collections_aliases: alias_mapping,
        };
        let mut config_file = tokio::fs::File::create(&config_path).await?;
        config_file
            .write_all(
                serde_json::to_string_pretty(&snapshot_config)
                    .unwrap()
                    .as_bytes(),
            )
            .await?;
    }

    let full_snapshot_path = base.join(&snapshot_name);
    let snapshot_file = tempfile::TempPath::from_path(&full_snapshot_path);

    let config_path_clone = config_path.clone();
    let full_snapshot_path_clone = full_snapshot_path.clone();
    let archiving = tokio::task::spawn_blocking(move || {
        // have to use std here, cause TarBuilder is not async
        let file = std::fs::File::create(&full_snapshot_path_clone)?;
        let mut builder = TarBuilder::new(file);
        for (_, snapshot_path, checksum_path) in created_snapshots {
            builder.append_path_with_name(&snapshot_path, snapshot_path.file_name().unwrap())?;
            snapshot_path.close()?;
            if let Err(err) = checksum_path.close() {
                log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
            }
        }
        builder.append_path_with_name(&config_path_clone, "config.json")?;

        builder.finish()?;
        Ok::<(), SnapshotManagerError>(())
    });
    archiving.await??;

    // Compute and store the file's checksum
    let checksum_path = PathBuf::from(format!("{}.checksum", full_snapshot_path.display()));
    let checksum = hash_file(full_snapshot_path.as_path()).await?;
    let checksum_file = tempfile::TempPath::from_path(&checksum_path);
    let mut file = tokio::fs::File::create(checksum_path.as_path()).await?;
    file.write_all(checksum.as_bytes()).await?;

    tokio::fs::remove_file(&config_path).await?;

    dispatcher
        .snapshot_manager()
        .save_snapshot(&snapshot_name, snapshot_file, checksum_file)
        .await?;

    let description = dispatcher
        .snapshot_manager()
        .get_snapshot_description(snapshot_name)
        .await?;

    Ok(description)
}
