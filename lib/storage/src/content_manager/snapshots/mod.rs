pub mod download;
pub mod recover;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use collection::common::sha_256::hash_file;
use collection::operations::snapshot_ops::{
    get_checksum_path, get_snapshot_description, list_snapshots_in_directory, SnapshotDescription,
};
use serde::{Deserialize, Serialize};
use tar::Builder as TarBuilder;
use tokio::io::AsyncWriteExt;

use crate::content_manager::toc::FULL_SNAPSHOT_FILE_NAME;
use crate::dispatcher::Dispatcher;
use crate::{StorageError, TableOfContent};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// Map collection name to snapshot file name
    pub collections_mapping: HashMap<String, String>,
    /// Aliases for collections `<alias>:<collection_name>`
    #[serde(default)]
    pub collections_aliases: HashMap<String, String>,
}

pub async fn get_full_snapshot_path(
    toc: &TableOfContent,
    snapshot_name: &str,
) -> Result<PathBuf, StorageError> {
    let snapshot_path = Path::new(toc.snapshots_path()).join(snapshot_name);
    if !snapshot_path.exists() {
        return Err(StorageError::NotFound {
            description: format!("Full storage snapshot {snapshot_name} not found"),
        });
    }
    Ok(snapshot_path)
}

pub async fn do_delete_full_snapshot(
    dispatcher: &Dispatcher,
    snapshot_name: &str,
    wait: bool,
) -> Result<bool, StorageError> {
    let dispatcher = dispatcher.clone();
    let snapshot_name = snapshot_name.to_string();
    let task =
        tokio::spawn(async move { _do_delete_full_snapshot(&dispatcher, &snapshot_name).await });

    if wait {
        task.await??;
    }

    Ok(true)
}

async fn _do_delete_full_snapshot(
    dispatcher: &Dispatcher,
    snapshot_name: &str,
) -> Result<bool, StorageError> {
    let snapshot_dir = get_full_snapshot_path(dispatcher.toc(), snapshot_name).await?;
    let checksum_path = get_checksum_path(snapshot_dir.as_path());
    log::info!("Deleting full storage snapshot {:?}", snapshot_dir);
    let (delete_snapshot, delete_checksum) = tokio::join!(
        tokio::fs::remove_file(snapshot_dir),
        tokio::fs::remove_file(checksum_path),
    );
    delete_snapshot?;

    // We might not have a checksum file for the snapshot, ignore deletion errors in that case
    if let Err(err) = delete_checksum {
        log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
    }

    Ok(true)
}

pub async fn do_delete_collection_snapshot(
    dispatcher: &Dispatcher,
    collection_name: &str,
    snapshot_name: &str,
    wait: bool,
) -> Result<bool, StorageError> {
    let dispatcher = dispatcher.clone();
    let collection_name = collection_name.to_string();
    let snapshot_name = snapshot_name.to_string();

    let task = tokio::spawn(async move {
        _do_delete_collection_snapshot(&dispatcher, &collection_name, &snapshot_name).await
    });

    if wait {
        task.await??;
    }

    Ok(true)
}

async fn _do_delete_collection_snapshot(
    dispatcher: &Dispatcher,
    collection_name: &str,
    snapshot_name: &str,
) -> Result<bool, StorageError> {
    let collection = dispatcher.get_collection(collection_name).await?;
    let file_name = collection.get_snapshot_path(snapshot_name).await?;
    let checksum_path = get_checksum_path(file_name.as_path());
    log::info!("Deleting collection snapshot {:?}", file_name);
    let (delete_snapshot, delete_checksum) = tokio::join!(
        tokio::fs::remove_file(file_name),
        tokio::fs::remove_file(checksum_path)
    );
    delete_snapshot?;

    // We might not have a checksum file for the snapshot, ignore deletion errors in that case
    if let Err(err) = delete_checksum {
        log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
    }

    Ok(true)
}

pub async fn do_list_full_snapshots(
    toc: &TableOfContent,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    let snapshots_path = Path::new(toc.snapshots_path());
    Ok(list_snapshots_in_directory(snapshots_path).await?)
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

    let snapshot_dir = Path::new(dispatcher.snapshots_path()).to_path_buf();

    let all_collections = dispatcher.all_collections().await;
    let mut created_snapshots: Vec<(&str, SnapshotDescription)> = vec![];
    for collection_name in &all_collections {
        let snapshot_details = dispatcher.create_snapshot(collection_name).await?;
        created_snapshots.push((collection_name, snapshot_details));
    }
    let current_time = chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S").to_string();

    let snapshot_name = format!("{FULL_SNAPSHOT_FILE_NAME}-{current_time}.snapshot");

    let collection_name_to_snapshot_path: HashMap<_, _> = created_snapshots
        .iter()
        .map(|(collection_name, snapshot_details)| {
            (collection_name.to_string(), snapshot_details.name.clone())
        })
        .collect();

    let mut alias_mapping: HashMap<String, String> = Default::default();
    for collection_name in &all_collections {
        for alias in dispatcher.collection_aliases(collection_name).await? {
            alias_mapping.insert(alias.to_string(), collection_name.to_string());
        }
    }

    let config_path = snapshot_dir.join(format!("config-{current_time}.json"));

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

    let full_snapshot_path = snapshot_dir.join(&snapshot_name);
    let snapshot_file = tempfile::TempPath::from_path(&full_snapshot_path);

    let config_path_clone = config_path.clone();
    let full_snapshot_path_clone = full_snapshot_path.clone();
    let created_snapshots_clone: Vec<_> = created_snapshots
        .iter()
        .map(|(x, y)| (x.to_string(), y.to_owned()))
        .collect();
    let archiving = tokio::task::spawn_blocking(move || {
        // have to use std here, cause TarBuilder is not async
        let file = std::fs::File::create(&full_snapshot_path_clone)?;
        let mut builder = TarBuilder::new(file);
        for (collection_name, snapshot_details) in created_snapshots_clone {
            let snapshot_path = snapshot_dir
                .join(collection_name)
                .join(&snapshot_details.name);
            builder.append_path_with_name(&snapshot_path, &snapshot_details.name)?;
            std::fs::remove_file(&snapshot_path)?;

            // Remove associated checksum if there is one
            let snapshot_checksum = get_checksum_path(snapshot_path.as_path());
            if let Err(err) = std::fs::remove_file(snapshot_checksum) {
                log::warn!("Failed to delete checksum file for snapshot, ignoring: {err}");
            }
        }
        builder.append_path_with_name(&config_path_clone, "config.json")?;

        builder.finish()?;
        Ok::<(), StorageError>(())
    });
    archiving.await??;

    // Compute and store the file's checksum
    log::debug!("Computing checksum for snapshot: {full_snapshot_path:?}");
    let checksum_path = get_checksum_path(full_snapshot_path.as_path());
    let checksum = hash_file(full_snapshot_path.as_path()).await?;
    let checksum_file = tempfile::TempPath::from_path(&checksum_path);
    let mut file = tokio::fs::File::create(checksum_path.as_path()).await?;
    file.write_all(checksum.as_bytes()).await?;

    tokio::fs::remove_file(&config_path).await?;

    // Snapshot files are ready now, so keep them
    snapshot_file.keep()?;
    checksum_file.keep()?;

    Ok(get_snapshot_description(&full_snapshot_path).await?)
}
