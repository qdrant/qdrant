pub mod download;
pub mod recover;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use collection::operations::snapshot_ops::SnapshotDescription;
use serde::{Deserialize, Serialize};
use tar::Builder as TarBuilder;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use crate::content_manager::toc::FULL_SNAPSHOT_FILE_NAME;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequrements, CollectionMultipass};
use crate::{StorageError, TableOfContent};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// Map collection name to snapshot file name
    pub collections_mapping: HashMap<String, String>,
    /// Aliases for collections `<alias>:<collection_name>`
    #[serde(default)]
    pub collections_aliases: HashMap<String, String>,
}

/// Get absolute file path for a full snapshot by name
///
/// This enforces the file to be inside the snapshots directory
pub async fn get_full_snapshot_path(
    toc: &TableOfContent,
    snapshot_name: &str,
) -> Result<PathBuf, StorageError> {
    let snapshots_path = toc.snapshots_path();

    let absolute_snapshot_dir = Path::new(snapshots_path)
        .canonicalize()
        .map_err(|_| StorageError::not_found(format!("Snapshot directory: {snapshots_path}")))?;

    let absolute_snapshot_path = absolute_snapshot_dir
        .join(snapshot_name)
        .canonicalize()
        .map_err(|_| StorageError::not_found(format!("Snapshot {snapshot_name}")))?;

    if !absolute_snapshot_path.starts_with(absolute_snapshot_dir) {
        return Err(StorageError::not_found(format!("Snapshot {snapshot_name}")));
    }

    if !absolute_snapshot_path.is_file() {
        return Err(StorageError::not_found(format!("Snapshot {snapshot_name}")));
    }

    Ok(absolute_snapshot_path)
}

pub async fn do_delete_full_snapshot(
    dispatcher: &Dispatcher,
    access: Access,
    snapshot_name: &str,
) -> Result<JoinHandle<Result<bool, StorageError>>, StorageError> {
    access.check_global_access(AccessRequrements::new().manage())?;
    let toc = dispatcher.toc(&access);
    let snapshot_manager = toc.get_snapshots_storage_manager();
    let snapshot_dir = get_full_snapshot_path(toc, snapshot_name).await?;
    log::info!("Deleting full storage snapshot {:?}", snapshot_dir);
    Ok(tokio::spawn(async move {
        Ok(snapshot_manager.delete_snapshot(&snapshot_dir).await?)
    }))
}

pub async fn do_delete_collection_snapshot(
    dispatcher: &Dispatcher,
    access: Access,
    collection_name: &str,
    snapshot_name: &str,
) -> Result<JoinHandle<Result<bool, StorageError>>, StorageError> {
    let collection_pass = access
        .check_collection_access(collection_name, AccessRequrements::new().write().whole())?;
    let toc = dispatcher.toc(&access);
    let snapshot_name = snapshot_name.to_string();
    let collection = toc.get_collection_by_pass(&collection_pass).await?;
    let file_name = collection.get_snapshot_path(&snapshot_name).await?;
    let snapshot_manager = toc.get_snapshots_storage_manager();

    log::info!("Deleting collection snapshot {:?}", file_name);
    Ok(tokio::spawn(async move {
        Ok(snapshot_manager.delete_snapshot(&file_name).await?)
    }))
}

pub async fn do_list_full_snapshots(
    toc: &TableOfContent,
    access: Access,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    access.check_global_access(AccessRequrements::new())?;
    let snapshots_manager = toc.get_snapshots_storage_manager();
    let snapshots_path = Path::new(toc.snapshots_path());
    Ok(snapshots_manager.list_snapshots(snapshots_path).await?)
}

pub fn do_create_full_snapshot(
    dispatcher: &Dispatcher,
    access: Access,
) -> Result<JoinHandle<Result<SnapshotDescription, StorageError>>, StorageError> {
    let multipass = access.check_global_access(AccessRequrements::new().manage())?;
    let toc = dispatcher.toc(&access).clone();
    Ok(tokio::spawn(async move {
        _do_create_full_snapshot(&toc, multipass).await
    }))
}

async fn _do_create_full_snapshot(
    toc: &TableOfContent,
    multipass: CollectionMultipass,
) -> Result<SnapshotDescription, StorageError> {
    let snapshot_dir = Path::new(toc.snapshots_path()).to_path_buf();

    let all_collections = toc.all_collections().await;
    let mut created_snapshots: Vec<(&str, SnapshotDescription)> = vec![];
    for collection_name in &all_collections {
        let snapshot_details = toc
            .create_snapshot(&multipass.issue_pass(collection_name))
            .await?;
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
        for alias in toc.collection_aliases(collection_name).await? {
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

    let temp_full_snapshot_path = toc
        .optional_temp_or_storage_temp_path()?
        .join(&snapshot_name);

    // Make sure temporary file is removed in case of error
    let _temp_full_snapshot_path_file = TempPath::from_path(&temp_full_snapshot_path);

    let config_path_clone = config_path.clone();

    // (tempfile_with_snapshot, snapshot_name)
    let mut temp_collection_snapshots = vec![];

    let temp_storage_path = toc.optional_temp_or_storage_temp_path()?;
    let snapshot_manager = toc.get_snapshots_storage_manager();

    for (collection_name, snapshot_details) in &created_snapshots {
        let snapshot_path = snapshot_dir
            .join(collection_name)
            .join(&snapshot_details.name);

        let local_temp_collection_snapshot = temp_storage_path
            .join(collection_name)
            .join(&snapshot_details.name);

        snapshot_manager
            .get_stored_file(&snapshot_path, &local_temp_collection_snapshot)
            .await?;

        temp_collection_snapshots.push((
            TempPath::from_path(local_temp_collection_snapshot),
            snapshot_details.name.clone(),
        ));
    }

    let full_snapshot_path_clone = temp_full_snapshot_path.clone();
    let archiving = tokio::task::spawn_blocking(move || {
        // have to use std here, cause TarBuilder is not async
        let file = std::fs::File::create(&full_snapshot_path_clone)?;
        let mut builder = TarBuilder::new(file);
        for (temp_file, snapshot_name) in temp_collection_snapshots {
            builder.append_path_with_name(&temp_file, &snapshot_name)?;
        }
        builder.append_path_with_name(&config_path_clone, "config.json")?;

        builder.finish()?;
        Ok::<(), StorageError>(())
    });
    archiving.await??;

    let snapshot_description = snapshot_manager
        .store_file(&temp_full_snapshot_path, &full_snapshot_path)
        .await?;
    tokio::fs::remove_file(&config_path).await?;
    Ok(snapshot_description)
}
