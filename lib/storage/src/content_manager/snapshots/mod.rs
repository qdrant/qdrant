pub mod download;
pub mod download_result;
pub mod download_tar;
pub mod recover;

use std::collections::HashMap;
use std::io::{BufWriter, Write};

use collection::operations::snapshot_ops::SnapshotDescription;
use collection::operations::verification::new_unchecked_verification_pass;
use fs_err as fs;
use fs_err::tokio as tokio_fs;
use serde::{Deserialize, Serialize};
use tar::Builder as TarBuilder;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;
use tokio_util::task::AbortOnDropHandle;

use crate::content_manager::toc::FULL_SNAPSHOT_FILE_NAME;
use crate::dispatcher::Dispatcher;
use crate::rbac::{AccessRequirements, Auth, CollectionMultipass};
use crate::{StorageError, TableOfContent};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// Map collection name to snapshot file name
    pub collections_mapping: HashMap<String, String>,
    /// Aliases for collections `<alias>:<collection_name>`
    #[serde(default)]
    pub collections_aliases: HashMap<String, String>,
}

pub async fn do_delete_full_snapshot(
    dispatcher: &Dispatcher,
    auth: Auth,
    snapshot_name: &str,
) -> Result<bool, StorageError> {
    auth.check_global_access(AccessRequirements::new().manage(), "delete_full_snapshot")?;

    // All checks should've been done at this point.
    let pass = new_unchecked_verification_pass();

    let toc = dispatcher.toc(&auth, &pass);

    let snapshot_manager = toc.get_snapshots_storage_manager()?;
    let snapshot_dir =
        snapshot_manager.get_full_snapshot_path(toc.snapshots_path(), snapshot_name)?;

    let res = tokio::spawn(async move {
        log::info!("Deleting full storage snapshot {snapshot_dir:?}");
        snapshot_manager.delete_snapshot(&snapshot_dir).await
    })
    .await??;

    Ok(res)
}

pub async fn do_delete_collection_snapshot(
    dispatcher: &Dispatcher,
    auth: Auth,
    collection_name: &str,
    snapshot_name: &str,
) -> Result<bool, StorageError> {
    let collection_pass = auth.check_collection_access(
        collection_name,
        AccessRequirements::new().write().extras(),
        "delete_collection_snapshot",
    )?;

    // All checks should've been done at this point.
    let pass = new_unchecked_verification_pass();

    let toc = dispatcher.toc(&auth, &pass);

    let snapshot_name = snapshot_name.to_string();
    let collection = toc.get_collection(&collection_pass).await?;
    let snapshot_manager = toc.get_snapshots_storage_manager()?;
    let file_name =
        snapshot_manager.get_snapshot_path(collection.snapshots_path(), &snapshot_name)?;

    let res = tokio::spawn(async move {
        log::info!("Deleting collection snapshot {file_name:?}");
        snapshot_manager.delete_snapshot(&file_name).await
    })
    .await??;

    Ok(res)
}

pub async fn do_list_full_snapshots(
    toc: &TableOfContent,
    auth: Auth,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    auth.check_global_access(AccessRequirements::new(), "list_full_snapshots")?;
    let snapshots_manager = toc.get_snapshots_storage_manager()?;
    let snapshots_path = toc.snapshots_path();
    Ok(snapshots_manager.list_snapshots(snapshots_path).await?)
}

pub async fn do_create_full_snapshot(
    dispatcher: &Dispatcher,
    auth: Auth,
) -> Result<SnapshotDescription, StorageError> {
    let collections_pass =
        auth.check_global_access(AccessRequirements::new().manage(), "create_full_snapshot")?;

    // All checks should've been done at this point.
    let pass = new_unchecked_verification_pass();
    let toc = dispatcher.toc(&auth, &pass).clone();

    let res = tokio::spawn(async move { _do_create_full_snapshot(&toc, collections_pass).await })
        .await??;
    Ok(res)
}

async fn _do_create_full_snapshot(
    toc: &TableOfContent,
    multipass: CollectionMultipass,
) -> Result<SnapshotDescription, StorageError> {
    let snapshot_dir = toc.snapshots_path();

    let all_collections = toc.multipass_into_collections(&multipass).await;
    let mut created_snapshots: Vec<(&str, SnapshotDescription)> = vec![];
    for collection_pass in &all_collections {
        let snapshot_details = toc.create_snapshot(collection_pass).await?;
        created_snapshots.push((collection_pass.name(), snapshot_details));
    }
    let current_time = chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S").to_string();

    let snapshot_name = format!("{FULL_SNAPSHOT_FILE_NAME}-{current_time}.snapshot");

    let collection_name_to_snapshot_path: HashMap<_, _> = created_snapshots
        .iter()
        .map(|&(collection_name, ref snapshot_details)| {
            (collection_name.to_string(), snapshot_details.name.clone())
        })
        .collect();

    let mut alias_mapping: HashMap<String, String> = Default::default();
    for collection_pass in &all_collections {
        for alias in toc
            .all_collection_aliases(collection_pass.name(), &multipass)
            .await
        {
            alias_mapping.insert(alias, collection_pass.name().to_string());
        }
    }

    let config_path = snapshot_dir.join(format!("config-{current_time}.json"));

    {
        let snapshot_config = SnapshotConfig {
            collections_mapping: collection_name_to_snapshot_path,
            collections_aliases: alias_mapping,
        };
        let mut config_file = tokio_fs::File::create(&config_path).await?;
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
    let snapshot_manager = toc.get_snapshots_storage_manager()?;

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
        let mut file = BufWriter::new(fs::File::create(&full_snapshot_path_clone)?);
        let mut builder = TarBuilder::new(&mut file);
        builder.sparse(true);
        for (temp_file, snapshot_name) in temp_collection_snapshots {
            builder.append_path_with_name(&temp_file, &snapshot_name)?;
        }
        builder.append_path_with_name(&config_path_clone, "config.json")?;
        builder.finish()?;

        // Explicitly flush write buffer so we can catch IO errors
        drop(builder);
        file.flush()?;

        Ok::<(), StorageError>(())
    });
    AbortOnDropHandle::new(archiving).await??;

    let snapshot_description = snapshot_manager
        .store_file(&temp_full_snapshot_path, &full_snapshot_path)
        .await?;
    tokio_fs::remove_file(&config_path).await?;
    Ok(snapshot_description)
}
