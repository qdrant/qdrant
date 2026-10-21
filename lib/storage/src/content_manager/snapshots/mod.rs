pub mod download;
pub mod download_result;
pub mod download_tar;
pub mod recover;

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use collection::common::snapshots_manager::{SnapshotStorageCloud, SnapshotStorageManager};
use collection::operations::snapshot_ops::{SnapshotDescription, unique_snapshot_timestamp};
use collection::operations::verification::new_unchecked_verification_pass;
use common::disk::dir_disk_size;
use common::disk_usage::disk_usage;
use common::tar_ext::BuilderExt;
use fs_err as fs;
use fs_err::tokio as tokio_fs;
use serde::{Deserialize, Serialize};
use tar::Builder as TarBuilder;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;
use tokio_util::io::SyncIoBridge;
use tokio_util::task::AbortOnDropHandle;

use crate::content_manager::toc::{COLLECTIONS_DIR, FULL_SNAPSHOT_FILE_NAME};
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
    match toc.get_snapshots_storage_manager()? {
        SnapshotStorageManager::Cloud(cloud) => {
            create_full_snapshot_streaming(toc, multipass, &cloud).await
        }
        SnapshotStorageManager::LocalFS(_) => create_full_snapshot_local(toc, multipass).await,
    }
}

/// Streams the full snapshot archive into object storage as it is built.
///
/// Collection snapshots are built one at a time in the temp directory, appended to the
/// archive and deleted, so they are never stored on their own. Temp disk holds one
/// collection archive at a time plus its build scratch space, and the archive is
/// uploaded once.
async fn create_full_snapshot_streaming(
    toc: &TableOfContent,
    multipass: CollectionMultipass,
    cloud: &SnapshotStorageCloud,
) -> Result<SnapshotDescription, StorageError> {
    let current_time = unique_snapshot_timestamp();
    let snapshot_name = format!("{FULL_SNAPSHOT_FILE_NAME}-{current_time}.snapshot");
    let full_snapshot_path = toc.snapshots_path().join(&snapshot_name);

    // Sizes the upload parts and caps it at 10,000 of them; doubled for compression and growth.
    let storage_path = toc.storage_config.storage_path.clone();
    let estimated_size = tokio::task::spawn_blocking(move || {
        match dir_disk_size(storage_path.join(COLLECTIONS_DIR)) {
            Ok(size) => size.saturating_mul(2),
            Err(err) => {
                // Files can vanish mid-scan; used disk space is a race-free upper bound.
                log::warn!("Failed to measure collections, sizing parts from disk usage: {err}");
                disk_usage(&storage_path).map_or(0, |usage| usage.used())
            }
        }
    })
    .await?;

    let (writer, upload) = cloud.start_stream_upload(&full_snapshot_path, estimated_size);
    let tar = BuilderExt::new_streaming_owned(SyncIoBridge::new(writer));

    let all_collections = toc.multipass_into_collections(&multipass).await;
    let archived = async {
        let mut collections_mapping = HashMap::new();
        let temp_dir = toc.optional_temp_or_storage_temp_path()?;
        for collection_pass in &all_collections {
            // Spans the append too, which is where this collection gets uploaded
            let _running_snapshots_guard = toc.count_snapshot_creation(collection_pass.name());
            let (collection_snapshot_name, archive) = toc
                .get_collection(collection_pass)
                .await?
                .create_snapshot_archive(&temp_dir, toc.this_peer_id)
                .await?;
            let collection_tar = tar.clone();
            let dst = PathBuf::from(&collection_snapshot_name);
            tokio::task::spawn_blocking(move || {
                collection_tar.blocking_append_file(archive.path(), &dst)
            })
            .await??;
            collections_mapping
                .insert(collection_pass.name().to_string(), collection_snapshot_name);
        }

        let mut collections_aliases: HashMap<String, String> = Default::default();
        for collection_pass in &all_collections {
            for alias in toc
                .all_collection_aliases(collection_pass.name(), &multipass)
                .await
            {
                collections_aliases.insert(alias, collection_pass.name().to_string());
            }
        }

        let snapshot_config = SnapshotConfig {
            collections_mapping,
            collections_aliases,
        };
        tar.append_data(
            serde_json::to_vec_pretty(&snapshot_config)?,
            Path::new("config.json"),
        )
        .await?;
        tar.finish().await?;
        Ok::<(), StorageError>(())
    }
    .await;

    if let Err(err) = archived {
        // A failed upload breaks the archive stream, so its error is the real cause.
        return Err(match upload.abort().await {
            Ok(()) => err,
            Err(upload_err) => {
                log::warn!("Full snapshot {snapshot_name} archive failed: {err}");
                StorageError::from(upload_err)
            }
        });
    }

    upload.commit().await?;
    Ok(cloud.get_snapshot_description(&full_snapshot_path).await?)
}

async fn create_full_snapshot_local(
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
    let current_time = unique_snapshot_timestamp();

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
    let _temp_full_snapshot_path_file = TempPath::try_from_path(&temp_full_snapshot_path)?;

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
            TempPath::try_from_path(local_temp_collection_snapshot)?,
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
