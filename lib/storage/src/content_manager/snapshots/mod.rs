pub mod download;
pub mod recover;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use collection::operations::snapshot_ops::{
    get_snapshot_description, list_snapshots_in_directory, SnapshotDescription,
};
use serde::{Deserialize, Serialize};
use tar::Builder as TarBuilder;
use tokio::io::AsyncWriteExt;

use crate::content_manager::toc::FULL_SNAPSHOT_FILE_NAME;
use crate::{StorageError, TableOfContent};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// Map collection name to snapshot file name
    pub collections_mapping: HashMap<String, String>,
    /// Aliases for collections <alias>:<collection_name>
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
            description: format!("Snapshot {} not found", snapshot_name),
        });
    }
    Ok(snapshot_path)
}

pub async fn do_list_full_snapshots(
    toc: &TableOfContent,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    let snapshots_path = Path::new(toc.snapshots_path());
    Ok(list_snapshots_in_directory(snapshots_path).await?)
}

pub async fn do_create_full_snapshot(
    toc: &TableOfContent,
) -> Result<SnapshotDescription, StorageError> {
    let snapshot_dir = Path::new(toc.snapshots_path());

    let all_collections = toc.all_collections().await;
    let mut created_snapshots: Vec<(&str, SnapshotDescription)> = vec![];
    for collection_name in &all_collections {
        let snapshot_details = toc.create_snapshot(collection_name).await?;
        created_snapshots.push((collection_name, snapshot_details));
    }
    let current_time = chrono::Utc::now().format("%Y-%m-%d-%H-%M-%S").to_string();

    let snapshot_name = format!("{}-{}.snapshot", FULL_SNAPSHOT_FILE_NAME, &current_time);

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
    // have to use std here, cause TarBuilder is not async
    let file = std::fs::File::create(&full_snapshot_path)?;
    let mut builder = TarBuilder::new(file);
    for (collection_name, snapshot_details) in created_snapshots {
        let snapshot_path = snapshot_dir
            .join(collection_name)
            .join(&snapshot_details.name);
        builder.append_path_with_name(&snapshot_path, &snapshot_details.name)?;
        tokio::fs::remove_file(snapshot_path).await?;
    }
    builder.append_path_with_name(&config_path, "config.json")?;

    builder.finish()?;

    tokio::fs::remove_file(&config_path).await?;

    Ok(get_snapshot_description(&full_snapshot_path).await?)
}
