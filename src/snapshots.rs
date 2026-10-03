use std::io::BufReader;
use std::path::{Path, PathBuf};

use collection::collection::Collection;
use collection::shards::shard::PeerId;
use common::fs::{move_dir, safe_delete_in_tmp};
use common::tar_unpack::tar_unpack_file;
use fs_err as fs;
use fs_err::File;
use log::info;
use shard::snapshots::snapshot_data::SnapshotData;
use storage::content_manager::alias_mapping::AliasPersistence;
use storage::content_manager::snapshots::SnapshotConfig;
use storage::content_manager::toc::{ALIASES_PATH, COLLECTIONS_DIR};

struct SnapshotMapping {
    snapshot_path: PathBuf,
    collection_name: String,
}

impl SnapshotMapping {
    fn from_cli_arg(snapshot_params: &str) -> Self {
        let (path, collection_name) = snapshot_params
            .rsplit_once(':')
            .unwrap_or_else(|| panic!("Collection name is missing: {snapshot_params}"));

        assert!(
            !path.is_empty(),
            "Snapshot path is missing: {snapshot_params}",
        );
        assert!(
            !collection_name.is_empty(),
            "Collection name is missing: {snapshot_params}",
        );
        assert!(
            !collection_name.contains(['/', '\\']),
            "Collection name must not contain slashes: {snapshot_params}",
        );

        Self {
            snapshot_path: PathBuf::from(path),
            collection_name: collection_name.to_string(),
        }
    }
}

/// Recover snapshots from the given arguments
///
/// # Arguments
///
/// * `mapping` - `[ <path>:<collection_name> ]`
/// * `force` - if true, allow to overwrite collections from snapshots
///
/// # Returns
///
/// * `Vec<String>` - list of collections that were recovered
pub fn recover_snapshots(
    mapping: &[String],
    force: bool,
    temp_dir: Option<&Path>,
    storage_dir: &Path,
    this_peer_id: PeerId,
    is_distributed: bool,
) -> Vec<String> {
    let mappings = mapping
        .iter()
        .map(|snapshot_params| SnapshotMapping::from_cli_arg(snapshot_params));

    recover_snapshot_mappings(
        mappings,
        force,
        temp_dir,
        storage_dir,
        this_peer_id,
        is_distributed,
    )
}

fn recover_snapshot_mappings(
    mapping: impl IntoIterator<Item = SnapshotMapping>,
    force: bool,
    temp_dir: Option<&Path>,
    storage_dir: &Path,
    this_peer_id: PeerId,
    is_distributed: bool,
) -> Vec<String> {
    let collection_dir_path = storage_dir.join(COLLECTIONS_DIR);
    fs::create_dir_all(&collection_dir_path).unwrap();
    let mut recovered_collections: Vec<String> = vec![];

    for SnapshotMapping {
        snapshot_path,
        collection_name,
    } in mapping
    {
        let snapshot_data = SnapshotData::new_packed_persistent(&snapshot_path);

        recovered_collections.push(collection_name.clone());
        info!(
            "Recovering snapshot {collection_name} from {}",
            snapshot_path.display()
        );
        // check if collection already exists
        // if it does, we need to check if we want to overwrite it
        // if not, we need to abort
        let collection_path = collection_dir_path.join(&collection_name);
        info!("Collection path: {}", collection_path.display());
        if collection_path.exists() {
            if !force {
                panic!(
                    "Collection {collection_name} already exists. Use --force-snapshot to overwrite it."
                );
            }
            info!("Overwriting collection {collection_name}");
        }
        let collection_temp_path = collection_path.with_extension("tmp");
        if collection_temp_path.exists() {
            fs::remove_dir_all(&collection_temp_path).unwrap();
        }
        let collection_recovery_temp =
            temp_dir.map(|temp_dir| snapshot_recovery_temp_dir(Some(temp_dir), storage_dir));
        let collection_recovery_path = collection_recovery_temp
            .as_ref()
            .map_or(collection_temp_path.as_path(), |temp_dir| temp_dir.path());
        if let Err(err) = Collection::restore_snapshot(
            snapshot_data,
            collection_recovery_path,
            this_peer_id,
            is_distributed,
        ) {
            panic!("Failed to recover snapshot {collection_name}: {err}");
        }
        if let Some(collection_recovery_temp) = collection_recovery_temp {
            move_dir(collection_recovery_temp.path(), &collection_temp_path).unwrap();
        }
        // Remove collection_path directory if exists
        if collection_path.exists()
            && let Err(err) = safe_delete_in_tmp(&collection_path, &storage_dir.join(".deleted"))
                .and_then(|to_delete| to_delete.close())
        {
            panic!("Failed to remove collection {collection_name}: {err}");
        }
        fs::rename(&collection_temp_path, &collection_path).unwrap();
    }
    recovered_collections
}

fn snapshot_recovery_temp_dir(temp_dir: Option<&Path>, storage_dir: &Path) -> tempfile::TempDir {
    let temp_base = temp_dir
        .map(|path| path.join("tmp"))
        .unwrap_or_else(|| storage_dir.to_path_buf());
    fs::create_dir_all(&temp_base).unwrap();

    tempfile::Builder::new()
        .prefix("snapshots-recovery-")
        .tempdir_in(temp_base)
        .unwrap()
}

pub fn recover_full_snapshot(
    temp_dir: Option<&Path>,
    snapshot_path: &str,
    storage_dir: &Path,
    force: bool,
    this_peer_id: PeerId,
    is_distributed: bool,
) -> Vec<String> {
    let snapshot_temp_dir = snapshot_recovery_temp_dir(temp_dir, storage_dir);
    let snapshot_temp_path = snapshot_temp_dir.path();

    // Un-tar snapshot into temporary directory
    tar_unpack_file(Path::new(snapshot_path), snapshot_temp_path).unwrap();

    // Read configuration file with snapshot-to-collection mapping
    let config_path = snapshot_temp_path.join("config.json");
    let config_file = BufReader::new(File::open(config_path).unwrap());
    let config_json: SnapshotConfig = serde_json::from_reader(config_file).unwrap();

    // Create mapping from the configuration file
    let mapping = config_json
        .collections_mapping
        .iter()
        .map(|(collection_name, snapshot_file)| SnapshotMapping {
            snapshot_path: snapshot_temp_path.join(snapshot_file),
            collection_name: collection_name.clone(),
        });

    // Launch regular recovery of snapshots
    let recovered_collection = recover_snapshot_mappings(
        mapping,
        force,
        temp_dir,
        storage_dir,
        this_peer_id,
        is_distributed,
    );

    let alias_path = storage_dir.join(ALIASES_PATH);
    let mut alias_persistence =
        AliasPersistence::open(&alias_path).expect("Can't open database by the provided config");
    for (alias, collection_name) in config_json.collections_aliases {
        if alias_persistence.get(&alias).is_some() && !force {
            panic!("Alias {alias} already exists. Use --force-snapshot to overwrite it.");
        }
        alias_persistence.insert(alias, collection_name).unwrap();
    }

    snapshot_temp_dir.close().unwrap();
    recovered_collection
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use fs_err as fs;

    use super::{SnapshotMapping, snapshot_recovery_temp_dir};

    #[test]
    fn snapshot_mapping_absolute_paths() {
        // Unix path
        let mapping = SnapshotMapping::from_cli_arg("/tmp/collection.snapshot:test_collection");
        assert_eq!(
            mapping.snapshot_path,
            PathBuf::from("/tmp/collection.snapshot"),
        );
        assert_eq!(mapping.collection_name, "test_collection");

        // Windows path
        // See: <https://github.com/qdrant/qdrant/pull/9723>
        let mapping = SnapshotMapping::from_cli_arg(r"C:\tmp\collection.snapshot:test_collection");
        assert_eq!(
            mapping.snapshot_path,
            PathBuf::from(r"C:\tmp\collection.snapshot"),
        );
        assert_eq!(mapping.collection_name, "test_collection");
    }

    #[test]
    fn snapshot_mapping_relative_path() {
        let mapping = SnapshotMapping::from_cli_arg("collection.snapshot:test_collection");
        assert_eq!(mapping.snapshot_path, PathBuf::from("collection.snapshot"));
        assert_eq!(mapping.collection_name, "test_collection");

        let mapping = SnapshotMapping::from_cli_arg("./collection.snapshot:test_collection");
        assert_eq!(
            mapping.snapshot_path,
            PathBuf::from("./collection.snapshot")
        );
        assert_eq!(mapping.collection_name, "test_collection");
    }

    #[test]
    fn snapshot_mapping_splits_on_last_colon() {
        let mapping = SnapshotMapping::from_cli_arg("/tmp/snapshots/a:b.snapshot:test_collection");
        assert_eq!(
            mapping.snapshot_path,
            PathBuf::from("/tmp/snapshots/a:b.snapshot"),
        );
        assert_eq!(mapping.collection_name, "test_collection");
    }

    #[test]
    #[should_panic(expected = "Collection name is missing")]
    fn snapshot_mapping_rejects_missing_separator() {
        SnapshotMapping::from_cli_arg("collection.snapshot");
    }

    #[test]
    #[should_panic(expected = "Snapshot path is missing")]
    fn snapshot_mapping_rejects_empty_path() {
        SnapshotMapping::from_cli_arg(":test_collection");
    }

    #[test]
    #[should_panic(expected = "Collection name is missing")]
    fn snapshot_mapping_rejects_empty_collection_name() {
        SnapshotMapping::from_cli_arg("collection.snapshot:");
    }

    #[test]
    fn snapshot_recovery_uses_owned_temp_subdirectory() {
        let configured_temp = tempfile::tempdir().unwrap();
        let storage_dir = tempfile::tempdir().unwrap();
        let sentinel_path = configured_temp.path().join("sentinel.txt");
        fs::write(&sentinel_path, "keep").unwrap();

        let recovery_temp =
            snapshot_recovery_temp_dir(Some(configured_temp.path()), storage_dir.path());
        let recovery_temp_path = recovery_temp.path().to_path_buf();

        assert!(recovery_temp_path.starts_with(configured_temp.path().join("tmp")));
        assert_ne!(recovery_temp_path, configured_temp.path());

        recovery_temp.close().unwrap();

        assert!(configured_temp.path().exists());
        assert!(sentinel_path.exists());
        assert!(!recovery_temp_path.exists());
    }

    #[test]
    #[should_panic(
        expected = "Collection name must not contain slashes: C:\\tmp\\collection.snapshot"
    )]
    fn snapshot_mapping_rejects_windows_path_without_collection_name() {
        SnapshotMapping::from_cli_arg(r"C:\tmp\collection.snapshot");
    }
}
