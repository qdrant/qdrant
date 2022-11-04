use std::fs::{remove_dir_all, rename};
use std::path::Path;

use collection::collection::Collection;
use log::info;
use storage::content_manager::alias_mapping::AliasPersistence;
use storage::content_manager::snapshots::SnapshotConfig;
use storage::content_manager::toc::{ALIASES_PATH, COLLECTIONS_DIR};

/// Recover snapshots from the given arguments
///
/// # Arguments
///
/// * `mapping` - [ "<path>:<collection_name>" ]
/// * `force` - if true, allow to overwrite collections from snapshots
///
/// # Returns
///
/// * `Vec<String>` - list of collections that were recovered
pub fn recover_snapshots(mapping: &[String], force: bool, storage_dir: &str) -> Vec<String> {
    let collection_dir_path = Path::new(storage_dir).join(COLLECTIONS_DIR);
    let mut recovered_collections: Vec<String> = vec![];

    for snapshot_params in mapping {
        let mut split = snapshot_params.split(':');
        let path = split
            .next()
            .unwrap_or_else(|| panic!("Snapshot path is missing: {}", snapshot_params));

        let snapshot_path = Path::new(path);
        let collection_name = split
            .next()
            .unwrap_or_else(|| panic!("Collection name is missing: {}", snapshot_params));
        recovered_collections.push(collection_name.to_string());
        assert!(
            split.next().is_none(),
            "Too many parts in snapshot mapping: {}",
            snapshot_params
        );
        info!("Recovering snapshot {} from {}", collection_name, path);
        // check if collection already exists
        // if it does, we need to check if we want to overwrite it
        // if not, we need to abort
        let collection_path = collection_dir_path.join(collection_name);
        info!("Collection path: {}", collection_path.display());
        if collection_path.exists() {
            if !force {
                panic!(
                    "Collection {} already exists. Use --force-snapshot to overwrite it.",
                    collection_name
                );
            }
            info!("Overwriting collection {}", collection_name);
        }
        let collection_temp_path = collection_path.with_extension("tmp");
        if let Err(err) = Collection::restore_snapshot(snapshot_path, &collection_temp_path) {
            panic!("Failed to recover snapshot {}: {}", collection_name, err);
        }
        // Remove collection_path directory if exists
        if collection_path.exists() {
            if let Err(err) = remove_dir_all(&collection_path) {
                panic!("Failed to remove collection {}: {}", collection_name, err);
            }
        }
        rename(&collection_temp_path, &collection_path).unwrap();
    }
    recovered_collections
}

pub fn recover_full_snapshot(snapshot_path: &str, storage_dir: &str, force: bool) -> Vec<String> {
    let temporary_dir = Path::new(storage_dir).join("snapshots_recovery_tmp");
    std::fs::create_dir_all(&temporary_dir).unwrap();

    // Un-tar snapshot into temporary directory
    let archive_file = std::fs::File::open(snapshot_path).unwrap();
    let mut ar = tar::Archive::new(archive_file);
    ar.unpack(&temporary_dir).unwrap();

    // Read configuration file with snapshot-to-collection mapping
    let config_path = temporary_dir.join("config.json");
    let config_file = std::fs::File::open(&config_path).unwrap();
    let config_json: SnapshotConfig = serde_json::from_reader(config_file).unwrap();

    // Create mapping from the configuration file
    let mapping: Vec<String> = config_json
        .collections_mapping
        .iter()
        .map(|(collection_name, snapshot_file)| {
            format!(
                "{}:{}",
                temporary_dir.join(snapshot_file).to_str().unwrap(),
                collection_name,
            )
        })
        .collect();

    // Launch regular recovery of snapshots
    let recovered_collection = recover_snapshots(&mapping, force, storage_dir);

    let alias_path = Path::new(storage_dir).join(ALIASES_PATH);
    let mut alias_persistence =
        AliasPersistence::open(alias_path).expect("Can't open database by the provided config");
    for (alias, collection_name) in config_json.collections_aliases {
        if alias_persistence.get(&alias).is_some() && !force {
            panic!(
                "Alias {} already exists. Use --force-snapshot to overwrite it.",
                alias
            );
        }
        alias_persistence.insert(alias, collection_name).unwrap();
    }

    // Remove temporary directory
    remove_dir_all(&temporary_dir).unwrap();
    recovered_collection
}
