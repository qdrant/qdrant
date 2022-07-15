use collection::collection::Collection;
use log::info;
use std::fs::{remove_dir_all, rename};
use std::path::Path;
use storage::content_manager::toc::COLLECTIONS_DIR;

/// Recover snapshots from the given arguments
///
/// # Arguments
///
/// * `mapping` - [ "<path>:<collection_name>" ]
/// * `force` - if true, allow to overwrite collections from snapshots
///
pub fn recover_snapshots(mapping: &[String], force: bool, collections_dir: &str) {
    let collection_dir_path = Path::new(collections_dir).join(COLLECTIONS_DIR);
    for snapshot_params in mapping {
        let mut split = snapshot_params.split(':');
        let path = split
            .next()
            .unwrap_or_else(|| panic!("Snapshot path is missing: {}", snapshot_params));

        let snapshot_path = Path::new(path);
        let collection_name = split
            .next()
            .unwrap_or_else(|| panic!("Collection name is missing: {}", snapshot_params));
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
}
