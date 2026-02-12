use std::path::{Path, PathBuf};

use common::tar_unpack::tar_unpack_file;
use fs_err as fs;
use segment::common::operation_error::OperationResult;
use segment::segment::Segment;

use crate::files::{ShardDataFiles, get_shard_data_files, segments_path};
use crate::snapshots::snapshot_manifest::SnapshotManifest;

pub struct SnapshotUtils;

impl SnapshotUtils {
    /// Unpacks and restores a shard snapshot from the given archive into the target path.
    ///
    /// `snapshot_path` - path to the snapshot archive.
    /// `target_path` - path to the directory where the snapshot should be unpacked.
    pub fn unpack_snapshot(snapshot_path: &Path, target_path: &Path) -> OperationResult<()> {
        if !target_path.exists() {
            fs::create_dir_all(target_path)?;
        }

        tar_unpack_file(snapshot_path, target_path)?;
        Self::restore_unpacked_snapshot(target_path)?;
        Ok(())
    }

    /// Restores internals of an unpacked shard snapshot inplace.
    ///
    /// `snapshot_path` - path to the directory, where snapshot was unpacked to.
    pub fn restore_unpacked_snapshot(snapshot_path: &Path) -> OperationResult<()> {
        // Read dir first as the directory contents would change during restore
        let entries = fs::read_dir(segments_path(snapshot_path))?.collect::<Result<Vec<_>, _>>()?;

        // Filter out hidden entries
        let entries = entries.into_iter().filter(|entry| {
            let is_hidden = entry
                .file_name()
                .to_str()
                .is_some_and(|s| s.starts_with('.'));
            if is_hidden {
                log::debug!(
                    "Ignoring hidden segment in local shard during snapshot recovery: {}",
                    entry.path().display(),
                );
            }
            !is_hidden
        });

        for entry in entries {
            Segment::restore_snapshot_in_place(&entry.path())?;
        }

        Ok(())
    }

    /// Create a plan to merge an existing shard with a partial snapshot.
    /// This function doesn't actually perform any file operations; it just prepares the plan.
    ///
    /// Plan can be executed either by either sync or async file operations.
    pub fn partial_snapshot_merge_plan(
        shard_path: &Path,
        shard_manifest: &SnapshotManifest,
        snapshot_path: &Path,
        snapshot_manifest: &SnapshotManifest,
    ) -> SnapshotMergePlan {
        let mut move_files = Vec::new();
        let mut replace_directories = Vec::new();
        let mut merge_directories = Vec::new();
        let mut delete_files = Vec::new();
        let mut delete_directories = Vec::new();

        let segments_path = segments_path(shard_path);

        for (segment_id, local_segment_manifest) in shard_manifest.iter() {
            let segment_path = segments_path.join(segment_id);

            // Delete local segment, if it's not present in partial snapshot
            let Some(segment_manifest) = snapshot_manifest.get(segment_id) else {
                delete_directories.push(segment_path);
                continue;
            };

            for (file, _local_version) in local_segment_manifest.file_versions() {
                let snapshot_version = segment_manifest.file_version(file);
                let is_removed = snapshot_version.is_none();

                if is_removed {
                    // If `file` is a regular file, delete it from disk, if it was
                    // *removed* from the snapshot
                    let path = segment_path.join(file);
                    delete_files.push(path);
                }
            }
        }

        let ShardDataFiles {
            wal_path: from_wal_path,
            segments_path: from_segments_path,
            newest_clocks_path: from_newest_clocks_path,
            oldest_clocks_path: from_oldest_clocks_path,
            applied_seq_path: from_applied_seq_path,
        } = get_shard_data_files(snapshot_path);

        let ShardDataFiles {
            wal_path: to_wal_path,
            segments_path: to_segments_path,
            newest_clocks_path: to_newest_clocks_path,
            oldest_clocks_path: to_oldest_clocks_path,
            applied_seq_path: to_applied_seq_path,
        } = get_shard_data_files(shard_path);

        merge_directories.push((from_segments_path, to_segments_path));
        replace_directories.push((from_wal_path, to_wal_path));

        if from_newest_clocks_path.exists() {
            move_files.push((from_newest_clocks_path, to_newest_clocks_path));
        }

        if from_oldest_clocks_path.exists() {
            move_files.push((from_oldest_clocks_path, to_oldest_clocks_path));
        }

        if from_applied_seq_path.exists() {
            move_files.push((from_applied_seq_path, to_applied_seq_path));
        }

        SnapshotMergePlan {
            move_files,
            replace_directories,
            merge_directories,
            delete_files,
            delete_directories,
        }
    }
}

#[derive(Debug, Default)]
pub struct SnapshotMergePlan {
    pub move_files: Vec<(PathBuf, PathBuf)>,
    pub replace_directories: Vec<(PathBuf, PathBuf)>,
    pub merge_directories: Vec<(PathBuf, PathBuf)>,
    pub delete_files: Vec<PathBuf>,
    pub delete_directories: Vec<PathBuf>,
}
