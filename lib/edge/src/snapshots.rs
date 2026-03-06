use std::path::Path;

use fs_err as fs;
use segment::common::operation_error::OperationResult;
use shard::snapshots::snapshot_manifest::SnapshotManifest;
use shard::snapshots::snapshot_utils::{SnapshotMergePlan, SnapshotUtils};

use crate::EdgeShard;

impl EdgeShard {
    pub fn unpack_snapshot(snapshot_path: &Path, target_path: &Path) -> OperationResult<()> {
        SnapshotUtils::unpack_snapshot(snapshot_path, target_path)
    }

    pub fn snapshot_manifest(&self) -> OperationResult<SnapshotManifest> {
        self.segments.read().snapshot_manifest()
    }

    pub fn recover_partial_snapshot(
        shard_path: &Path,
        current_manifest: &SnapshotManifest,
        snapshot_path: &Path,
        snapshot_manifest: &SnapshotManifest,
    ) -> OperationResult<Self> {
        let merge_plan = SnapshotUtils::partial_snapshot_merge_plan(
            shard_path,
            current_manifest,
            snapshot_path,
            snapshot_manifest,
        );

        let SnapshotMergePlan {
            move_files,
            replace_directories,
            merge_directories,
            delete_files,
            delete_directories,
        } = merge_plan;

        for (move_file_from, move_file_to) in move_files {
            common::fs::move_file(&move_file_from, &move_file_to)?;
        }

        for (replace_dir_from, replace_dir_to) in replace_directories {
            if replace_dir_to.exists() {
                fs::remove_dir_all(&replace_dir_to)?;
            }
            common::fs::move_dir(&replace_dir_from, &replace_dir_to)?;
        }

        for (merge_dir_from, merge_dir_to) in merge_directories {
            common::fs::move_dir(&merge_dir_from, &merge_dir_to)?;
        }

        for delete_file in delete_files {
            fs::remove_file(&delete_file)?;
        }

        for delete_dir in delete_directories {
            fs::remove_dir_all(&delete_dir)?;
        }

        EdgeShard::load(shard_path, None)
    }
}
