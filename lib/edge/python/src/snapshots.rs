use std::mem;
use std::path::PathBuf;

use edge::EdgeShard;
use segment::common::operation_error::{OperationError, OperationResult};
use shard::files::{clear_data, move_data};
use shard::snapshots::snapshot_manifest::SnapshotManifest;
use tempfile::Builder;

use crate::PyEdgeShard;

impl PyEdgeShard {
    pub fn _update_from_snapshot(
        &mut self,
        snapshot_path: PathBuf,
        tmp_dir: Option<PathBuf>,
    ) -> OperationResult<()> {
        let tmp_dir = if let Some(dir) = tmp_dir {
            dir
        } else if let Some(dir) = snapshot_path.parent() {
            dir.to_path_buf()
        } else {
            std::env::temp_dir()
        };

        // A place where we can temporarily unpack the snapshot
        let unpack_dir = Builder::new().tempdir_in(tmp_dir)?;
        edge::EdgeShard::unpack_snapshot(&snapshot_path, unpack_dir.path())?;

        let snapshot_manifest = SnapshotManifest::load_from_snapshot(unpack_dir.path(), None)?;

        // Assume full snapshot recovery in case of empty manifest
        let full_recovery = snapshot_manifest.is_empty();

        let Some(shard) = mem::take(&mut self.0) else {
            return Err(OperationError::service_error("Shard is not initialized"));
        };

        let shard_path = shard.path().to_path_buf();

        if full_recovery {
            drop(shard);
            clear_data(&shard_path)?;
            move_data(unpack_dir.path(), &shard_path)?;

            let shard = EdgeShard::load(&shard_path, None)?;

            self.0 = Some(shard);

            return Ok(());
        }

        let current_manifest = match shard.snapshot_manifest() {
            Ok(current_manifest) => current_manifest,
            Err(err) => {
                // Restore the shard before returning
                self.0 = Some(shard);
                return Err(err);
            }
        };

        drop(shard);

        let new_shard = EdgeShard::recover_partial_snapshot(
            &shard_path,
            &current_manifest,
            unpack_dir.path(),
            &snapshot_manifest,
        )?;

        self.0 = Some(new_shard);
        Ok(())
    }
}
