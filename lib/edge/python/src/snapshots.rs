use std::mem;
use std::path::PathBuf;

use segment::common::operation_error::{OperationError, OperationResult};
use tempfile::Builder;

use crate::PyShard;

impl PyShard {
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
        edge::Shard::unpack_snapshot(&snapshot_path, unpack_dir.path())?;

        let Some(shard) = mem::take(&mut self.0) else {
            return Err(OperationError::service_error("Shard is not initialized"));
        };

        let shard_path = shard.path().to_path_buf();

        drop(shard);

        // Apply partial snapshot
        todo!()
    }
}
