use crate::files::segments_path;
use fs_err as fs;
use segment::common::operation_error::OperationResult;
use segment::common::validate_snapshot_archive::open_snapshot_archive;
use segment::segment::Segment;
use std::path::Path;

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

        let mut ar = open_snapshot_archive(snapshot_path)?;
        ar.unpack(target_path)?;
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
}
