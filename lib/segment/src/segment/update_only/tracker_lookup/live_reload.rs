use common::universal_io::{CachedReadFs, UniversalReadFsAsync};

use super::TrackerLookup;
use crate::common::operation_error::OperationResult;

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// Pick up changes written since the last open or reload.
    pub fn live_reload(&mut self) -> OperationResult<()> {
        let Self { fs, id_tracker, .. } = self;

        let probe = futures::executor::block_on(id_tracker.probe_committed(fs.inner()))?;
        if probe.is_unchanged() {
            return Ok(());
        }
        let max_committed_id = probe.max_committed_id();

        // Prepare new LIST snapshot
        fs.cache_file_info()?;

        let futs = id_tracker.live_preload(fs)?;
        futures::executor::block_on(async {
            futures::join!(fs.wait_all(), futures::future::join_all(futs))
        });

        id_tracker.live_reload(fs, max_committed_id)?;
        id_tracker.publish_staged();

        fs.rotate_cache_file_info();
        Ok(())
    }
}
