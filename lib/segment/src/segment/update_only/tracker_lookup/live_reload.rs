use common::universal_io::{CachedReadFs, UniversalReadFsAsync};

use super::TrackerLookup;
use crate::common::operation_error::OperationResult;

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// Pick up changes written since the last open or reload.
    pub fn live_reload(&mut self) -> OperationResult<()> {
        let Self { fs, id_tracker, .. } = self;

        // Prepare new LIST snapshot
        fs.cache_file_info()?;

        let futs = id_tracker.live_preload(fs)?;
        futures::executor::block_on(async {
            futures::join!(fs.wait_all(), futures::future::join_all(futs))
        });

        id_tracker.live_reload(fs)?;

        fs.rotate_cache_file_info();
        Ok(())
    }
}
