use common::universal_io::{CachedReadFs, UniversalReadFsAsync};

use super::TrackerLookup;
use crate::common::operation_error::OperationResult;

impl<Fs: UniversalReadFsAsync> TrackerLookup<Fs> {
    /// Pick up changes written since the last open or reload.
    pub fn live_reload(&mut self) -> OperationResult<()> {
        let Self {
            fs,
            segment_path,
            id_tracker,
            segment_config: _,
            appendable: _,
        } = self;

        let probe = futures::executor::block_on(id_tracker.probe_committed(fs.inner()))?;
        if probe.is_unchanged() {
            return Ok(());
        }
        let max_committed_id = probe.max_committed_id();

        // Selectively refresh only the files this tracker format may mutate, avoiding a LIST.
        let paths = id_tracker.live_reload_paths(segment_path);
        futures::executor::block_on(fs.select_cache_file_info_async(&paths))?;

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
