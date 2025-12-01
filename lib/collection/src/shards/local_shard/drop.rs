use std::thread;

use crate::shards::local_shard::LocalShard;

impl Drop for LocalShard {
    fn drop(&mut self) {
        // Drop might happen in the runtime, so we need to spawn a thread to do blocking operations.
        let already_stopped = thread::scope(|s| {
            let handle = thread::Builder::new()
                .name("drop-shard".to_string())
                .spawn_scoped(s, || self.blocking_ask_workers_to_stop());
            handle
                .expect("Failed to create thread for shard drop")
                .join()
                .expect("Drop shard thread panicked")
        });

        // Normally we expect, that LocalShard should be explicitly stopped in asynchronous
        // runtime before being dropped.
        // We want this because:
        // - We don't want a conflict of background tasks.
        // - We might want to delete files, and can't have other threads working with them.
        //
        // However.
        // It is not possible to explicitly catch all cases of LocalShard being dropped.
        // For example, then the service is shutdown by interruption, we might not have enough
        // control over the shutdown sequence. Or we might simply have forgotten to gracefully
        // shutdown before drop.
        // So we have to assume, that it is fine to not await for explicit shutdown in some cases.
        // But we still want to call explicit shutdown on all removes and internal operations.
        if !already_stopped {
            log::debug!("Dropping LocalShard while it is not already stopped");
        }
    }
}
