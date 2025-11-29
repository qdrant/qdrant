use crate::shards::local_shard::LocalShard;

impl Drop for LocalShard {
    fn drop(&mut self) {
        let already_stopped = self.blocking_ask_workers_to_stop();

        // Normally we expect, that LocalShard should be explicitly stopped in asynchronous
        // runtime before being dropped.
        // We want this because:
        // - We don't want a conflict of background tasks.
        // - We might want to delete files, and can't have other threads working with them.
        //
        // However.
        // It is not possible to explicitly catch all cases of LocalShard being dropped.
        // For example, then the service is shutdown by interruption, we might not have a
        // deep enough control over the shutdown sequence.
        // So we have to assume, that it is fine to not await for explicit shutdown in some cases.
        // But we still want to call explicit shutdown on all removes and internal operations.
        if !already_stopped {
            log::debug!("Dropping LocalShard while it is not already stopped");
        }
    }
}
