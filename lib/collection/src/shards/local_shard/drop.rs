use std::thread;

use crate::shards::local_shard::LocalShard;
use crate::update_handler::UpdateSignal;

impl Drop for LocalShard {
    fn drop(&mut self) {
        if self.is_gracefully_stopped {
            return;
        }

        log::debug!(
            "Local shard {} is not explicitly stopped before drop. Attempting to stop background workers.",
            self.shard_path().display()
        );

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

        let update_handler = self.update_handler.clone();
        let update_sender = self.update_sender.load().clone();

        // Operation might happen in the runtime, so we need to spawn a thread to do blocking operations.
        let handle_res = thread::Builder::new()
            .name("drop-shard".to_string())
            .spawn(move || {
                {
                    let mut update_handler = update_handler.blocking_lock();
                    if update_handler.is_stopped() {
                        return true;
                    }
                    update_handler.stop_flush_worker();
                }

                // This can block longer, if the channel is full
                // If channel is closed, assume it is already stopped
                if let Err(err) = update_sender.blocking_send(UpdateSignal::Stop) {
                    log::trace!("Error sending update signal to update handler: {err}");
                }
                false
            });

        match handle_res {
            Ok(_) => {
                // We spawned a thread, but we can't wait for it here, because we are in Drop.
                // So we just let it run.
            }
            Err(err) => {
                log::warn!("Failed to background ask workers to stop: {err:?}");
            }
        }
    }
}
