use crate::shards::local_shard::LocalShard;

impl Drop for LocalShard {
    fn drop(&mut self) {
        let already_stopped = self.blocking_ask_workers_to_stop();

        #[cfg(any(debug_assertions, feature = "staging"))]
        if !already_stopped {
            eprintln!(
                "LocalShard::drop {:?}",
                std::backtrace::Backtrace::capture()
            );
            panic!(
                "Dropping LocalShard which was not already stopped. Waiting for graceful shutdown."
            );
        }

        if !already_stopped {
            log::error!("Dropping LocalShard which was not already stopped.");
        }
    }
}
