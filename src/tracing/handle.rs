use std::sync::Arc;

use tokio::sync::RwLock;
use tracing_subscriber::{reload, Registry};

use super::*;

#[derive(Clone)]
pub struct LoggerHandle {
    config: Arc<RwLock<config::LoggerConfig>>,
    default: DefaultLoggerReloadHandle,
}

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line
type DefaultLoggerReloadHandle<S = Registry> = reload::Handle<
    default::Logger<S>,
    S,
>;

impl LoggerHandle {
    pub fn new(config: config::LoggerConfig, default: DefaultLoggerReloadHandle) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            default,
        }
    }

    pub async fn get_config(&self) -> config::LoggerConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config(&self, mut diff: config::LoggerConfigDiff) -> anyhow::Result<()> {
        let mut config = self.config.write().await;

        // `tracing-subscriber` does not support `reload`ing `Filtered` layers, so we *have to* use
        // `modify`. However, `modify` would *deadlock* if provided closure logs anything or produce
        // any `tracing` event.
        //
        // So, we structure `update_config` to only do an absolute minimum of changes and only use
        // the most trivial operations during `modify`, to guarantee we won't deadlock.
        //
        // See:
        // - https://docs.rs/tracing-subscriber/latest/tracing_subscriber/reload/struct.Handle.html#method.reload
        // - https://github.com/tokio-rs/tracing/issues/1629
        // - https://github.com/tokio-rs/tracing/pull/2657

        // Only update logger configuration, if `diff` contains changes to current parameters
        diff.filter(&config);

        // Parse `diff` and prepare `update` *outside* of `modify` call
        if let Some(update) = diff.default.prepare_update() {
            // Apply prepared `update` using trivial code that should never trigger a deadlock
            self.default.modify(move |logger| update.apply(logger))?;
        }

        // Update `config`
        config.default.update(diff.default);

        Ok(())
    }
}
