use std::sync::Arc;

use tokio::sync::RwLock;
use tracing_subscriber::{Registry, layer, reload};

use super::*;

#[derive(Clone)]
pub struct LoggerHandle {
    config: Arc<RwLock<config::LoggerConfig>>,
    default: DefaultLoggerReloadHandle,
    on_disk: OnDiskLoggerReloadHandle,
}

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line
type DefaultLoggerReloadHandle<S = DefaultLoggerSubscriber> = reload::Handle<
    Logger<S>,
    S,
>;

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line
type DefaultLoggerSubscriber<S = Registry> = layer::Layered<
    reload::Layer<Logger<S>, S>,
    S,
>;

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line
type OnDiskLoggerReloadHandle<S = Registry> = reload::Handle<
    Logger<S>,
    S,
>;

impl LoggerHandle {
    pub fn new(
        config: config::LoggerConfig,
        default: DefaultLoggerReloadHandle,
        on_disk: OnDiskLoggerReloadHandle,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            default,
            on_disk,
        }
    }

    pub async fn get_config(&self) -> config::LoggerConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config(&self, new_config: config::LoggerConfig) -> anyhow::Result<()> {
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

        let mut merged_config = config.clone();
        merged_config.merge(new_config);

        if merged_config.on_disk != config.on_disk {
            let new_layer = on_disk::new_layer(&merged_config.on_disk)?;
            let new_filter = on_disk::new_filter(&merged_config.on_disk);

            self.on_disk.modify(move |logger| {
                *logger.inner_mut() = new_layer;
                *logger.filter_mut() = new_filter;
            })?;

            config.on_disk = merged_config.on_disk;
        }

        if merged_config.default != config.default {
            let new_layer = default::new_layer(&merged_config.default);
            let new_filter = default::new_filter(&merged_config.default);

            self.default.modify(|logger| {
                *logger.inner_mut() = Some(new_layer);
                *logger.filter_mut() = new_filter;
            })?;

            config.default = merged_config.default;
        }

        Ok(())
    }
}
