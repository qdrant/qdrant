pub fn setup() -> anyhow::Result<()> {
    // Use `console` and/or `tracy` features to enable both `tracing-subscriber` and the layer(s)
    #[cfg(feature = "tracing-subscriber")]
    {
        use tracing_subscriber::prelude::*;

        let reg = tracing_subscriber::registry();

        // Use `console` feature to enable both `tracing-subscriber` and `console-subscriber`
        #[cfg(feature = "console-subscriber")]
        let reg = reg.with(console_subscriber::spawn());

        // Note, that `console-subscriber` requires manually enabling
        // `--cfg tokio_unstable` rust flags during compilation!
        //
        // Otherwise `console_subscriber::spawn` call panics!
        //
        // See https://docs.rs/tokio/latest/tokio/#unstable-features
        #[cfg(all(feature = "console-subscriber", not(tokio_unstable)))]
        eprintln!(
            "`console-subscriber` requires manually enabling \
             `--cfg tokio_unstable` rust flags during compilation!"
        );

        // Use `tracy` feature to enable both `tracing-subscriber` and `tracing-tracy`
        #[cfg(feature = "tracing-tracy")]
        let reg = reg.with(tracing_tracy::TracyLayer::new().with_filter(
            tracing_subscriber::filter::filter_fn(|metadata| metadata.is_span()),
        ));

        #[cfg(all(feature = "tracing-log-always", feature = "tracing-logger"))]
        eprintln!(
            "Both `tracing-log-always` and `tracing-logger` features are enabled at the same time. \
             This will cause some logs to be printed twice!"
        );

        #[cfg(feature = "tracing-logger")]
        let reg = reg.with(
            tracing_subscriber::fmt::layer()
                .with_ansi(true)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
                .with_filter(tracing_subscriber::filter::EnvFilter::from_default_env()),
        );

        #[cfg(feature = "tracing-logger")]
        tracing_log::LogTracer::init()?;

        tracing::subscriber::set_global_default(reg)?;
    }

    Ok(())
}
