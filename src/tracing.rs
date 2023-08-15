use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, fmt};

pub fn setup(log_level: &str) -> anyhow::Result<()> {
    tracing_log::LogTracer::init()?;

    let reg = tracing_subscriber::registry().with(
        fmt::layer()
            .with_ansi(true)
            .with_span_events(fmt::format::FmtSpan::NEW)
            .with_filter(filter::EnvFilter::builder().parse_lossy(log_level)),
    );

    // Use `console` or `console-subscriber` feature to enable `console-subscriber`
    //
    // Note, that `console-subscriber` requires manually enabling
    // `--cfg tokio_unstable` rust flags during compilation!
    //
    // Otherwise `console_subscriber::spawn` call panics!
    //
    // See https://docs.rs/tokio/latest/tokio/#unstable-features
    #[cfg(all(feature = "console-subscriber", tokio_unstable))]
    let reg = reg.with(console_subscriber::spawn());

    #[cfg(all(feature = "console-subscriber", not(tokio_unstable)))]
    eprintln!(
        "`console-subscriber` requires manually enabling \
         `--cfg tokio_unstable` rust flags during compilation!"
    );

    // Use `tracy` or `tracing-tracy` feature to enable `tracing-tracy`
    #[cfg(feature = "tracing-tracy")]
    let reg = reg.with(tracing_tracy::TracyLayer::new().with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| metadata.is_span()),
    ));

    tracing::subscriber::set_global_default(reg)?;

    Ok(())
}
