use std::future::Future;
use std::time::{Duration, Instant};

/// Log a warning if a single wait on a lock or on disk takes longer than this.
const SLOW_WAIT_REPORT_THRESHOLD: Duration = Duration::from_secs(1);

/// Await `future`, reporting it if it blocks for longer than
/// `SLOW_WAIT_REPORT_THRESHOLD`.
///
/// `what` names the thing being waited on, and is only formatted when the warning fires.
pub async fn slow_await<T>(what: &str, future: impl Future<Output = T>) -> T {
    let started = Instant::now();
    let output = future.await;
    report_if_slow(what, started);
    output
}

/// [`slow_await`] for a blocking call — a synchronous lock, or disk IO on a thread that
/// cannot afford to wait on it.
pub fn slow_block<T>(what: &str, f: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let output = f();
    report_if_slow(what, started);
    output
}

fn report_if_slow(what: &str, started: Instant) {
    let elapsed = started.elapsed();
    if elapsed >= SLOW_WAIT_REPORT_THRESHOLD {
        log::warn!("Slow wait: {what} took {elapsed:.2?}");
    }
}
