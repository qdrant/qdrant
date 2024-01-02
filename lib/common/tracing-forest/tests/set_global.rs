//! This test sets a global subscriber so that the sender half of the channel
//! inside the subscriber is never dropped. It tests that the receiver can be closed
//! manually via the shutdown signal that is sent once the `Future` passed to
//! `Runtime::on` finishes.
//!
//! Addresses https://github.com/QnnOkabayashi/tracing-forest/issues/4
#![cfg(feature = "tokio")]
use tokio::time::{timeout, Duration};

#[tokio::test(flavor = "multi_thread")]
async fn join_hanging_task() {
    let f = tracing_forest::worker_task()
        .set_global(true)
        .build()
        .on(async {
            // nothing
        });

    timeout(Duration::from_millis(500), f)
        .await
        .expect("Shutdown signal wasn't sent");
}
