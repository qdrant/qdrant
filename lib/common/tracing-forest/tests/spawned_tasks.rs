//! This test simulates capturing trace data from different threads, and ensures
//! that everything is still captured by the subscriber.
//!
//! Addresses https://github.com/QnnOkabayashi/tracing-forest/issues/3
#![cfg(feature = "tokio")]
use tokio::time::{sleep, Duration};
use tracing::info;

type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn spawned_tasks() -> Result<()> {
    let logs = tracing_forest::capture()
        .set_global(true)
        .build()
        .on(async {
            info!("Waiting on signal");
            let handle = tokio::spawn(async {
                info!("Test message");
            });
            sleep(Duration::from_millis(100)).await;
            handle.await.unwrap();
            info!("Stopping");
        })
        .await;

    assert!(logs.len() == 3);

    let waiting = logs[0].event()?;
    assert!(waiting.message() == Some("Waiting on signal"));

    let test = logs[1].event()?;
    assert!(test.message() == Some("Test message"));

    let stopping = logs[2].event()?;
    assert!(stopping.message() == Some("Stopping"));

    Ok(())
}
