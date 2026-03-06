use std::sync::Arc;
use std::time::{Duration, Instant};

pub async fn try_unwrap_with_timeout_async<T>(
    mut arc: Arc<T>,
    spin: Duration,
    timeout: Duration,
) -> Result<T, Arc<T>> {
    let start = Instant::now();

    loop {
        arc = match Arc::try_unwrap(arc) {
            Ok(unwrapped) => return Ok(unwrapped),
            Err(arc) => arc,
        };

        if start.elapsed() >= timeout {
            return Err(arc);
        }

        tokio::time::sleep(spin).await;
    }
}
