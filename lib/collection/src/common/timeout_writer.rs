use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::time::{Sleep, sleep};

/// An [`AsyncWrite`] adapter that aborts a write when it makes no progress for a
/// given idle duration.
///
/// This guards the writing end of a streamed snapshot. The writer is driven by
/// a remote consumer through a small in-memory pipe ([`tokio::io::duplex`]); if
/// that consumer stops reading, the inner write stays [`Poll::Pending`]
/// indefinitely. Because the snapshot holds a read lock on the shard's segment
/// holder (and occupies a blocking thread) while streaming, a stalled consumer
/// would otherwise wedge the whole shard and leak the thread forever.
///
/// When no bytes can be handed to the inner writer for `timeout`, the next poll
/// resolves with an [`io::ErrorKind::TimedOut`] error. That error unwinds the
/// snapshot, releases the segment holder lock, and frees the blocking thread.
///
/// The timer measures *idle* time only: it is armed while the inner writer is
/// not ready and reset every time the inner writer accepts data or flushes. So
/// the timeout fires only after `timeout` of continuous back-pressure,
/// regardless of how long the overall transfer takes.
pub struct TimeoutWriter<W> {
    inner: W,
    timeout: Duration,
    /// Idle timer, armed lazily while the inner writer is not ready and cleared
    /// as soon as it makes progress.
    idle: Option<Pin<Box<Sleep>>>,
}

impl<W> TimeoutWriter<W> {
    pub fn new(inner: W, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            idle: None,
        }
    }

    /// Run `inner_poll`, arming/resetting the idle timer around it.
    ///
    /// Resolves with [`io::ErrorKind::TimedOut`] if the inner poll stays
    /// [`Poll::Pending`] for longer than `timeout` without making progress.
    fn poll_with_timeout<T>(
        &mut self,
        cx: &mut Context<'_>,
        inner_poll: impl FnOnce(Pin<&mut W>, &mut Context<'_>) -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>>
    where
        W: Unpin,
    {
        match inner_poll(Pin::new(&mut self.inner), cx) {
            // Made progress (or failed outright): disarm the idle timer.
            Poll::Ready(result) => {
                self.idle = None;
                Poll::Ready(result)
            }
            // No progress: arm the idle timer (keeping any existing deadline) and
            // abort if it has elapsed.
            Poll::Pending => {
                let timeout = self.timeout;
                let idle = self.idle.get_or_insert_with(|| Box::pin(sleep(timeout)));
                match idle.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        self.idle = None;
                        Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("write stalled: no data was read for {}s", timeout.as_secs()),
                        )))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TimeoutWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut()
            .poll_with_timeout(cx, |inner, cx| inner.poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .poll_with_timeout(cx, |inner, cx| inner.poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut()
            .poll_with_timeout(cx, |inner, cx| inner.poll_shutdown(cx))
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// A stalled consumer must abort the write after the idle timeout. Our
    /// `sleep` is the only timer in play, so a short real timeout is enough to
    /// keep this deterministic.
    #[tokio::test]
    async fn aborts_write_when_consumer_stalls() {
        // Keep `read_half` alive but never read from it, so the pipe fills up and
        // stays full (dropping it would yield a `BrokenPipe` error instead).
        let (_read_half, write_half) = tokio::io::duplex(8);
        let mut writer = TimeoutWriter::new(write_half, Duration::from_millis(100));

        // First chunk fits in the buffer.
        writer.write_all(&[0u8; 8]).await.unwrap();

        // The buffer is now full and nobody drains it: the next write stalls and
        // must abort once the idle timeout elapses.
        let err = writer.write_all(&[0u8; 8]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    /// A consumer that keeps reading must never trip the idle timeout, no matter
    /// how small the pipe buffer is relative to the payload. The timeout is set
    /// generously so it cannot fire during the near-instant transfer.
    #[tokio::test]
    async fn does_not_abort_while_consumer_reads() {
        let (mut read_half, write_half) = tokio::io::duplex(8);
        let mut writer = TimeoutWriter::new(write_half, Duration::from_secs(30));

        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let mut total = 0;
            while let Ok(n) = read_half.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                total += n;
            }
            total
        });

        writer.write_all(&[0u8; 64]).await.unwrap();
        writer.shutdown().await.unwrap();
        drop(writer);

        assert_eq!(reader.await.unwrap(), 64);
    }
}
