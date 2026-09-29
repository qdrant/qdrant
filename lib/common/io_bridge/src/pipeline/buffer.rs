use std::future::Future;
use std::ops::Range;

use aligned_vec::{AVec, RuntimeAlign};
use common::uio_trace::{Op, Outcome};
use common::universal_io::{IsNotFound as _, UioResult, UniversalIoError};
use futures::StreamExt as _;

use crate::file::BlobFile;
use crate::read::{AsyncRead, OffsetByteStream, with_running_offsets};

/// Build the future that allocates an exact-size, `align`-aligned destination
/// byte buffer, streams the backend read for `range` into it, and returns it as
/// the future's output.
///
/// Shared by the borrowed and owned pipeline `schedule` impls. The buffer lives
/// inside the future for the duration of the read — no shared mutable state
/// between the pipeline thread and the worker task, so no raw-pointer unsafe is
/// needed to cross threads. The buffer arrives back at the pipeline as a
/// normal move through the reply channel.
pub fn read_into_byte_buffer<A: AsyncRead>(
    file: &BlobFile<A>,
    range: Range<u64>,
    align: usize,
) -> impl Future<Output = UioResult<AVec<u8, RuntimeAlign>>> + Send + 'static {
    let len = (range.end - range.start) as usize;
    let request = file.stats.request(Op::Read, &file.path, range.clone());
    let stream_fut = file.inner.read_range(&file.path, range);
    request.wrap(async move {
        let stream = stream_fut.await?;
        scatter_stream_into_buffer(with_running_offsets(stream), len, align).await
    })
}

/// Like [`read_into_byte_buffer`], but fetches the whole object, sizing the
/// buffer from the response length (no separate `len`/HEAD).
pub fn read_whole_into_byte_buffer<A: AsyncRead + Clone>(
    file: &BlobFile<A>,
    align: usize,
) -> impl Future<Output = UioResult<AVec<u8, RuntimeAlign>>> + Send + 'static {
    read_from_into_byte_buffer(file, 0, align)
}

/// Like [`read_into_byte_buffer`], but fetches everything from byte offset
/// `from` to the end of the object, sizing the buffer from the object's total
/// length carried in the response — no separate `len`/HEAD round-trip on the
/// happy path. `from == 0` reads the whole object.
pub fn read_from_into_byte_buffer<A: AsyncRead + Clone>(
    file: &BlobFile<A>,
    from: u64,
    align: usize,
) -> impl Future<Output = UioResult<AVec<u8, RuntimeAlign>>> + Send + 'static {
    read_from_with(
        file,
        from,
        move || AVec::new(align),
        move |stream, len| scatter_stream_into_buffer(stream, len, align),
    )
}

/// Like [`read_from_into_byte_buffer`] from offset 0, but hands every chunk to `sink` as
/// `(offset, bytes)` the moment it arrives instead of collecting a buffer. Yields the
/// object length.
pub fn read_whole_into_sink<A, F>(
    file: &BlobFile<A>,
    sink: F,
) -> impl Future<Output = UioResult<u64>> + Send + 'static
where
    A: AsyncRead + Clone,
    F: FnMut(u64, &[u8]) -> UioResult<()> + Send + 'static,
{
    read_from_with(
        file,
        0,
        || 0,
        move |stream, len| async move {
            scatter_stream_into_sink(stream, len, sink).await?;
            Ok(len as u64)
        },
    )
}

/// Open a [`AsyncRead::read_from`] stream from `from` and hand it to `consume` along with
/// the tail length, tracing the request.
///
/// An offset at or past the end has no tail to read. The backend reports that as
/// an unsatisfiable-range error rather than an empty body, so the error path
/// confirms with a single `len`: if `from >= eof` the tail is genuinely empty
/// and `empty` supplies the result; otherwise the original read error stands.
fn read_from_with<A, T, E, C, Fut>(
    file: &BlobFile<A>,
    from: u64,
    empty: E,
    consume: C,
) -> impl Future<Output = UioResult<T>> + Send + 'static
where
    A: AsyncRead + Clone,
    E: FnOnce() -> T + Send + 'static,
    C: FnOnce(OffsetByteStream, usize) -> Fut + Send + 'static,
    Fut: Future<Output = UioResult<T>> + Send,
{
    let mut request = file.stats.request(Op::ReadFrom, &file.path, from..from);
    let read_fut = file.inner.read_from(&file.path, from);
    // Cloned for the cold disambiguation path only; building the `len` future is
    // deferred until a read error actually occurs.
    let inner = file.inner.clone();
    let stats = file.stats.clone();
    let path = file.path.clone();
    async move {
        request.start();
        let (size, stream) = match read_fut.await {
            Ok(ok) => ok,
            // A missing object has no tail to be empty; the probe would only repeat the answer.
            Err(err) if err.is_not_found() => {
                request.set_err(&err);
                return Err(err);
            }
            Err(err) => {
                // Settled after the `len` probe: a tail at or past EOF is an empty
                // read, not a failed one.
                let eof = stats
                    .request(Op::Len, &path, 0..0)
                    .wrap(inner.len(&path))
                    .await;
                if let Ok(eof) = eof
                    && from >= eof
                {
                    request.set(Outcome::Ok);
                    return Ok(empty());
                }
                request.set_err(&err);
                eof?;
                return Err(err);
            }
        };
        request.set_end(size);
        let len = size.saturating_sub(from) as usize;
        let result = consume(stream, len).await;
        request.set_result(&result);
        result
    }
}

/// Scatter every `(offset, bytes)` chunk of `stream` into a fresh
/// `align`-aligned buffer of exactly `expected_len` bytes.
///
/// Chunks may arrive **out of order** (offsets are relative to the start of
/// the stream, see [`AsyncRead::read_from`]); each is copied straight to its
/// final position the moment it arrives, so a multi-request backend is never
/// stalled behind in-order delivery. The buffer is allocated as capacity only
/// — no zero pre-fill — and its length is set only after verifying the chunks
/// were disjoint and covered the buffer exactly, so a malformed stream yields
/// an error, never uninitialized or double-written bytes.
async fn scatter_stream_into_buffer(
    mut stream: OffsetByteStream,
    expected_len: usize,
    align: usize,
) -> UioResult<AVec<u8, RuntimeAlign>> {
    let mut buf = AVec::<u8, RuntimeAlign>::with_capacity(align, expected_len);
    let mut coverage = Coverage::new(expected_len);
    while let Some(chunk) = stream.next().await {
        let (offset, bytes) = chunk?;
        if bytes.is_empty() {
            continue;
        }
        let range = coverage.claim(offset, bytes.len())?;
        // SAFETY: `claim` guarantees `range.end <= expected_len <= capacity`, and that
        // `range` is disjoint from every prior write.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                buf.as_mut_ptr().add(range.start),
                bytes.len(),
            );
        }
    }
    coverage.finish()?;
    // SAFETY: `finish` proves every byte in `0..expected_len` was written exactly once.
    unsafe { buf.set_len(expected_len) };
    Ok(buf)
}

/// Like [`scatter_stream_into_buffer`], but hands each chunk to `sink` instead of copying
/// it into a buffer. Errors on the same malformed streams.
async fn scatter_stream_into_sink<F>(
    mut stream: OffsetByteStream,
    expected_len: usize,
    mut sink: F,
) -> UioResult<()>
where
    F: FnMut(u64, &[u8]) -> UioResult<()>,
{
    let mut coverage = Coverage::new(expected_len);
    while let Some(chunk) = stream.next().await {
        let (offset, bytes) = chunk?;
        if bytes.is_empty() {
            continue;
        }
        coverage.claim(offset, bytes.len())?;
        sink(offset, &bytes)?;
    }
    coverage.finish()
}

/// Tracks which bytes of `0..expected_len` the chunks of a stream have covered, rejecting
/// out-of-bounds and overlapping chunks, and a short total.
struct Coverage {
    expected_len: usize,
    /// Disjoint runs of already-covered bytes, grown/merged as chunks land.
    /// There are few in practice — an in-order stream is a single run, an
    /// out-of-order one adds a run per concurrent "hole" — so a linear scan is fast.
    /// Revisit if the run bound (`READ_CHUNK_CONCURRENCY` in `io_bridge_object_store`) grows large.
    runs: Vec<Range<usize>>,
    covered: usize,
}

impl Coverage {
    fn new(expected_len: usize) -> Self {
        Self {
            expected_len,
            runs: Vec::new(),
            covered: 0,
        }
    }

    /// Record a chunk of `len` bytes at `offset`; returns its byte range.
    fn claim(&mut self, offset: u64, len: usize) -> UioResult<Range<usize>> {
        let Self {
            expected_len,
            runs,
            covered,
        } = self;
        let start = usize::try_from(offset).ok();
        let end = start.and_then(|start| start.checked_add(len));
        let Some((start, end)) = start.zip(end).filter(|&(_, end)| end <= *expected_len) else {
            return Err(UniversalIoError::S3 {
                path: None,
                source: Box::from(format!(
                    "over-read: chunk at offset {offset} of {len} bytes exceeds a buffer of size \
                     {expected_len}",
                )),
            });
        };
        if runs.iter().any(|run| run.start < end && start < run.end) {
            return Err(UniversalIoError::S3 {
                path: None,
                source: Box::from(format!(
                    "overlapping read: chunk {start}..{end} intersects already-received bytes"
                )),
            });
        }
        *covered += len;
        // Grow an adjacent run or start a new one.
        // Runs are disjoint, so each side matches at most once.
        let before = runs.iter().position(|run| run.end == start);
        let after = runs.iter().position(|run| run.start == end);
        match (before, after) {
            (Some(before), Some(after)) => {
                runs[before].end = runs[after].end;
                runs.swap_remove(after);
            }
            (Some(before), None) => runs[before].end = end,
            (None, Some(after)) => runs[after].start = start,
            (None, None) => runs.push(start..end),
        }
        Ok(start..end)
    }

    /// Every claim was in-bounds and disjoint, so matching totals prove the
    /// chunks tiled `0..expected_len` exactly; anything less means a gap.
    fn finish(self) -> UioResult<()> {
        let Self {
            expected_len,
            runs: _,
            covered,
        } = self;
        if covered != expected_len {
            return Err(UniversalIoError::S3 {
                path: None,
                source: format!("short read: expected {expected_len} bytes, got {covered}").into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn scatter(
        chunks: Vec<(u64, &'static [u8])>,
        expected_len: usize,
    ) -> UioResult<AVec<u8, RuntimeAlign>> {
        let stream = futures::stream::iter(
            chunks
                .into_iter()
                .map(|(offset, bytes)| Ok((offset, Bytes::from_static(bytes)))),
        )
        .boxed();
        futures::executor::block_on(scatter_stream_into_buffer(stream, expected_len, 8))
    }

    #[test]
    fn scatter_reassembles_out_of_order_chunks() {
        let buf = scatter(vec![(5, b"world"), (0, b"hello")], 10).expect("scatter");
        assert_eq!(&buf[..], b"helloworld");
    }

    #[test]
    fn scatter_accepts_empty_stream_for_empty_buffer() {
        let buf = scatter(vec![], 0).expect("scatter");
        assert!(buf.is_empty());
    }

    #[test]
    fn scatter_rejects_overlapping_chunks() {
        let err = scatter(vec![(0, b"hello"), (3, b"xyz")], 8).unwrap_err();
        assert!(err.to_string().contains("overlapping"), "{err}");
    }

    #[test]
    fn scatter_rejects_gaps_as_short_read() {
        let err = scatter(vec![(0, b"he"), (5, b"lo")], 7).unwrap_err();
        assert!(err.to_string().contains("short read"), "{err}");
    }

    #[test]
    fn scatter_rejects_chunks_past_the_end() {
        let err = scatter(vec![(8, b"abc")], 10).unwrap_err();
        assert!(err.to_string().contains("over-read"), "{err}");
    }
}
