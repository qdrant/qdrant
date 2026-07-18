use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;

use aligned_vec::{AVec, RuntimeAlign};
use common::universal_io::{Result, UniversalIoError};
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
) -> impl Future<Output = Result<AVec<u8, RuntimeAlign>>> + Send + 'static {
    let len = (range.end - range.start) as usize;
    let stream_fut = file.inner.read_range(&file.path, range);
    async move {
        let stream = stream_fut.await?;
        scatter_stream_into_buffer(with_running_offsets(stream), len, align).await
    }
}

/// Like [`read_into_byte_buffer`], but fetches the whole object, sizing the
/// buffer from the response length (no separate `len`/HEAD).
pub fn read_whole_into_byte_buffer<A: AsyncRead + Clone>(
    file: &BlobFile<A>,
    align: usize,
) -> impl Future<Output = Result<AVec<u8, RuntimeAlign>>> + Send + 'static {
    read_from_into_byte_buffer(file, 0, align)
}

/// Like [`read_into_byte_buffer`], but fetches everything from byte offset
/// `from` to the end of the object, sizing the buffer from the object's total
/// length carried in the response — no separate `len`/HEAD round-trip on the
/// happy path. `from == 0` reads the whole object.
///
/// An offset at or past the end has no tail to read. The backend reports that as
/// an unsatisfiable-range error rather than an empty body, so the error path
/// confirms with a single `len`: if `from >= eof` the tail is genuinely empty
/// and we yield a zero-length buffer; otherwise the original read error stands.
pub fn read_from_into_byte_buffer<A: AsyncRead + Clone>(
    file: &BlobFile<A>,
    from: u64,
    align: usize,
) -> impl Future<Output = Result<AVec<u8, RuntimeAlign>>> + Send + 'static {
    let read_fut = file.inner.read_from(&file.path, from);
    // Cloned for the cold disambiguation path only; building the `len` future is
    // deferred until a read error actually occurs.
    let inner = file.inner.clone();
    let path = file.path.clone();
    async move {
        let (size, stream) = match read_fut.await {
            Ok(ok) => ok,
            Err(err) => {
                let eof = inner.len(&path).await?;
                if from >= eof {
                    return Ok(AVec::new(align));
                }
                return Err(err);
            }
        };
        let len = size.saturating_sub(from) as usize;
        scatter_stream_into_buffer(stream, len, align).await
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
) -> Result<AVec<u8, RuntimeAlign>> {
    let mut buf = AVec::<u8, RuntimeAlign>::with_capacity(align, expected_len);
    // Written ranges so far as `start -> end`, disjoint and merged eagerly so
    // the map stays small: an in-order stream collapses to a single entry, an
    // out-of-order one holds an entry per "hole", bounded by the backend's
    // concurrency window.
    let mut written: BTreeMap<usize, usize> = BTreeMap::new();
    let mut bytes_written = 0usize;
    while let Some(chunk) = stream.next().await {
        let (offset, bytes) = chunk?;
        if bytes.is_empty() {
            continue;
        }
        let (start, end) = usize::try_from(offset)
            .ok()
            .and_then(|start| Some((start, start.checked_add(bytes.len())?)))
            .filter(|&(_, end)| end <= expected_len)
            .ok_or_else(|| UniversalIoError::S3Config {
                description: format!(
                    "over-read: chunk at offset {offset} of {} bytes exceeds a buffer of size \
                     {expected_len}",
                    bytes.len(),
                ),
            })?;
        let overlap = || UniversalIoError::S3Config {
            description: format!(
                "overlapping read: chunk {start}..{end} intersects already-received bytes"
            ),
        };
        // Merge with a range ending exactly at `start` and/or one starting
        // exactly at `end`; any true intersection is a protocol violation.
        let mut new_start = start;
        let mut new_end = end;
        if let Some((&prev_start, &prev_end)) = written.range(..=start).next_back() {
            if prev_end > start {
                return Err(overlap());
            }
            if prev_end == start {
                written.remove(&prev_start);
                new_start = prev_start;
            }
        }
        if let Some((&next_start, &next_end)) = written.range(start..).next() {
            if next_start < end {
                return Err(overlap());
            }
            if next_start == end {
                written.remove(&next_start);
                new_end = next_end;
            }
        }
        // SAFETY: `end <= expected_len <= capacity`, and the checks above
        // guarantee `start..end` is disjoint from every prior write.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.as_mut_ptr().add(start), bytes.len());
        }
        written.insert(new_start, new_end);
        bytes_written += bytes.len();
    }
    // Every write was in-bounds and disjoint, so matching totals prove the
    // chunks tiled `0..expected_len` exactly; anything less means a gap.
    if bytes_written != expected_len {
        return Err(UniversalIoError::S3Config {
            description: format!("short read: expected {expected_len} bytes, got {bytes_written}"),
        });
    }
    // SAFETY: the coverage check above proves every byte in `0..expected_len`
    // was written exactly once.
    unsafe { buf.set_len(expected_len) };
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::StreamExt as _;

    use super::*;

    fn scatter(
        chunks: Vec<(u64, &'static [u8])>,
        expected_len: usize,
    ) -> Result<AVec<u8, RuntimeAlign>> {
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
