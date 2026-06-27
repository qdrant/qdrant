use std::future::Future;
use std::ops::Range;

use aligned_vec::{AVec, RuntimeAlign, avec_rt};
use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError};
use futures::StreamExt as _;
use futures::stream::BoxStream;

use crate::file::BlobFile;
use crate::read::AsyncRead;

/// Build the future that allocates an exact-size, `align`-aligned destination
/// byte buffer, streams the backend read for `range` into it, and returns it as
/// the future's output.
///
/// Shared by the borrowed and owned pipeline `schedule` impls. The buffer lives
/// inside the future for the duration of the read — no shared mutable state
/// between the pipeline thread and the worker task, so no raw-pointer unsafe is
/// needed to cross threads. The buffer arrives back at the pipeline as a
/// normal move through the reply channel.
pub(crate) fn read_into_byte_buffer<A: AsyncRead>(
    file: &BlobFile<A>,
    range: Range<u64>,
    align: usize,
) -> impl Future<Output = Result<AVec<u8, RuntimeAlign>>> + Send + 'static {
    let len = (range.end - range.start) as usize;
    let stream_fut = file.inner.read_range(&file.path, range);
    async move {
        let stream = stream_fut.await?;
        let buf = avec_rt!([align] | 0u8; len);
        fold_stream_into_buffer(stream, buf).await
    }
}

/// Like [`read_into_byte_buffer`], but fetches the whole object in one GET,
/// sizing the buffer from the response length (no separate `len`/HEAD).
pub(crate) fn read_whole_into_byte_buffer<A: AsyncRead + Clone>(
    file: &BlobFile<A>,
    align: usize,
) -> impl Future<Output = Result<AVec<u8, RuntimeAlign>>> + Send + 'static {
    read_from_into_byte_buffer(file, 0, align)
}

/// Like [`read_into_byte_buffer`], but fetches everything from byte offset
/// `from` to the end of the object in one open-ended GET, sizing the buffer
/// from the object's total length carried in the response — no separate
/// `len`/HEAD round-trip on the happy path. `from == 0` reads the whole object.
///
/// An offset at or past the end has no tail to read. The backend reports that as
/// an unsatisfiable-range error rather than an empty body, so the error path
/// confirms with a single `len`: if `from >= eof` the tail is genuinely empty
/// and we yield a zero-length buffer; otherwise the original read error stands.
pub(crate) fn read_from_into_byte_buffer<A: AsyncRead + Clone>(
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
                    return Ok(avec_rt!([align] | 0u8; 0));
                }
                return Err(err);
            }
        };
        let len = size.saturating_sub(from) as usize;
        let buf = avec_rt!([align] | 0u8; len);
        fold_stream_into_buffer(stream, buf).await
    }
}

/// Copy every chunk of `stream` into `buf`, erroring if the streamed bytes
/// don't exactly fill it.
async fn fold_stream_into_buffer(
    mut stream: BoxStream<'static, Result<Bytes>>,
    mut buf: AVec<u8, RuntimeAlign>,
) -> Result<AVec<u8, RuntimeAlign>> {
    let mut off = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let end = off + chunk.len();
        if end > buf.len() {
            return Err(UniversalIoError::S3Config {
                description: format!(
                    "over-read: tried to write {end} bytes into a buffer of size {}",
                    buf.len(),
                ),
            });
        }
        buf[off..end].copy_from_slice(&chunk);
        off = end;
    }
    if off != buf.len() {
        return Err(UniversalIoError::S3Config {
            description: format!("short read: expected {} bytes, got {off}", buf.len()),
        });
    }
    Ok(buf)
}
