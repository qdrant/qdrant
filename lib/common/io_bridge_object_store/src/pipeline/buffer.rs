use std::future::Future;

use common::universal_io::{Item, ReadRange, Result, UniversalIoError};
use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;

use crate::file::BlobFile;
use crate::read::AsyncRead;
use crate::writer::AlignedBufWriter;

/// Build the future that allocates an exact-size destination buffer, streams
/// the backend read for `range` into it, and returns it as the future's output.
///
/// Shared by the borrowed and owned pipeline `schedule` impls. The buffer lives
/// inside the future for the duration of the read — no shared mutable state
/// between the pipeline thread and the worker task, so no raw-pointer unsafe is
/// needed to cross threads. The buffer arrives back at the pipeline as a
/// normal move through the reply channel.
pub(super) fn read_into_buffer<A: AsyncRead, T: Item>(
    file: &BlobFile<A>,
    range: ReadRange,
) -> impl Future<Output = Result<Vec<T>>> + Send + 'static {
    let item_size = size_of::<T>() as u64;
    let start = range.byte_offset;
    let end = start + range.length * item_size;
    let stream_fut = file.inner.read_range(&file.path, start..end);
    async move {
        let mut stream = stream_fut.await?;
        let mut buf = vec![T::zeroed(); range.length as usize];
        {
            let mut writer = AlignedBufWriter::new(&mut buf);
            while let Some(chunk) = stream.next().await {
                writer
                    .write_all(&chunk?)
                    .await
                    .map_err(UniversalIoError::s3)?;
            }
            if writer.written() != writer.capacity() {
                return Err(UniversalIoError::S3Config {
                    description: format!(
                        "short read: expected {} bytes, got {}",
                        writer.capacity(),
                        writer.written(),
                    ),
                });
            }
        }
        Ok(buf)
    }
}
