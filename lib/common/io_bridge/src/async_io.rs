//! The async surface of the blob backend — the genuinely asynchronous
//! [`UniversalReadFsAsync`] / [`UniversalReadAsync`] impls: reads are spawned
//! onto the [`BridgeRuntime`](crate::BridgeRuntime), so a future handed out
//! here keeps making progress in the background while parked (e.g. in a
//! `CachedFs` prefetch pool).

use std::ops::Range;
use std::path::{Path, PathBuf};

use common::ambient::AmbientFutureExt as _;
use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    ChunkSink, ListedFile, OpenOptions, UioResult, UniversalReadAsync, UniversalReadFs,
    UniversalReadFsAsync,
};

use crate::file::BlobFile;
use crate::fs::BlobFs;
use crate::pipeline::{read_from_into_sink, read_into_byte_buffer};
use crate::read::AsyncRead;

impl<A: AsyncRead + Clone> UniversalReadFsAsync for BlobFs<A> {
    async fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: (),
    ) -> UioResult<BlobFile<A>> {
        // BlobFile does not populate on open.
        self.open(path, options, extra)
    }

    fn list_files_async<'a>(
        &'a self,
        prefix_path: &'a Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + use<'a, A> {
        self.spawn(self.list_files_traced(prefix_path))
    }

    fn select_files_async<'a, P: AsRef<Path> + Send + Sync>(
        &'a self,
        paths: &'a [P],
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + use<'a, A, P> {
        let paths: Vec<PathBuf> = paths.iter().map(|p| p.as_ref().to_path_buf()).collect();
        self.spawn(self.select_files_traced(paths))
    }
}

impl<A: AsyncRead + Clone> UniversalReadAsync for BlobFile<A> {
    async fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        _access_pattern: P,
        align: usize,
    ) -> UioResult<ACow<'_>> {
        let started = std::time::Instant::now();
        log::trace!(
            target: crate::LATENCY_LOG_TARGET,
            "scheduled async read of {}, {:?}",
            self.path.display(),
            range
        );

        let buf = self
            .runtime
            .handle()
            .spawn(read_into_byte_buffer::<A>(self, range, align).in_current_ambient())
            .await??;

        log::trace!(
            target: crate::LATENCY_LOG_TARGET,
            "awaited async read of {}, {:?} bytes took {}ms",
            self.path.display(),
            buf.len(),
            started.elapsed().as_millis()
        );
        Ok(ACow::Owned(buf))
    }

    /// Streams the object starting at `from` with a single request, handing each chunk to the sink
    /// on the bridge runtime as it arrives.
    fn read_from_into_async<W, I>(
        &self,
        from: u64,
        init: I,
    ) -> impl Future<Output = UioResult<W>> + Send
    where
        I: FnOnce(u64) -> UioResult<W> + Send + 'static,
        W: ChunkSink + Send + 'static,
    {
        let started = std::time::Instant::now();
        log::trace!(
            target: crate::LATENCY_LOG_TARGET,
            "schedule read for 0 of {} range {from}..",
            self.path.display()
        );
        let task = self
            .runtime
            .handle()
            .spawn(read_from_into_sink::<A, W, I>(self, from, init).in_current_ambient());
        async move {
            let (sink, bytes) = task.await??;
            log::trace!(
                target: crate::LATENCY_LOG_TARGET,
                "awaited read for 0 returned {bytes} bytes in {:?}",
                started.elapsed()
            );
            Ok(sink)
        }
    }
}
