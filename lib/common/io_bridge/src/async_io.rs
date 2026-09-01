//! The async surface of the blob backend — the genuinely asynchronous
//! [`UniversalReadFsAsync`] / [`UniversalReadAsync`] impls: reads are spawned
//! onto the [`BridgeRuntime`](crate::BridgeRuntime), so a future handed out
//! here keeps making progress in the background while parked (e.g. in a
//! `CachedFs` prefetch pool).

use std::ops::Range;
use std::path::PathBuf;

use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    OpenOptions, UioResult, UniversalReadAsync, UniversalReadFs, UniversalReadFsAsync,
};

use crate::file::BlobFile;
use crate::fs::BlobFs;
use crate::pipeline::read_into_byte_buffer;
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
            .spawn(read_into_byte_buffer::<A>(self, range, align))
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
}
