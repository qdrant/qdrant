//! Conditional async impls for the read-only wrapper: `ReadOnly<S>` /
//! `ReadOnlyFs<F>` are async-capable exactly when the wrapped backend is.

use std::ops::Range;
use std::path::{Path, PathBuf};

use super::read_only::{ReadOnly, ReadOnlyFs};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    ChunkSink, ListedFile, OpenOptions, UioResult, UniversalReadAsync, UniversalReadFsAsync,
};

impl<F: UniversalReadFsAsync> UniversalReadFsAsync for ReadOnlyFs<F> {
    async fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Self::File> {
        debug_assert!(!options.writeable);
        Ok(ReadOnly(self.0.open_async(path, options, extra).await?))
    }

    async fn list_files_async(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        self.0.list_files_async(prefix_path).await
    }
}

impl<S> UniversalReadAsync for ReadOnly<S>
where
    S: UniversalReadAsync,
{
    #[inline]
    fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> impl Future<Output = UioResult<ACow<'_>>> {
        self.0.read_bytes_async(range, access_pattern, align)
    }

    #[inline]
    fn read_from_into_async<W, I>(
        &self,
        from: u64,
        init: I,
    ) -> impl Future<Output = UioResult<W>> + Send
    where
        I: FnOnce(u64) -> UioResult<W> + Send + 'static,
        W: ChunkSink + Send + 'static,
    {
        self.0.read_from_into_async(from, init)
    }
}
