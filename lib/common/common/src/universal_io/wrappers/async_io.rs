//! Conditional async impls for the read-only wrapper: `ReadOnly<S>` /
//! `ReadOnlyFs<F>` are async-capable exactly when the wrapped backend is.

use std::ops::Range;
use std::path::PathBuf;

use super::read_only::{ReadOnly, ReadOnlyFs};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{OpenOptions, UioResult, UniversalReadAsync, UniversalReadFsAsync};

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
}
