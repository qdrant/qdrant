use std::fmt;
use std::ops::Deref;
use std::path::Path;

use crate::mmap::{Advice, AdviceSetting};
use crate::universal_io::{OpenOptions, Populate, Result, UniversalRead, UniversalReadFs};

/// Thin RAII wrapper around a [`UniversalRead`] handle for a one-shot read.
///
/// It dereferences to the wrapped backend, so it reads exactly like any
/// [`UniversalRead`]. On drop it evicts the data from the RAM/page cache via
/// [`UniversalRead::clear_ram_cache`], so loading a file fully into application
/// memory does not leave the cache populated afterwards.
///
/// This is the [`UniversalRead`] counterpart of [`crate::fs::OneshotFile`]: it
/// works over any backend (mmap, io_uring, object storage, …) instead of a local
/// file handle.
pub struct OneshotFile<S: UniversalRead> {
    inner: S,
}

impl<S: UniversalRead> OneshotFile<S> {
    /// Open `path` read-only through `fs` for a single sequential read.
    pub fn open(fs: &S::Fs, path: impl AsRef<Path>) -> Result<Self> {
        let inner = fs.open(
            path,
            OpenOptions {
                writeable: false,
                need_sequential: true,
                populate: Populate::No,
                advice: AdviceSetting::Advice(Advice::Sequential),
            },
            Default::default(),
        )?;
        Ok(Self { inner })
    }

    /// Wrap an already-opened [`UniversalRead`] handle for one-shot use.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: UniversalRead> Deref for OneshotFile<S> {
    type Target = S;

    fn deref(&self) -> &S {
        &self.inner
    }
}

impl<S: UniversalRead> fmt::Debug for OneshotFile<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneshotFile")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<S: UniversalRead> Drop for OneshotFile<S> {
    fn drop(&mut self) {
        if let Err(err) = self.inner.clear_ram_cache() {
            log::warn!("Failed to clear RAM cache for one-shot file: {err}");
        }
    }
}
