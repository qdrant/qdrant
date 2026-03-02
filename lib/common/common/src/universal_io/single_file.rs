//! Single-file adapter over [`StorageRead`] / [`StorageWrite`].
//!
//! See [`SingleFile`].

use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::Path;
use std::{fmt, mem};

use crate::universal_io::multi_universal_read::{SourceId, StorageRead};
use crate::universal_io::{
    ElementOffset, ElementsRange, Flusher, OpenOptions, Result, StorageWrite,
};

/// Wraps a [`StorageRead`] (multi-source) backend, exposing single-file operations.
///
/// On [`open`](Self::open) the backend is created with one attached source (`SourceId(0)`).
pub struct SingleFile<T, S> {
    storage: S,
    _element: PhantomData<T>,
}

impl<T, S: fmt::Debug> fmt::Debug for SingleFile<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleFile")
            .field("storage", &self.storage)
            .field("element_size", &mem::size_of::<T>())
            .finish()
    }
}

const SOURCE: SourceId = SourceId(0);

// Read operations
impl<T: Copy + 'static, S: StorageRead<T>> SingleFile<T, S> {
    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let mut storage = S::new(options);
        storage.attach(path.as_ref())?;
        Ok(Self {
            storage,
            _element: PhantomData,
        })
    }

    pub fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        self.storage.read::<SEQUENTIAL>(SOURCE, range)
    }

    pub fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        self.storage.read_whole(SOURCE)
    }

    pub fn read_batch<'a, const SEQUENTIAL: bool>(
        &'a self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        callback: impl FnMut(usize, Cow<'a, [T]>) -> Result<()>,
    ) -> Result<()> {
        self.storage
            .read_batch_multi::<SEQUENTIAL>(ranges.into_iter().map(|r| (SOURCE, r)), callback)
    }

    pub fn len(&self) -> Result<u64> {
        self.storage.source_len(SOURCE)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn populate(&self) -> Result<()> {
        self.storage.populate()
    }

    pub fn clear_ram_cache(&self) -> Result<()> {
        self.storage.clear_ram_cache()
    }
}

// Write operations
impl<T: Copy + 'static, S: StorageWrite<T>> SingleFile<T, S> {
    pub fn write(&mut self, offset: ElementOffset, data: &[T]) -> Result<()> {
        self.storage
            .write_batch_multi(std::iter::once((SOURCE, offset, data)))
    }

    pub fn write_batch<'a>(
        &mut self,
        offset_data: impl IntoIterator<Item = (ElementOffset, &'a [T])>,
    ) -> Result<()> {
        self.storage
            .write_batch_multi(offset_data.into_iter().map(|(o, d)| (SOURCE, o, d)))
    }

    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }
}
