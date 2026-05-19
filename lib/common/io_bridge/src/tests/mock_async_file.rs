use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use common::generic_consts::AccessPattern;
use common::universal_io::{ByteOffset, OpenOptions, ReadRange, UniversalIoError, UniversalKind};

use crate::async_io::{AsyncFlusher, AsyncRead, AsyncReadFileOps, AsyncWrite};

pub(super) struct MockAsyncFile {
    contents: Arc<Mutex<Vec<u8>>>,
}

impl MockAsyncFile {
    pub(super) fn with_contents(bytes: Vec<u8>) -> Self {
        Self {
            contents: Arc::new(Mutex::new(bytes)),
        }
    }

    pub(super) fn contents_handle(&self) -> Arc<Mutex<Vec<u8>>> {
        Arc::clone(&self.contents)
    }
}

impl fmt::Debug for MockAsyncFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockAsyncFile")
            .field("len", &self.contents.lock().unwrap().len())
            .finish()
    }
}

impl AsyncReadFileOps for MockAsyncFile {
    async fn list_files(_prefix_path: &Path) -> Result<Vec<PathBuf>, UniversalIoError> {
        Ok(Vec::new())
    }

    async fn exists(_path: &Path) -> Result<bool, UniversalIoError> {
        Ok(false)
    }
}

impl AsyncRead for MockAsyncFile {
    async fn open(_path: &Path, _options: OpenOptions) -> Result<Self, UniversalIoError> {
        Ok(Self::with_contents(Vec::new()))
    }

    fn read<P: AccessPattern, T: bytemuck::Pod>(
        &self,
        range: ReadRange,
    ) -> impl Future<Output = Result<Cow<'_, [T]>, UniversalIoError>> {
        let start = range.byte_offset as usize;
        let byte_len = range.length as usize * size_of::<T>();
        let bytes = self.contents.lock().unwrap()[start..start + byte_len].to_vec();
        async move {
            let count = bytes.len() / size_of::<T>();
            let mut v: Vec<T> = vec![T::zeroed(); count];
            bytemuck::cast_slice_mut::<T, u8>(&mut v)
                .copy_from_slice(&bytes[..count * size_of::<T>()]);
            Ok(Cow::Owned(v))
        }
    }

    fn len<T>(&self) -> impl Future<Output = Result<u64, UniversalIoError>> {
        let l = (self.contents.lock().unwrap().len() / size_of::<T>()) as u64;
        async move { Ok(l) }
    }

    async fn populate(&self) -> Result<(), UniversalIoError> {
        Ok(())
    }

    async fn clear_ram_cache(&self) -> Result<(), UniversalIoError> {
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::Mmap
    }
}

impl AsyncWrite for MockAsyncFile {
    fn write<T: bytemuck::Pod>(
        &mut self,
        byte_offset: ByteOffset,
        data: &[T],
    ) -> impl Future<Output = Result<(), UniversalIoError>> {
        let bytes_to_write: Vec<u8> = bytemuck::cast_slice::<T, u8>(data).to_vec();
        let contents = Arc::clone(&self.contents);
        async move {
            let mut guard = contents.lock().unwrap();
            let end = byte_offset as usize + bytes_to_write.len();
            if guard.len() < end {
                guard.resize(end, 0);
            }
            guard[byte_offset as usize..end].copy_from_slice(&bytes_to_write);
            Ok(())
        }
    }

    fn write_batch<T: bytemuck::Pod>(
        &mut self,
        offset_data: Vec<(ByteOffset, &[T])>,
    ) -> impl Future<Output = Result<(), UniversalIoError>> {
        let writes: Vec<(ByteOffset, Vec<u8>)> = offset_data
            .into_iter()
            .map(|(offset, slice)| (offset, bytemuck::cast_slice::<T, u8>(slice).to_vec()))
            .collect();
        let contents = Arc::clone(&self.contents);
        async move {
            let mut guard = contents.lock().unwrap();
            for (offset, bytes) in writes {
                let end = offset as usize + bytes.len();
                if guard.len() < end {
                    guard.resize(end, 0);
                }
                guard[offset as usize..end].copy_from_slice(&bytes);
            }
            Ok(())
        }
    }

    fn flusher(&self) -> AsyncFlusher {
        Box::new(|| Box::pin(async { Ok(()) }))
    }
}
