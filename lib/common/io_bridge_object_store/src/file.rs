use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{Item, Result, UniversalKind, UniversalRead, UserData};

use crate::fs::BlobFs;
use crate::pipeline::{
    BorrowedBlobPipeline, OwnedBlobPipeline, read_into_byte_buffer, read_whole_into_byte_buffer,
};
use crate::read::AsyncRead;
use crate::runtime::BridgeRuntime;

/// Sync wrapper around a [`AsyncRead`] backend that implements [`UniversalRead`].
///
/// Pins a single object (`path`) on the backend handle (`inner`) and routes the
/// backend's async operations through a [`BridgeRuntime`]:
///   * single reads / metadata lookups via `block_on`,
///   * batched/pipelined reads via the runtime's worker thread (MPSC channel).
#[derive(Clone)]
pub struct BlobFile<A: AsyncRead> {
    pub(crate) inner: A,
    pub(crate) runtime: BridgeRuntime,
    pub(crate) path: PathBuf,
}

impl<A: AsyncRead> std::fmt::Debug for BlobFile<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobFile")
            .field("runtime", &self.runtime)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl<A: AsyncRead> BlobFile<A> {
    pub fn new(inner: A, runtime: BridgeRuntime, path: impl Into<PathBuf>) -> Self {
        Self {
            inner,
            runtime,
            path: path.into(),
        }
    }

    /// Build the backend handle from its config and pin it to `path`. Performs
    /// no IO — the object is not touched until the first read or metadata call.
    pub fn open(
        config: &A::Config,
        runtime: BridgeRuntime,
        path: impl Into<PathBuf>,
    ) -> Result<Self> {
        let inner = A::open(config)?;
        Ok(Self::new(inner, runtime, path))
    }

    pub fn runtime(&self) -> &BridgeRuntime {
        &self.runtime
    }

    pub fn source(&self) -> &A {
        &self.inner
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<A: AsyncRead + Clone> UniversalRead for BlobFile<A> {
    type Fs = BlobFs<A>;

    type BorrowedReadPipeline<'a, U>
        = BorrowedBlobPipeline<'a, A, U>
    where
        Self: 'a,
        U: UserData;

    type OwnedReadPipeline<U>
        = OwnedBlobPipeline<A, U>
    where
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        Ok(())
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        let buf = self
            .runtime
            .block_on(read_into_byte_buffer::<A>(self, range, align))?;
        Ok(ACow::Owned(buf))
    }

    fn read_whole<T: Item>(&self) -> Result<Cow<'_, [T]>> {
        let buf = self
            .runtime
            .block_on(read_whole_into_byte_buffer::<A>(self, align_of::<T>()))?;
        Ok(ACow::Owned(buf)
            .try_cast_bytemuck()
            .expect("aligned whole-object buffer casts to [T]"))
    }

    fn len<T>(&self) -> Result<u64> {
        let item_size = size_of::<T>() as u64;
        let len = self.runtime.block_on(self.inner.len(&self.path))?;
        debug_assert_eq!(len % item_size, 0);
        Ok(len / item_size)
    }

    fn populate(&self) -> Result<()> {
        Ok(())
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> Result<()> {
        Ok(())
    }

    fn kind() -> UniversalKind {
        A::kind()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;

    use bytes::Bytes;
    use common::universal_io::{OpenOptions, ReadRange, UniversalIoError, UniversalReadFs};
    use futures::stream::{BoxStream, StreamExt};

    use super::*;

    #[derive(Clone)]
    struct MockSource {
        data: Bytes,
    }

    impl MockSource {
        fn new(data: &'static [u8]) -> Self {
            Self {
                data: Bytes::from_static(data),
            }
        }
    }

    impl AsyncRead for MockSource {
        type Config = ();

        fn open(_config: &()) -> Result<Self> {
            Err(UniversalIoError::S3Config {
                description: "MockSource has no real open path; construct directly in tests".into(),
            })
        }

        fn list_files(
            &self,
            _prefix: &Path,
        ) -> impl Future<Output = Result<Vec<std::path::PathBuf>>> + Send + 'static {
            std::future::ready(Ok(vec![]))
        }

        fn exists(&self, _path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
            std::future::ready(Ok(true))
        }

        fn create(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
            std::future::ready(Ok(()))
        }

        fn remove(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
            std::future::ready(Ok(()))
        }

        fn remove_dir(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
            std::future::ready(Ok(()))
        }

        fn atomic_save(
            &self,
            _path: &Path,
            _bytes: Bytes,
        ) -> impl Future<Output = Result<()>> + Send + 'static {
            std::future::ready(Ok(()))
        }

        fn read_range(
            &self,
            _path: &Path,
            range: Range<u64>,
        ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static
        {
            let bytes = self.data.slice(range.start as usize..range.end as usize);
            async move { Ok(futures::stream::once(async move { Ok(bytes) }).boxed()) }
        }

        fn read_whole(
            &self,
            _path: &Path,
        ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
        {
            let data = self.data.clone();
            let size = data.len() as u64;
            async move { Ok((size, futures::stream::once(async move { Ok(data) }).boxed())) }
        }

        fn len(&self, _path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static {
            let len = self.data.len() as u64;
            async move { Ok(len) }
        }

        fn kind() -> UniversalKind {
            UniversalKind::S3
        }
    }

    #[test]
    fn blob_fs_opens_readable_file() {
        let fs = BlobFs::new(MockSource::new(b"hello world"), BridgeRuntime::global());
        let file = fs
            .open("obj", OpenOptions::new_for_test(), ())
            .expect("open");
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 11))
            .expect("read");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn read_returns_bytes_through_runtime() {
        let file = BlobFile::new(
            MockSource::new(b"hello world"),
            BridgeRuntime::global(),
            "obj",
        );
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 11))
            .expect("read");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn read_subrange() {
        let file = BlobFile::new(
            MockSource::new(b"hello world"),
            BridgeRuntime::global(),
            "obj",
        );
        let cow = file
            .read::<common::generic_consts::Random, u8>(ReadRange::new(6, 5))
            .expect("read");
        assert_eq!(&cow[..], b"world");
    }

    #[test]
    fn len_divides_by_type_size() {
        let file = BlobFile::new(
            MockSource::new(b"\x01\x00\x02\x00"),
            BridgeRuntime::global(),
            "obj",
        );
        let len: u64 = <BlobFile<MockSource> as UniversalRead>::len::<u16>(&file).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn read_batch_returns_all_pairs() {
        let file = BlobFile::new(
            MockSource::new(b"helloWORLDxyz"),
            BridgeRuntime::global(),
            "obj",
        );
        let inputs = vec![
            (1u32, ReadRange::new(0, 5)),
            (2u32, ReadRange::new(5, 5)),
            (3u32, ReadRange::new(10, 3)),
        ];
        let mut got: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        file.read_batch::<common::generic_consts::Random, u8, _>(inputs, |u, s| {
            got.insert(u, s.to_vec());
            Ok(())
        })
        .expect("read_batch");
        assert_eq!(got[&1], b"hello");
        assert_eq!(got[&2], b"WORLD");
        assert_eq!(got[&3], b"xyz");
    }
}
