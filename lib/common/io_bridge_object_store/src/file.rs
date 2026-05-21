use std::borrow::Cow;
use std::path::Path;

use common::generic_consts::AccessPattern;
use common::universal_io::{
    OpenOptions, ReadRange, Result, UniversalIoError, UniversalKind, UniversalRead,
    UniversalReadFileOps, UserData,
};

use crate::pipeline::{BorrowedBlobPipeline, OwnedBlobPipeline};
use crate::read::AsyncRead;
use crate::runtime::BridgeRuntime;

/// Sync wrapper around a [`AsyncRead`] backend that implements [`UniversalRead`].
///
/// The backend's async reads are routed through a [`BridgeRuntime`]:
///   * single reads via `block_on`,
///   * batched/pipelined reads via the runtime's worker thread (MPSC channel).
pub struct BlobFile<A: AsyncRead> {
    pub(crate) inner: A,
    pub(crate) runtime: BridgeRuntime,
}

impl<A: AsyncRead> std::fmt::Debug for BlobFile<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobFile")
            .field("runtime", &self.runtime)
            .field("len", &self.inner.len())
            .finish_non_exhaustive()
    }
}

impl<A: AsyncRead> BlobFile<A> {
    pub fn new(inner: A, runtime: BridgeRuntime) -> Self {
        Self { inner, runtime }
    }

    pub fn runtime(&self) -> &BridgeRuntime {
        &self.runtime
    }

    pub fn source(&self) -> &A {
        &self.inner
    }
}

impl<A: AsyncRead> UniversalReadFileOps for BlobFile<A> {
    fn list_files(_prefix_path: &Path) -> Result<Vec<std::path::PathBuf>> {
        Err(UniversalIoError::S3Config {
            description: "BlobFile does not expose static list_files via UniversalReadFileOps; \
                          call A::list_files(runtime, config, prefix) on the backend type"
                .into(),
        })
    }

    fn exists(_path: &Path) -> Result<bool> {
        Err(UniversalIoError::S3Config {
            description: "BlobFile does not expose static exists via UniversalReadFileOps; \
                          call A::exists(runtime, config, path) on the backend type"
                .into(),
        })
    }
}

impl<A: AsyncRead> UniversalRead for BlobFile<A> {
    type BorrowedReadPipeline<'a, T, U>
        = BorrowedBlobPipeline<'a, A, T, U>
    where
        T: bytemuck::Pod,
        U: UserData;

    type OwnedReadPipeline<T, U>
        = OwnedBlobPipeline<A, T, U>
    where
        T: bytemuck::Pod,
        U: UserData;

    fn open(_path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self> {
        Err(UniversalIoError::S3Config {
            description: "BlobFile cannot be opened through UniversalRead::open; \
                          call A::open(runtime, config, key) on the backend type instead"
                .into(),
        })
    }

    fn read<P: AccessPattern, T: bytemuck::Pod>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;
        let bytes = self.runtime.block_on(self.inner.read_range(start..end))?;
        let items = bytemuck::try_cast_slice(&bytes)?;
        Ok(Cow::Owned(items.to_vec()))
    }

    fn len<T>(&self) -> Result<u64> {
        let item_size = size_of::<T>() as u64;
        debug_assert_eq!(self.inner.len() % item_size, 0);
        Ok(self.inner.len() / item_size)
    }

    fn populate(&self) -> Result<()> {
        Ok(())
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

        fn open(
            _runtime: Option<BridgeRuntime>,
            _config: &(),
            _key: &Path,
        ) -> Result<BlobFile<Self>> {
            Err(UniversalIoError::S3Config {
                description: "MockSource has no real open path; construct directly in tests".into(),
            })
        }

        fn list_files(
            _runtime: Option<BridgeRuntime>,
            _config: &(),
            _prefix: &Path,
        ) -> Result<Vec<std::path::PathBuf>> {
            Ok(vec![])
        }

        fn exists(_runtime: Option<BridgeRuntime>, _config: &(), _path: &Path) -> Result<bool> {
            Ok(false)
        }

        fn read_range(
            &self,
            range: Range<u64>,
        ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
            let bytes = self.data.slice(range.start as usize..range.end as usize);
            async move { Ok(bytes) }
        }

        fn len(&self) -> u64 {
            self.data.len() as u64
        }

        fn kind() -> UniversalKind {
            UniversalKind::S3
        }
    }

    #[test]
    fn open_via_universal_read_trait_errors() {
        let result =
            <BlobFile<MockSource> as UniversalRead>::open("k", OpenOptions::new_for_test());
        assert!(matches!(
            result.unwrap_err(),
            UniversalIoError::S3Config { .. }
        ));
    }

    #[test]
    fn read_returns_bytes_through_runtime() {
        let file = BlobFile::new(MockSource::new(b"hello world"), BridgeRuntime::global());
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 11))
            .expect("read");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn read_subrange() {
        let file = BlobFile::new(MockSource::new(b"hello world"), BridgeRuntime::global());
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
        );
        let len: u64 = <BlobFile<MockSource> as UniversalRead>::len::<u16>(&file).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn read_batch_returns_all_pairs() {
        let file = BlobFile::new(MockSource::new(b"helloWORLDxyz"), BridgeRuntime::global());
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
