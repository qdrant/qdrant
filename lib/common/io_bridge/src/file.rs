use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    ByteOffset, Flusher, Item, UioResult, UniversalFlush, UniversalIoError, UniversalKind,
    UniversalRead, UserData,
};

use crate::fs::BlobFs;
use crate::pipeline::{BlobReadPipeline, read_into_byte_buffer, read_whole_into_byte_buffer};
use crate::read::AsyncRead;
use crate::runtime::BridgeRuntime;
use crate::write::AsyncAppend;

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
    /// Whether this handle accepts appends. Directly-constructed handles
    /// are writeable; [`BlobFs::open`] feeds `OpenOptions::writeable`
    /// through [`Self::with_writeable`].
    writeable: bool,
}

impl<A: AsyncRead> std::fmt::Debug for BlobFile<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            runtime,
            path,
            writeable,
            inner: _,
        } = self;
        f.debug_struct("BlobFile")
            .field("runtime", runtime)
            .field("path", path)
            .field("writeable", writeable)
            .finish_non_exhaustive()
    }
}

impl<A: AsyncRead> BlobFile<A> {
    pub fn new(inner: A, runtime: BridgeRuntime, path: impl Into<PathBuf>) -> Self {
        Self {
            inner,
            runtime,
            path: path.into(),
            writeable: true,
        }
    }

    /// Set whether this handle accepts appends.
    pub fn with_writeable(mut self, writeable: bool) -> Self {
        self.writeable = writeable;
        self
    }

    /// Build the backend handle from its config and pin it to `path`. Performs
    /// no IO — the object is not touched until the first read or metadata call.
    pub fn open(
        config: &A::Config,
        runtime: BridgeRuntime,
        path: impl Into<PathBuf>,
    ) -> UioResult<Self> {
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

    type ReadPipeline<'a, U>
        = BlobReadPipeline<'a, A, U>
    where
        Self: 'a,
        U: UserData;

    fn live_reload(&mut self) -> UioResult<()> {
        Ok(())
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        _access_pattern: P,
        align: usize,
    ) -> UioResult<ACow<'_>> {
        let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
        let start_time = enabled.then(std::time::Instant::now);
        let buf = self
            .runtime
            .block_on(read_into_byte_buffer::<A>(self, range.clone(), align))?;

        if let Some(start_time) = start_time {
            log::trace!(
                target: crate::LATENCY_LOG_TARGET,
                "read_bytes({}, {:?}) took {:?} and returned {} bytes",
                self.path.display(),
                range,
                start_time.elapsed(),
                buf.len()
            );
        }
        Ok(ACow::Owned(buf))
    }

    fn read_whole<T: Item>(&self) -> UioResult<Cow<'_, [T]>> {
        let buf = self
            .runtime
            .block_on(read_whole_into_byte_buffer::<A>(self, align_of::<T>()))?;
        Ok(ACow::Owned(buf)
            .try_cast_bytemuck()
            .expect("buffer has compatible layout"))
    }

    fn len<T>(&self) -> UioResult<u64> {
        let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
        let start_time = enabled.then(std::time::Instant::now);
        let item_size = size_of::<T>() as u64;
        let len = self.runtime.block_on(self.inner.len(&self.path))?;
        debug_assert_eq!(len % item_size, 0);

        if let Some(start_time) = start_time {
            log::trace!(
                target: crate::LATENCY_LOG_TARGET,
                "len::<{}>({}) took {:?} and measured {} bytes",
                std::any::type_name::<T>(),
                self.path.display(),
                start_time.elapsed(),
                len
            );
        }
        Ok(len / item_size)
    }

    fn populate(&self) -> UioResult<()> {
        Ok(())
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> UioResult<()> {
        Ok(())
    }

    fn kind() -> UniversalKind {
        A::kind()
    }
}

impl<A: AsyncAppend + Clone> UniversalFlush for BlobFile<A> {
    fn flusher(&self) -> Flusher {
        // Appends are durable once the backend acknowledges them.
        Box::new(|| Ok(()))
    }
}

impl<A: AsyncAppend + Clone> BlobFile<A> {
    /// One append RPC at exactly `offset`; the backend itself validates the
    /// offset against the current object size (the compare-and-swap), so no
    /// local length tracking is needed.
    ///
    /// `expected_etag`, when provided, is the entity tag the caller last
    /// observed for the object (from a listing, or tracked across its own
    /// appends); it travels to the backend as a server-side precondition —
    /// see [`AsyncAppend::append`] for which operations can honor it.
    pub fn append_bytes(
        &self,
        offset: ByteOffset,
        data: Bytes,
        expected_etag: Option<String>,
    ) -> UioResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !self.writeable {
            return Err(UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "append requires a handle opened with writeable=true",
            )));
        }

        self.runtime
            .block_on(self.inner.append(&self.path, offset, data, expected_etag))
            .map(drop)
    }
}

// Deliberately no `UniversalAppend` impl: the append-capable universal-IO
// citizen for object stores is `CachedBlobFile`, which drives this handle
// through `append_bytes` and supplies its tracked etag. A raw `BlobFile`
// append through the trait would bypass that guard.

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use common::generic_consts::{Random, Sequential};
    use common::universal_io::{
        ListedFile, OpenOptions, ReadRange, UniversalIoError, UniversalReadFs,
    };
    use futures::stream::{BoxStream, StreamExt};

    use super::*;
    use crate::{AsyncWrite, OffsetByteStream};

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

        fn open(_config: &()) -> UioResult<Self> {
            Err(UniversalIoError::S3Config {
                description: "MockSource has no real open path; construct directly in tests".into(),
            })
        }

        fn list_files(
            &self,
            _prefix: &Path,
        ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static {
            std::future::ready(Ok(vec![]))
        }

        fn exists(&self, _path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static {
            std::future::ready(Ok(true))
        }

        fn read_range(
            &self,
            _path: &Path,
            range: Range<u64>,
        ) -> impl Future<Output = UioResult<BoxStream<'static, UioResult<Bytes>>>> + Send + 'static
        {
            let bytes = self.data.slice(range.start as usize..range.end as usize);
            async move { Ok(futures::stream::once(async move { Ok(bytes) }).boxed()) }
        }

        fn read_from(
            &self,
            _path: &Path,
            from: u64,
        ) -> impl Future<Output = UioResult<(u64, OffsetByteStream)>> + Send + 'static {
            let size = self.data.len() as u64;
            let tail = self.data.slice(from as usize..);
            async move {
                Ok((
                    size,
                    futures::stream::once(async move { Ok((0, tail)) }).boxed(),
                ))
            }
        }

        fn len(&self, _path: &Path) -> impl Future<Output = UioResult<u64>> + Send + 'static {
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
            .read::<_, u8>(ReadRange::new(0, 11), Sequential)
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
            .read::<_, u8>(ReadRange::new(0, 11), Sequential)
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
            .read::<_, u8>(ReadRange::new(6, 5), Random)
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
        file.read_batch(inputs, Random, |u, s| {
            got.insert(u, s.to_vec());
            UioResult::Ok(())
        })
        .expect("read_batch");
        assert_eq!(got[&1], b"hello");
        assert_eq!(got[&2], b"WORLD");
        assert_eq!(got[&3], b"xyz");
    }

    /// A mutable [`AsyncRead`] + [`AsyncWrite`] + [`AsyncAppend`] mock: one
    /// object (`None` = missing) behind a shared store, with call counters.
    #[derive(Clone, Default)]
    struct MutableMockSource {
        store: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
        len_calls: Arc<std::sync::atomic::AtomicUsize>,
        append_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MutableMockSource {
        fn content(&self) -> Option<Vec<u8>> {
            self.store.lock().unwrap().clone()
        }
    }

    impl AsyncRead for MutableMockSource {
        type Config = ();

        fn open(_config: &()) -> UioResult<Self> {
            Ok(Self::default())
        }

        fn list_files(
            &self,
            _prefix: &Path,
        ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static {
            std::future::ready(Ok(vec![]))
        }

        fn exists(&self, _path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static {
            std::future::ready(Ok(self.store.lock().unwrap().is_some()))
        }

        fn read_range(
            &self,
            path: &Path,
            range: Range<u64>,
        ) -> impl Future<Output = UioResult<BoxStream<'static, UioResult<Bytes>>>> + Send + 'static
        {
            let result = match &*self.store.lock().unwrap() {
                Some(data) => Ok(Bytes::copy_from_slice(
                    &data[range.start as usize..range.end as usize],
                )),
                None => Err(UniversalIoError::NotFound { path: path.into() }),
            };
            async move {
                let bytes = result?;
                Ok(futures::stream::once(async move { Ok(bytes) }).boxed())
            }
        }

        fn read_from(
            &self,
            path: &Path,
            from: u64,
        ) -> impl Future<Output = UioResult<(u64, OffsetByteStream)>> + Send + 'static {
            let result = match &*self.store.lock().unwrap() {
                Some(data) => Ok((
                    data.len() as u64,
                    Bytes::copy_from_slice(&data[from as usize..]),
                )),
                None => Err(UniversalIoError::NotFound { path: path.into() }),
            };
            async move {
                let (size, tail) = result?;
                Ok((
                    size,
                    futures::stream::once(async move { Ok((0, tail)) }).boxed(),
                ))
            }
        }

        fn len(&self, path: &Path) -> impl Future<Output = UioResult<u64>> + Send + 'static {
            self.len_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let result = match &*self.store.lock().unwrap() {
                Some(data) => Ok(data.len() as u64),
                None => Err(UniversalIoError::NotFound { path: path.into() }),
            };
            std::future::ready(result)
        }

        fn kind() -> UniversalKind {
            UniversalKind::S3
        }
    }

    impl AsyncWrite for MutableMockSource {
        fn create(&self, _path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static {
            *self.store.lock().unwrap() = Some(Vec::new());
            std::future::ready(Ok(()))
        }

        fn remove(&self, path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static {
            let result = match self.store.lock().unwrap().take() {
                Some(_) => Ok(()),
                None => Err(UniversalIoError::NotFound { path: path.into() }),
            };
            std::future::ready(result)
        }

        fn save(
            &self,
            _path: &Path,
            bytes: Bytes,
        ) -> impl Future<Output = UioResult<()>> + Send + 'static {
            *self.store.lock().unwrap() = Some(bytes.to_vec());
            std::future::ready(Ok(()))
        }
    }

    impl AsyncAppend for MutableMockSource {
        fn append_support(&self) -> crate::AppendSupport {
            crate::AppendSupport::Always
        }

        fn append(
            &self,
            path: &Path,
            offset: u64,
            data: Bytes,
            expected_etag: Option<String>,
        ) -> impl Future<Output = UioResult<u64>> + Send + 'static {
            self.append_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut guard = self.store.lock().unwrap();
            // A content-derived etag, honored like a real store's
            // precondition.
            let current_etag = guard
                .as_ref()
                .map(|object| format!("etag-{}", object.len()));
            // Validate the preconditions before materializing anything: a
            // rejected append must not leave an empty object behind.
            let current_len = guard.as_ref().map_or(0, |object| object.len() as u64);
            let result = if expected_etag.is_some() && expected_etag != current_etag {
                Err(UniversalIoError::AppendEtagMismatch { path: path.into() })
            } else if current_len == offset {
                let object = guard.get_or_insert_with(Vec::new);
                object.extend_from_slice(&data);
                Ok(object.len() as u64)
            } else {
                Err(UniversalIoError::AppendOffsetConflict {
                    path: path.into(),
                    offset,
                })
            };
            std::future::ready(result)
        }
    }

    fn mutable_file(source: &MutableMockSource) -> BlobFile<MutableMockSource> {
        BlobFile::new(source.clone(), BridgeRuntime::global(), "obj")
    }

    #[test]
    fn append_creates_missing_object() {
        let source = MutableMockSource::default();
        let file = mutable_file(&source);

        // A rejected stale append must not materialize the object.
        let err = BridgeRuntime::global()
            .block_on(source.append(Path::new("obj"), 5, Bytes::from_static(b"x"), None))
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::AppendOffsetConflict { .. }));
        assert!(source.content().is_none());

        file.append_bytes(0, Bytes::from_static(b"abc"), None)
            .unwrap();
        file.append_bytes(3, Bytes::from_static(b"de"), None)
            .unwrap();
        assert_eq!(source.content().unwrap(), b"abcde");
        assert_eq!(<BlobFile<_> as UniversalRead>::len::<u8>(&file).unwrap(), 5);
    }

    #[test]
    fn empty_append_succeeds_without_request() {
        let source = MutableMockSource::default();
        let file = mutable_file(&source);

        file.append_bytes(0, Bytes::from_static(b"abc"), None)
            .unwrap();
        file.append_bytes(3, Bytes::new(), None).unwrap();

        let append_calls = source
            .append_calls
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(append_calls, 1);
    }

    /// Two handles appending to the same object: the one with the stale
    /// offset gets a conflict and nothing lands twice; re-deriving the
    /// offset from the actual length recovers.
    #[test]
    fn append_conflict_recovery() {
        let source = MutableMockSource::default();
        let first = mutable_file(&source);
        let second = mutable_file(&source);

        first
            .append_bytes(0, Bytes::from_static(b"aaa"), None)
            .unwrap();
        second
            .append_bytes(3, Bytes::from_static(b"bbb"), None)
            .unwrap();

        let err = first
            .append_bytes(3, Bytes::from_static(b"ccc"), None)
            .unwrap_err();
        assert!(matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 3, .. }
        ));

        let eof = <BlobFile<_> as UniversalRead>::len::<u8>(&first).unwrap();
        first
            .append_bytes(eof, Bytes::from_static(b"ccc"), None)
            .unwrap();
        assert_eq!(source.content().unwrap(), b"aaabbbccc");
    }

    /// The expected etag travels to the backend as a precondition: a
    /// matching etag lets the append through, a stale one (or a missing
    /// object) rejects it without mutating anything.
    #[test]
    fn append_bytes_passes_the_expected_etag_precondition() {
        let source = MutableMockSource::default();
        let file = mutable_file(&source);

        // No object yet: an expected etag cannot match anything.
        let err = file
            .append_bytes(0, Bytes::from_static(b"abc"), Some("etag-0".to_string()))
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::AppendEtagMismatch { .. }));
        assert!(source.content().is_none());

        file.append_bytes(0, Bytes::from_static(b"abc"), None)
            .unwrap();

        // The current etag: the append proceeds.
        file.append_bytes(3, Bytes::from_static(b"de"), Some("etag-3".to_string()))
            .unwrap();

        // The previous etag is stale now: rejected, nothing lands.
        let err = file
            .append_bytes(5, Bytes::from_static(b"f"), Some("etag-3".to_string()))
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::AppendEtagMismatch { .. }));
        assert_eq!(source.content().unwrap(), b"abcde");
    }

    #[test]
    fn append_requires_writeable_open() {
        let fs = BlobFs::new(MutableMockSource::default(), BridgeRuntime::global());
        let file = fs
            .open(
                "obj",
                OpenOptions {
                    writeable: false,
                    ..OpenOptions::new_for_test()
                },
                (),
            )
            .unwrap();

        assert!(
            file.append_bytes(0, Bytes::from_static(b"x"), None)
                .is_err()
        );
    }

    #[test]
    fn append_flusher_is_a_no_op() {
        let source = MutableMockSource::default();
        let file = mutable_file(&source);

        file.append_bytes(0, Bytes::from_static(b"abc"), None)
            .unwrap();
        (file.flusher())().unwrap();
    }

    #[test]
    fn blob_fs_write_ops_round_trip() {
        use common::universal_io::UniversalReadFileOps as _;

        let source = MutableMockSource::default();
        let fs = BlobFs::new(source.clone(), BridgeRuntime::global());
        let path = Path::new("obj");

        assert!(!fs.exists(path).unwrap());
        fs.create(path).unwrap();
        assert!(fs.exists(path).unwrap());

        fs.atomic_save(path, b"xyz").unwrap();
        assert_eq!(source.content().unwrap(), b"xyz");

        fs.remove(path).unwrap();
        assert!(!fs.exists(path).unwrap());
    }
}
