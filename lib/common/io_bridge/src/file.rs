use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    ByteOffset, Flusher, IsNotFound as _, Item, Result, UniversalAppend, UniversalFlush,
    UniversalIoError, UniversalKind, UniversalRead, UserData,
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
    /// Object size cached for [`UniversalAppend`]: the end-of-file offset
    /// this handle believes to be current. `None` until the first append
    /// (or after [`UniversalRead::reopen`]), then maintained locally under
    /// the single-writer contract to avoid a HEAD per append.
    append_len: Option<u64>,
}

impl<A: AsyncRead> std::fmt::Debug for BlobFile<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            runtime,
            path,
            writeable,
            append_len,
            inner: _,
        } = self;
        f.debug_struct("BlobFile")
            .field("runtime", runtime)
            .field("path", path)
            .field("writeable", writeable)
            .field("append_len", append_len)
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
            append_len: None,
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

    type ReadPipeline<'a, U>
        = BlobReadPipeline<'a, A, U>
    where
        Self: 'a,
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        // Drop the cached append offset so the next append re-probes the
        // backend — the documented recovery path after
        // `AppendOffsetConflict`.
        self.append_len = None;
        Ok(())
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
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

    fn read_whole<T: Item>(&self) -> Result<Cow<'_, [T]>> {
        let buf = self
            .runtime
            .block_on(read_whole_into_byte_buffer::<A>(self, align_of::<T>()))?;
        Ok(ACow::Owned(buf)
            .try_cast_bytemuck()
            .expect("buffer has compatible layout"))
    }

    fn len<T>(&self) -> Result<u64> {
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

impl<A: AsyncAppend + Clone> UniversalFlush for BlobFile<A> {
    fn flusher(&self) -> Flusher {
        // Appends are durable once the backend acknowledges them.
        Box::new(|| Ok(()))
    }
}

impl<A: AsyncAppend + Clone> BlobFile<A> {
    /// The end-of-file offset this handle believes to be current, probing
    /// the backend on first use. A missing object counts as empty
    /// (create-on-first-append).
    fn append_offset(&mut self) -> Result<u64> {
        if let Some(len) = self.append_len {
            return Ok(len);
        }

        let len = match self.runtime.block_on(self.inner.len(&self.path)) {
            Ok(len) => len,
            Err(err) if err.is_not_found() => 0,
            Err(err) => return Err(err),
        };
        self.append_len = Some(len);

        Ok(len)
    }

    fn append_bytes(&mut self, data: Bytes) -> Result<ByteOffset> {
        if !self.writeable {
            return Err(UniversalIoError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "append requires a handle opened with writeable=true",
            )));
        }

        let offset = self.append_offset()?;
        if data.is_empty() {
            return Ok(offset);
        }

        match self
            .runtime
            .block_on(self.inner.append(&self.path, offset, data))
        {
            Ok(new_len) => {
                self.append_len = Some(new_len);
                Ok(offset)
            }
            Err(err) => {
                // Force a fresh size probe on the next attempt; combined
                // with `reopen()` this is the documented conflict recovery.
                self.append_len = None;
                Err(err)
            }
        }
    }
}

impl<A: AsyncAppend + Clone> UniversalAppend for BlobFile<A> {
    fn append<T: bytemuck::Pod>(&mut self, data: &[T]) -> Result<ByteOffset> {
        self.append_bytes(Bytes::copy_from_slice(bytemuck::cast_slice(data)))
    }

    fn append_batch<'a, T: bytemuck::Pod>(
        &mut self,
        items: impl IntoIterator<Item = &'a [T]>,
    ) -> Result<ByteOffset> {
        // Concatenate into a single buffer so the whole batch lands in one
        // request.
        let slices: Vec<&[u8]> = items
            .into_iter()
            .map(|item| bytemuck::cast_slice(item))
            .collect();
        let total: usize = slices.iter().map(|slice| slice.len()).sum();
        let mut buffer = Vec::with_capacity(total);
        for slice in slices {
            buffer.extend_from_slice(slice);
        }

        self.append_bytes(Bytes::from(buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use common::universal_io::{
        ListedFile, OpenOptions, ReadRange, UniversalIoError, UniversalReadFs,
    };
    use futures::stream::{BoxStream, StreamExt};

    use super::*;
    use crate::AsyncWrite;

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
        ) -> impl Future<Output = Result<Vec<ListedFile>>> + Send + 'static {
            std::future::ready(Ok(vec![]))
        }

        fn exists(&self, _path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
            std::future::ready(Ok(true))
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

        fn read_from(
            &self,
            _path: &Path,
            from: u64,
        ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
        {
            let size = self.data.len() as u64;
            let tail = self.data.slice(from as usize..);
            async move { Ok((size, futures::stream::once(async move { Ok(tail) }).boxed())) }
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

        fn open(_config: &()) -> Result<Self> {
            Ok(Self::default())
        }

        fn list_files(
            &self,
            _prefix: &Path,
        ) -> impl Future<Output = Result<Vec<ListedFile>>> + Send + 'static {
            std::future::ready(Ok(vec![]))
        }

        fn exists(&self, _path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
            std::future::ready(Ok(self.store.lock().unwrap().is_some()))
        }

        fn read_range(
            &self,
            path: &Path,
            range: Range<u64>,
        ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static
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
        ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
        {
            let result = match &*self.store.lock().unwrap() {
                Some(data) => Ok((
                    data.len() as u64,
                    Bytes::copy_from_slice(&data[from as usize..]),
                )),
                None => Err(UniversalIoError::NotFound { path: path.into() }),
            };
            async move {
                let (size, tail) = result?;
                Ok((size, futures::stream::once(async move { Ok(tail) }).boxed()))
            }
        }

        fn len(&self, path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static {
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
        fn create(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
            *self.store.lock().unwrap() = Some(Vec::new());
            std::future::ready(Ok(()))
        }

        fn remove(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
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
        ) -> impl Future<Output = Result<()>> + Send + 'static {
            *self.store.lock().unwrap() = Some(bytes.to_vec());
            std::future::ready(Ok(()))
        }
    }

    impl AsyncAppend for MutableMockSource {
        fn append(
            &self,
            path: &Path,
            offset: u64,
            data: Bytes,
        ) -> impl Future<Output = Result<u64>> + Send + 'static {
            self.append_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut guard = self.store.lock().unwrap();
            // Validate the offset before materializing anything: a rejected
            // append must not leave an empty object behind.
            let current_len = guard.as_ref().map_or(0, |object| object.len() as u64);
            let result = if current_len == offset {
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
        let mut file = mutable_file(&source);

        // A rejected stale append must not materialize the object.
        let err = BridgeRuntime::global()
            .block_on(source.append(Path::new("obj"), 5, Bytes::from_static(b"x")))
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::AppendOffsetConflict { .. }));
        assert!(source.content().is_none());

        assert_eq!(file.append(b"abc".as_slice()).unwrap(), 0);
        assert_eq!(file.append(b"de".as_slice()).unwrap(), 3);
        assert_eq!(source.content().unwrap(), b"abcde");
        assert_eq!(<BlobFile<_> as UniversalRead>::len::<u8>(&file).unwrap(), 5);
    }

    #[test]
    fn append_probes_length_once() {
        let source = MutableMockSource::default();
        let mut file = mutable_file(&source);

        for _ in 0..5 {
            file.append(b"x".as_slice()).unwrap();
        }

        let len_calls = source.len_calls.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(len_calls, 1);
    }

    #[test]
    fn append_batch_is_a_single_request() {
        let source = MutableMockSource::default();
        let mut file = mutable_file(&source);

        file.append(b"start".as_slice()).unwrap();
        let batch: [&[u8]; 3] = [b"ab", b"cd", b"ef"];
        assert_eq!(file.append_batch(batch).unwrap(), 5);

        let append_calls = source
            .append_calls
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(append_calls, 2);
        assert_eq!(source.content().unwrap(), b"startabcdef");
    }

    #[test]
    fn empty_append_returns_eof_without_request() {
        let source = MutableMockSource::default();
        let mut file = mutable_file(&source);

        file.append(b"abc".as_slice()).unwrap();
        assert_eq!(file.append::<u8>(&[]).unwrap(), 3);
        assert_eq!(file.append_batch::<u8>(std::iter::empty()).unwrap(), 3);

        let append_calls = source
            .append_calls
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(append_calls, 1);
    }

    /// Two handles appending to the same object: the loser gets an offset
    /// conflict, recovers via `reopen()`, and its retry lands after the
    /// winner's bytes.
    #[test]
    fn append_conflict_recovers_after_reopen() {
        let source = MutableMockSource::default();
        let mut first = mutable_file(&source);
        let mut second = mutable_file(&source);

        assert_eq!(first.append(b"aaa".as_slice()).unwrap(), 0);
        assert_eq!(second.append(b"bbb".as_slice()).unwrap(), 3);

        let err = first.append(b"ccc".as_slice()).unwrap_err();
        assert!(matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 3, .. }
        ));

        first.reopen().unwrap();
        assert_eq!(first.append(b"ccc".as_slice()).unwrap(), 6);
        assert_eq!(source.content().unwrap(), b"aaabbbccc");
    }

    #[test]
    fn append_requires_writeable_open() {
        let fs = BlobFs::new(MutableMockSource::default(), BridgeRuntime::global());
        let mut file = fs
            .open(
                "obj",
                OpenOptions {
                    writeable: false,
                    ..OpenOptions::new_for_test()
                },
                (),
            )
            .unwrap();

        assert!(file.append(b"x".as_slice()).is_err());
    }

    #[test]
    fn append_flusher_is_a_no_op() {
        let source = MutableMockSource::default();
        let mut file = mutable_file(&source);

        file.append(b"abc".as_slice()).unwrap();
        (file.flusher())().unwrap();
    }

    #[test]
    fn blob_fs_write_file_ops_round_trip() {
        use common::universal_io::{UniversalReadFileOps as _, UniversalWriteFileOps as _};

        let source = MutableMockSource::default();
        let fs = BlobFs::new(source.clone(), BridgeRuntime::global());
        let path = Path::new("obj");

        assert!(!fs.exists(path).unwrap());
        fs.create(path, 0).unwrap();
        assert!(fs.exists(path).unwrap());

        fs.atomic_save(path, b"xyz").unwrap();
        assert_eq!(source.content().unwrap(), b"xyz");

        // Directory ops are no-ops for object stores.
        fs.create_dir(Path::new("dir")).unwrap();
        fs.remove_dir(Path::new("dir")).unwrap();

        fs.remove(path).unwrap();
        assert!(!fs.exists(path).unwrap());
    }
}
