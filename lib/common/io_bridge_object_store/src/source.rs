//! [`AsyncRead`] implementation over any [`ObjectStore`] backend.
//!
//! [`ObjectStoreSource<S>`] is the read handle: a local newtype around `Arc<S>`
//! for any [`BlobBackend`] `S`. The newtype is what lets this crate implement the
//! foreign [`AsyncRead`] trait without falling foul of the orphan rule (a blanket
//! impl on the foreign `Arc<S>` is not allowed from here). The object key is
//! supplied per call. Sync access lands through
//! [`BlobFile<ObjectStoreSource<S>>`](io_bridge::BlobFile).

// We map `object_store::Error::NotFound` specifically and intentionally bucket
// every other variant into `UniversalIoError::s3(other)`. Enumerating every
// variant just to silence the lint would couple us to upstream's variant set
// with no real benefit.
#![allow(clippy::wildcard_enum_match_arm)]

use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use common::universal_io::{ListedFile, Result, UniversalIoError, UniversalKind};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use io_bridge::{AsyncRead, AsyncWrite, OffsetByteStream};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};

use crate::append::AppendContext;
use crate::backend::BlobBackend;

/// Size of the bounded range GETs that [`AsyncRead::read_from`] splits a large
/// object into. Bounds the blast radius of a dropped connection (a retry
/// re-fetches one chunk, not the whole object) while staying large enough that
/// the per-request round-trip stays amortized.
const DEFAULT_READ_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

/// Number of follow-up chunk GETs a multi-part [`AsyncRead::read_from`] keeps
/// in flight at once. Chunks are yielded as they complete (out of order,
/// tagged with their offset); a larger value hides more per-request round-trip
/// latency at the cost of buffering up to `concurrency * chunk_size` bytes
/// ahead of the consumer.
const READ_CHUNK_CONCURRENCY: usize = 32;

/// [`AsyncRead`] handle over an object store. Holds the store as `Arc<S>` so it
/// is cheap to clone; the object key is supplied per call.
///
/// This is a thin local newtype: it exists so the crate can implement the
/// foreign [`AsyncRead`] trait for an object-store backend (the orphan rule
/// forbids a blanket impl on `Arc<S>` from a crate that owns neither `Arc` nor
/// `AsyncRead`).
pub struct ObjectStoreSource<S> {
    store: Arc<S>,
    /// Context for the native append RPC, when the backend supports it and
    /// this source was built from a config (see
    /// [`BlobBackend::append_context`]).
    append: Option<AppendContext>,
    /// Chunk size for multi-part [`AsyncRead::read_from`] reads.
    chunk_size: u64,
}

impl<S> Clone for ObjectStoreSource<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            append: self.append.clone(),
            chunk_size: self.chunk_size,
        }
    }
}

impl<S> ObjectStoreSource<S> {
    /// Wrap an already-built object store as an [`AsyncRead`] handle.
    ///
    /// Sources built this way have no [`AppendContext`] — construct from a
    /// config ([`AsyncRead::open`]) or chain
    /// [`with_append_context`](Self::with_append_context) to enable appends.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            append: None,
            chunk_size: DEFAULT_READ_CHUNK_SIZE,
        }
    }

    /// Attach an [`AppendContext`] enabling the native append RPC.
    pub fn with_append_context(mut self, context: AppendContext) -> Self {
        self.append = Some(context);
        self
    }

    /// Override the chunk size used by multi-part [`AsyncRead::read_from`]
    /// reads. Must be non-zero.
    #[cfg(test)]
    fn with_chunk_size(mut self, chunk_size: u64) -> Self {
        assert_ne!(chunk_size, 0, "read chunk size must be non-zero");
        self.chunk_size = chunk_size;
        self
    }

    /// Borrow the underlying object store.
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub(crate) fn append_context(&self) -> Option<&AppendContext> {
        self.append.as_ref()
    }
}

impl<S: BlobBackend> AsyncRead for ObjectStoreSource<S> {
    type Config = S::Config;

    fn open(config: &Self::Config) -> Result<Self> {
        Ok(Self {
            store: Arc::new(S::build_store(config)?),
            append: S::append_context(config)?,
            chunk_size: DEFAULT_READ_CHUNK_SIZE,
        })
    }

    fn list_files(
        &self,
        prefix: &Path,
    ) -> impl Future<Output = Result<Vec<ListedFile>>> + Send + 'static {
        let store = self.store.clone();
        let prefix_path = prefix.to_path_buf();
        // object_store lists by whole path segment; emulate the byte-prefix
        // contract (list the parent dir, then filter) — see `local_list_files`.
        let prefix_str = prefix.to_string_lossy().into_owned();
        let dir_prefix = prefix
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(build_dir_prefix);

        async move {
            use futures::TryStreamExt;

            match store
                .list(dir_prefix.as_ref())
                .try_collect::<Vec<object_store::ObjectMeta>>()
                .await
            {
                Ok(entries) => Ok(entries
                    .into_iter()
                    .filter_map(|e| {
                        let location = e.location.to_string();
                        location.starts_with(&prefix_str).then(|| ListedFile {
                            path: PathBuf::from(location),
                            size: e.size,
                            last_modified: Some(SystemTime::from(e.last_modified)),
                        })
                    })
                    .collect()),
                Err(object_store::Error::NotFound { .. }) => {
                    Err(UniversalIoError::NotFound { path: prefix_path })
                }
                Err(other) => Err(UniversalIoError::s3(other)),
            }
        }
    }

    fn exists(&self, path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);

        async move {
            match store.head(&key).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(other) => Err(UniversalIoError::s3(other)),
            }
        }
    }

    fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);
        async move {
            let opts = GetOptions {
                range: Some(GetRange::Bounded(range)),
                ..Default::default()
            };
            let result = store
                .get_opts(&key, opts)
                .await
                .map_err(|err| map_get_err(err, &key))?;
            Ok(result.into_stream().map_err(UniversalIoError::s3).boxed())
        }
    }

    fn read_from(
        &self,
        path: &Path,
        from: u64,
    ) -> impl Future<Output = Result<(u64, OffsetByteStream)>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);
        let chunk_size = self.chunk_size;
        async move {
            // Multi-part read: instead of one open-ended `Range: bytes=from-`
            // GET, the object is fetched in bounded chunks of `chunk_size`. The
            // first chunk doubles as the size probe — a bounded range response
            // still carries the object's total size (`Content-Range`), so no
            // separate HEAD is needed on the happy path.
            let opts = GetOptions {
                range: Some(GetRange::Bounded(from..from.saturating_add(chunk_size))),
                ..Default::default()
            };
            // Note: a bounded range is unsatisfiable on an empty object even at
            // offset 0 — the at-or-past-end error the trait documents; callers
            // that must tolerate an empty tail disambiguate with `len` (see
            // `read_from_into_byte_buffer`).
            let mut first = store
                .get_opts(&key, opts)
                .await
                .map_err(|err| map_get_err(err, &key))?;
            let total = first.meta.size;
            // Follow-up chunks are pinned to the version the probe saw: a
            // concurrent overwrite fails the read (HTTP 412) instead of
            // silently stitching bytes from two object versions.
            let e_tag = first.meta.e_tag.take();
            let first_end = first.range.end;
            let first_stream =
                io_bridge::with_running_offsets(first.into_stream().map_err(UniversalIoError::s3));
            if first_end >= total {
                return Ok((total, first_stream));
            }
            // The probe revealed the total size, so all remaining ranges are
            // known up front and up to `READ_CHUNK_CONCURRENCY` of them are
            // downloaded concurrently. Each chunk body is collected *inside*
            // its future: the unordered window only drives futures while it is
            // polled, so a future that merely opened the response and handed
            // back a body stream would stall the other transfers. Chunks are
            // yielded the moment they complete, tagged with their tail-relative
            // offsets — a slow or retried chunk never blocks delivery of the
            // others. Peak buffering is `READ_CHUNK_CONCURRENCY * chunk_size`
            // bytes ahead of the consumer.
            let rest = futures::stream::iter((first_end..total).step_by(chunk_size as usize))
                .map(move |start| {
                    let store = store.clone();
                    let key = key.clone();
                    let e_tag = e_tag.clone();
                    let end = start.saturating_add(chunk_size).min(total);
                    async move {
                        let opts = GetOptions {
                            range: Some(GetRange::Bounded(start..end)),
                            if_match: e_tag,
                            ..Default::default()
                        };
                        let chunk = store
                            .get_opts(&key, opts)
                            .await
                            .map_err(|err| map_get_err(err, &key))?;
                        // Keep the body's frames as-is instead of coalescing
                        // them into one contiguous `Bytes` — that would memcpy
                        // nearly the whole object a second time. The consumer
                        // scatters by offset, so per-frame tagging suffices.
                        let mut offset = start - from;
                        let frames: Vec<_> = chunk
                            .into_stream()
                            .map_err(UniversalIoError::s3)
                            .map_ok(|frame| {
                                let at = offset;
                                offset += frame.len() as u64;
                                (at, frame)
                            })
                            .try_collect()
                            .await?;
                        Ok::<_, UniversalIoError>(futures::stream::iter(
                            frames.into_iter().map(Ok::<_, UniversalIoError>),
                        ))
                    }
                })
                .buffer_unordered(READ_CHUNK_CONCURRENCY)
                .try_flatten();
            // `select` polls both sides, so the follow-up window opens while
            // the probe body is still streaming.
            Ok((total, futures::stream::select(first_stream, rest).boxed()))
        }
    }

    fn len(&self, path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);
        async move {
            store
                .head(&key)
                .await
                .map(|meta| meta.size)
                .map_err(|err| map_get_err(err, &key))
        }
    }

    fn kind() -> UniversalKind {
        <S as BlobBackend>::kind()
    }
}

impl<S: BlobBackend> AsyncWrite for ObjectStoreSource<S> {
    fn create(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);
        async move {
            // An empty whole-object put both creates and truncates.
            store
                .put(&key, PutPayload::default())
                .await
                .map(drop)
                .map_err(UniversalIoError::s3)
        }
    }

    fn remove(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);
        async move {
            store
                .delete(&key)
                .await
                .map_err(|err| map_get_err(err, &key))
        }
    }

    fn save(&self, path: &Path, bytes: Bytes) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.store.clone();
        let key = build_key(path);
        async move {
            // A whole-object put is atomic on object stores.
            store
                .put(&key, bytes.into())
                .await
                .map(drop)
                .map_err(UniversalIoError::s3)
        }
    }
}

pub(crate) fn build_key(path: &Path) -> object_store::path::Path {
    object_store::path::Path::from(path.to_string_lossy().as_ref())
}

/// Map an [`object_store::Error`] into [`UniversalIoError`], surfacing
/// `NotFound` with the object key as the path.
fn map_get_err(err: object_store::Error, key: &object_store::path::Path) -> UniversalIoError {
    match err {
        object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
            path: PathBuf::from(key.to_string()),
        },
        other => UniversalIoError::s3(other),
    }
}

fn build_dir_prefix(path: &Path) -> object_store::path::Path {
    let path = path.to_string_lossy();
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        object_store::path::Path::from("")
    } else {
        object_store::path::Path::from(format!("{path}/"))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common::universal_io::{ListedFile, ReadRange, UniversalAppend, UniversalRead};
    use io_bridge::{BlobFile, BridgeRuntime};
    use object_store::memory::InMemory;
    use object_store::{PutMode, UpdateVersion};

    use super::*;

    /// Test-only backend: an in-memory store with a no-op config so that
    /// unit tests exercise the full `ObjectStoreSource<S>` → `BlobFile` → pipeline
    /// stack without needing a network mock.
    #[derive(Clone, Debug, Default)]
    pub struct InMemoryConfig;

    impl BlobBackend for InMemory {
        type Config = InMemoryConfig;

        fn build_store(_config: &Self::Config) -> Result<Self> {
            Ok(InMemory::new())
        }

        fn kind() -> UniversalKind {
            UniversalKind::S3
        }
    }

    /// TEST-ONLY emulation of the native append RPC over [`InMemory`]: a
    /// head + get + conditional-put CAS loop. Production backends must be
    /// single-request (see [`crate::append`]); this exists so the `BlobFile`
    /// append stack can be exercised hermetically.
    impl io_bridge::AsyncAppend for ObjectStoreSource<InMemory> {
        fn append(
            &self,
            path: &Path,
            offset: u64,
            data: Bytes,
        ) -> impl Future<Output = Result<u64>> + Send + 'static {
            let store = self.store().clone();
            let key = build_key(path);

            async move {
                let conflict = || UniversalIoError::AppendOffsetConflict {
                    path: PathBuf::from(key.to_string()),
                    offset,
                };

                loop {
                    match store.head(&key).await {
                        Ok(meta) => {
                            if meta.size != offset {
                                return Err(conflict());
                            }

                            let existing = store
                                .get(&key)
                                .await
                                .map_err(UniversalIoError::s3)?
                                .bytes()
                                .await
                                .map_err(UniversalIoError::s3)?;
                            let mut combined = Vec::with_capacity(existing.len() + data.len());
                            combined.extend_from_slice(&existing);
                            combined.extend_from_slice(&data);

                            let update = PutMode::Update(UpdateVersion {
                                e_tag: meta.e_tag.clone(),
                                version: meta.version.clone(),
                            });
                            match store.put_opts(&key, combined.into(), update.into()).await {
                                Ok(_) => return Ok(offset + data.len() as u64),
                                // Lost a race; retry from a fresh head.
                                Err(object_store::Error::Precondition { .. }) => {}
                                Err(other) => return Err(UniversalIoError::s3(other)),
                            }
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            if offset != 0 {
                                return Err(conflict());
                            }

                            let create = PutMode::Create;
                            match store
                                .put_opts(&key, data.clone().into(), create.into())
                                .await
                            {
                                Ok(_) => return Ok(data.len() as u64),
                                // Lost a race; retry from a fresh head.
                                Err(object_store::Error::AlreadyExists { .. }) => {}
                                Err(other) => return Err(UniversalIoError::s3(other)),
                            }
                        }
                        Err(other) => return Err(UniversalIoError::s3(other)),
                    }
                }
            }
        }
    }

    type InMemorySource = ObjectStoreSource<InMemory>;

    fn make_file(
        runtime: BridgeRuntime,
        store: Arc<InMemory>,
        key: &str,
    ) -> BlobFile<InMemorySource> {
        BlobFile::new(ObjectStoreSource::new(store), runtime, PathBuf::from(key))
    }

    fn inmemory_with(runtime: &BridgeRuntime, objects: &[(&str, &'static [u8])]) -> Arc<InMemory> {
        let store = Arc::new(InMemory::new());
        runtime.block_on(async {
            for (k, v) in objects {
                store
                    .put(
                        &object_store::path::Path::from(*k),
                        Bytes::from_static(v).into(),
                    )
                    .await
                    .unwrap();
            }
        });
        store
    }

    #[test]
    fn read_full_range() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"hello world")]);
        let file = make_file(runtime, store, "obj");
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 11))
            .expect("read");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn read_subrange() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"hello world")]);
        let file = make_file(runtime, store, "obj");
        let cow = file
            .read::<common::generic_consts::Random, u8>(ReadRange::new(6, 5))
            .expect("read");
        assert_eq!(&cow[..], b"world");
    }

    #[test]
    fn read_batch_returns_all_pairs() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("merged", b"helloWORLDxyz")]);
        let file = make_file(runtime, store, "merged");

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

    /// Drive `read_from` directly and reassemble the streamed `(offset, bytes)`
    /// chunks, asserting they are disjoint and tile the tail exactly.
    fn collect_read_from(
        runtime: &BridgeRuntime,
        source: &InMemorySource,
        key: &str,
        from: u64,
    ) -> (u64, Vec<u8>) {
        runtime.block_on(async {
            let (total, mut stream) = source.read_from(Path::new(key), from).await.expect("read");
            let mut pieces = Vec::new();
            while let Some(chunk) = stream.next().await {
                pieces.push(chunk.expect("chunk"));
            }
            pieces.sort_by_key(|(offset, _)| *offset);
            let mut bytes = Vec::new();
            for (offset, piece) in pieces {
                assert_eq!(offset as usize, bytes.len(), "chunks must tile the tail");
                bytes.extend_from_slice(&piece);
            }
            (total, bytes)
        })
    }

    #[test]
    fn read_from_in_chunks() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"0123456789")]);
        let source = ObjectStoreSource::new(store).with_chunk_size(3);

        let (total, bytes) = collect_read_from(&runtime, &source, "obj", 0);
        assert_eq!(total, 10);
        assert_eq!(bytes, b"0123456789");

        let (total, bytes) = collect_read_from(&runtime, &source, "obj", 4);
        assert_eq!(total, 10);
        assert_eq!(bytes, b"456789");

        // Tail smaller than one chunk: served entirely by the probe request.
        let (total, bytes) = collect_read_from(&runtime, &source, "obj", 8);
        assert_eq!(total, 10);
        assert_eq!(bytes, b"89");
    }

    #[test]
    fn read_from_more_chunks_than_concurrency_reassembles_exactly() {
        let runtime = BridgeRuntime::global();
        let store = Arc::new(InMemory::new());
        let data: Vec<u8> = (0..=255).collect();
        runtime.block_on(async {
            store
                .put(
                    &object_store::path::Path::from("obj"),
                    Bytes::from(data.clone()).into(),
                )
                .await
                .unwrap();
        });
        // 7-byte chunks over 256 bytes: ~36 follow-up GETs racing through the
        // unordered window; the offset-tagged chunks must tile the object
        // exactly whatever order they complete in.
        let source = ObjectStoreSource::new(store).with_chunk_size(7);
        let (total, bytes) = collect_read_from(&runtime, &source, "obj", 0);
        assert_eq!(total, 256);
        assert_eq!(bytes, data);
    }

    /// A bounded probe on an empty object is an unsatisfiable range, so the
    /// raw `read_from` errors; the pipeline's `len` disambiguation (see
    /// `read_from_into_byte_buffer`) turns that into an empty buffer.
    #[test]
    fn read_whole_empty_object_yields_empty_buffer() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("empty", b"")]);

        let source = ObjectStoreSource::new(store.clone());
        assert!(
            runtime
                .block_on(source.read_from(Path::new("empty"), 0))
                .is_err(),
            "bounded probe on an empty object is unsatisfiable"
        );

        let file = make_file(runtime, store, "empty");
        let bytes = file.read_whole::<u8>().expect("read_whole");
        assert!(bytes.is_empty());
    }

    #[test]
    fn read_whole_through_blob_file_in_chunks() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"hello world")]);
        let file = BlobFile::new(
            ObjectStoreSource::new(store).with_chunk_size(4),
            runtime,
            PathBuf::from("obj"),
        );
        let cow = file.read_whole::<u8>().expect("read_whole");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn read_from_fails_on_concurrent_overwrite() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"0123456789")]);
        let source = ObjectStoreSource::new(store.clone()).with_chunk_size(4);
        runtime.block_on(async {
            // The probe resolves inside `read_from` and pins the ETag; the
            // follow-up GETs only go out once the stream is polled. Overwrite
            // in between: every follow-up chunk must fail the `if_match`
            // precondition rather than stitch bytes from two versions.
            let (total, mut stream) = source.read_from(Path::new("obj"), 0).await.expect("read");
            assert_eq!(total, 10);
            store
                .put(
                    &object_store::path::Path::from("obj"),
                    Bytes::from_static(b"XXXXXXXXXX").into(),
                )
                .await
                .unwrap();
            let mut results = Vec::new();
            while let Some(item) = stream.next().await {
                results.push(item);
            }
            assert!(
                results.iter().any(Result::is_err),
                "expected a precondition failure, got {results:?}"
            );
        });
    }

    #[test]
    fn kind_is_inmemory_tagged_as_s3() {
        assert_eq!(<InMemorySource as AsyncRead>::kind(), UniversalKind::S3);
    }

    #[test]
    fn list_files_byte_prefixes_final_component() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(
            &runtime,
            &[
                ("dir/page_0.dat", b"a"),
                ("dir/page_1.dat", b"b"),
                ("dir/tracker.dat", b"c"),
                ("dir/sub/page_9.dat", b"d"),
                ("other/page_0.dat", b"e"),
            ],
        );
        let source = ObjectStoreSource::new(store);
        let mut files: Vec<(String, u64)> = runtime
            .block_on(source.list_files(Path::new("dir/page_")))
            .expect("list_files")
            .into_iter()
            .map(
                |ListedFile {
                     path,
                     size,
                     last_modified: _,
                 }| (path.to_string_lossy().into_owned(), size),
            )
            .collect();
        files.sort();
        assert_eq!(
            files,
            [
                ("dir/page_0.dat".to_string(), 1),
                ("dir/page_1.dat".to_string(), 1),
            ]
        );
    }

    #[test]
    fn populate_and_clear_are_noops() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("o", b"x")]);
        let file = make_file(runtime, store, "o");
        file.populate().unwrap();
        file.clear_ram_cache().unwrap();
    }

    #[test]
    fn len_divides_by_type_size() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"\x01\x00\x02\x00")]);
        let file = make_file(runtime, store, "obj");
        let len: u64 = <BlobFile<InMemorySource> as UniversalRead>::len::<u16>(&file).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn read_only_wrapper_compiles_with_blob_file() {
        use common::universal_io::ReadOnly;
        fn assert_universal_read<R: UniversalRead>() {}
        assert_universal_read::<ReadOnly<BlobFile<InMemorySource>>>();
    }

    /// The backend-generic append battery from `common`, run over the S3
    /// stack (`BlobFs`/`BlobFile` over an object store) via the in-memory
    /// append emulation. The real write-offset RPC is covered by the gated
    /// `test_native_append_flow` integration test.
    #[test]
    fn append_conformance_over_object_store() {
        let fs = io_bridge::BlobFs::new(
            ObjectStoreSource::new(Arc::new(InMemory::new())),
            BridgeRuntime::global(),
        );
        common::universal_io::conformance::run_append_conformance(&fs, Path::new("conformance"));
    }

    #[test]
    fn append_through_blob_file() {
        let runtime = BridgeRuntime::global();
        let store = Arc::new(InMemory::new());
        let mut file = make_file(runtime, store, "log");

        // Create-on-first-append at offset 0, then single-request batches.
        file.append(0, b"hello ".as_slice()).unwrap();
        file.append(6, b"world".as_slice()).unwrap();
        let batch: [&[u8]; 2] = [b"!", b"?"];
        file.append_batch(11, batch).unwrap();

        let bytes = file.read_whole::<u8>().unwrap();
        assert_eq!(&bytes[..], b"hello world!?");
    }
}
