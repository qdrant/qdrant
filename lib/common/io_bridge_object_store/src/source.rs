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
use io_bridge::{AsyncRead, AsyncWrite};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};

use crate::append::AppendContext;
use crate::backend::BlobBackend;

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
}

impl<S> Clone for ObjectStoreSource<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            append: self.append.clone(),
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
        }
    }

    /// Attach an [`AppendContext`] enabling the native append RPC.
    pub fn with_append_context(mut self, context: AppendContext) -> Self {
        self.append = Some(context);
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
            let result = store.get_opts(&key, opts).await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })?;
            Ok(result.into_stream().map_err(UniversalIoError::s3).boxed())
        }
    }

    fn read_from(
        &self,
        path: &Path,
        from: u64,
    ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
    {
        let store = self.store.clone();
        let key = build_key(path);
        async move {
            // `from == 0` is a plain whole-object GET; a positive offset asks the
            // backend for everything from `from` onward in a single open-ended
            // GET (`Range: bytes=from-`). Either way the response carries the
            // object's total size, so no separate HEAD is needed.
            let opts = GetOptions {
                range: (from > 0).then_some(GetRange::Offset(from)),
                ..Default::default()
            };
            let result = store.get_opts(&key, opts).await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })?;
            let size = result.meta.size;
            let stream = result.into_stream().map_err(UniversalIoError::s3).boxed();
            Ok((size, stream))
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
                .map_err(|err| match err {
                    object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                        path: PathBuf::from(key.to_string()),
                    },
                    other => UniversalIoError::s3(other),
                })
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
            store.delete(&key).await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })
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

    #[test]
    fn append_through_blob_file() {
        let runtime = BridgeRuntime::global();
        let store = Arc::new(InMemory::new());
        let mut file = make_file(runtime, store, "log");

        // Create-on-first-append, offsets, single-request batches.
        assert_eq!(file.append(b"hello ".as_slice()).unwrap(), 0);
        assert_eq!(file.append(b"world".as_slice()).unwrap(), 6);
        let batch: [&[u8]; 2] = [b"!", b"?"];
        assert_eq!(file.append_batch(batch).unwrap(), 11);

        let bytes = file.read_whole::<u8>().unwrap();
        assert_eq!(&bytes[..], b"hello world!?");
    }

    /// Two handles appending to the same object: the one with a stale
    /// cached offset gets a conflict, recovers via `reopen()`, and its
    /// retry lands after the winner's bytes.
    #[test]
    fn append_conflict_recovers_after_reopen() {
        let runtime = BridgeRuntime::global();
        let store = Arc::new(InMemory::new());
        let mut first = make_file(runtime.clone(), store.clone(), "log");
        let mut second = make_file(runtime, store, "log");

        assert_eq!(first.append(b"aaa".as_slice()).unwrap(), 0);
        assert_eq!(second.append(b"bbb".as_slice()).unwrap(), 3);

        let err = first.append(b"ccc".as_slice()).unwrap_err();
        assert!(matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 3, .. }
        ));

        first.reopen().unwrap();
        assert_eq!(first.append(b"ccc".as_slice()).unwrap(), 6);

        let bytes = first.read_whole::<u8>().unwrap();
        assert_eq!(&bytes[..], b"aaabbbccc");
    }

    /// The full stack: `DiskCache` (write-through mirror) over `BlobFile`
    /// (offset tracking) over an object store — create, append, read back.
    #[test]
    fn append_through_disk_cache_over_object_store() {
        use common::mmap::AdviceSetting;
        use common::universal_io::{
            DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, OpenOptions, Populate,
            UniversalReadFileOps as _, UniversalReadFs as _, UniversalWriteFileOps as _,
        };

        let tmp = tempfile::tempdir().unwrap();
        let local_dir = tmp.path().to_path_buf();

        let fs = DiskCacheFs::<BlobFile<InMemorySource>>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(PathBuf::from("remote"), local_dir).unwrap()),
            remote: InMemoryConfig,
        })
        .unwrap();

        let path = Path::new("remote/log.bin");
        // The mirror materializes from the remote length, so the object must
        // exist before the cache can be used — same create-then-open flow as
        // the local backends.
        fs.create(path, 0).unwrap();
        assert!(fs.exists(path).unwrap());

        let mut cache = fs
            .open(
                path,
                OpenOptions {
                    writeable: true,
                    need_sequential: false,
                    populate: Populate::No,
                    advice: AdviceSetting::Global,
                },
                Default::default(),
            )
            .unwrap();

        assert_eq!(cache.append(b"hello ".as_slice()).unwrap(), 0);
        assert_eq!(cache.append(b"world".as_slice()).unwrap(), 6);
        let batch: [&[u8]; 2] = [b"!", b"?"];
        assert_eq!(cache.append_batch(batch).unwrap(), 11);
        assert_eq!(cache.len::<u8>().unwrap(), 13);

        let bytes = cache.read_whole::<u8>().unwrap();
        assert_eq!(&bytes[..], b"hello world!?");

        let bytes = cache
            .read::<common::generic_consts::Random, u8>(ReadRange::new(6, 7))
            .unwrap();
        assert_eq!(&bytes[..], b"world!?");
    }
}
