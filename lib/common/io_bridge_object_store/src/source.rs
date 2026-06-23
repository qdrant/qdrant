//! [`AsyncRead`] implementation over any [`ObjectStore`] backend. The store
//! itself (held as `Arc<S>`) is the read handle; the object key is supplied per
//! call. Sync access lands through [`BlobFile<Arc<S>>`](crate::BlobFile).

// We map `object_store::Error::NotFound` specifically and intentionally bucket
// every other variant into `UniversalIoError::s3(other)`. Enumerating every
// variant just to silence the lint would couple us to upstream's variant set
// with no real benefit.
#![allow(clippy::wildcard_enum_match_arm)]

use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError, UniversalKind};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};

use crate::backend::BlobBackend;
use crate::read::AsyncRead;

impl<S: BlobBackend> AsyncRead for Arc<S> {
    type Config = S::Config;

    fn open(config: &Self::Config) -> Result<Self> {
        Ok(Arc::new(S::build_store(config)?))
    }

    fn list_files(
        &self,
        prefix: &Path,
    ) -> impl Future<Output = Result<Vec<PathBuf>>> + Send + 'static {
        let store = self.clone();
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
                    .map(|e| e.location.to_string())
                    .filter(|key| key.starts_with(&prefix_str))
                    .map(PathBuf::from)
                    .collect()),
                Err(object_store::Error::NotFound { .. }) => {
                    Err(UniversalIoError::NotFound { path: prefix_path })
                }
                Err(other) => Err(UniversalIoError::s3(other)),
            }
        }
    }

    fn exists(&self, path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
        let store = self.clone();
        let key = build_key(path);

        async move {
            match store.head(&key).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(other) => Err(UniversalIoError::s3(other)),
            }
        }
    }

    fn create(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.clone();
        let key = build_key(path);

        async move {
            store
                .put(&key, Bytes::new().into())
                .await
                .map(drop)
                .map_err(UniversalIoError::s3)
        }
    }

    fn remove(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.clone();
        let key = build_key(path);

        async move {
            store.delete(&key).await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                err => UniversalIoError::s3(err),
            })
        }
    }

    fn remove_dir(&self, path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.clone();
        let prefix_path = path.to_path_buf();
        let prefix = build_dir_prefix(path);

        async move {
            let mut objects = store.list(Some(&prefix));
            while let Some(meta) = objects.try_next().await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: prefix_path.clone(),
                },
                other => UniversalIoError::s3(other),
            })? {
                match store.delete(&meta.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(other) => return Err(UniversalIoError::s3(other)),
                }
            }

            Ok(())
        }
    }

    fn atomic_save(
        &self,
        path: &Path,
        bytes: Bytes,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let store = self.clone();
        let key = build_key(path);

        async move {
            store
                .put(&key, bytes.into())
                .await
                .map(drop)
                .map_err(UniversalIoError::s3)
        }
    }

    fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static {
        let store = self.clone();
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

    fn len(&self, path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static {
        let store = self.clone();
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

fn build_key(path: &Path) -> object_store::path::Path {
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
    use common::universal_io::{ReadRange, UniversalRead, UniversalReadFileOps};
    use object_store::memory::InMemory;

    use super::*;
    use crate::file::BlobFile;
    use crate::runtime::BridgeRuntime;

    /// Test-only backend: an in-memory store with a no-op config so that
    /// unit tests exercise the full `Arc<S>` → `BlobFile` → pipeline stack
    /// without needing a network mock.
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

    fn make_file(
        runtime: BridgeRuntime,
        store: Arc<InMemory>,
        key: &str,
    ) -> BlobFile<Arc<InMemory>> {
        BlobFile::new(store, runtime, PathBuf::from(key))
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
        assert_eq!(<Arc<InMemory> as AsyncRead>::kind(), UniversalKind::S3);
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
        let mut files: Vec<String> = runtime
            .block_on(store.list_files(Path::new("dir/page_")))
            .expect("list_files")
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        files.sort();
        assert_eq!(files, ["dir/page_0.dat", "dir/page_1.dat"]);
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
        let len: u64 = <BlobFile<Arc<InMemory>> as UniversalRead>::len::<u16>(&file).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn fs_create_writes_empty_object_and_create_dir_is_noop() {
        let runtime = BridgeRuntime::global();
        let store = Arc::new(InMemory::new());
        let fs = crate::BlobFs::new(store.clone(), runtime.clone());

        fs.create_dir(Path::new("prefix")).unwrap();
        fs.create(Path::new("prefix/empty"), 1024).unwrap();
        fs.create(Path::new("prefix/nested/empty"), 1024).unwrap();
        fs.create(Path::new("prefix-sibling/empty"), 1024).unwrap();
        fs.atomic_save(Path::new("prefix/saved"), b"saved").unwrap();

        assert!(fs.exists(Path::new("prefix/empty")).unwrap());
        let len = runtime.block_on(store.head(&object_store::path::Path::from("prefix/empty")));
        assert_eq!(len.unwrap().size, 0);
        let len = runtime.block_on(store.head(&object_store::path::Path::from("prefix/saved")));
        assert_eq!(len.unwrap().size, 5);

        fs.remove(Path::new("prefix/empty")).unwrap();
        fs.remove_dir(Path::new("prefix")).unwrap();
        assert!(!fs.exists(Path::new("prefix/empty")).unwrap());
        assert!(!fs.exists(Path::new("prefix/nested/empty")).unwrap());
        assert!(!fs.exists(Path::new("prefix/saved")).unwrap());
        assert!(fs.exists(Path::new("prefix-sibling/empty")).unwrap());
    }

    #[test]
    fn read_only_wrapper_compiles_with_blob_file() {
        use common::universal_io::ReadOnly;
        fn assert_universal_read<R: UniversalRead>() {}
        assert_universal_read::<ReadOnly<BlobFile<Arc<InMemory>>>>();
    }
}
