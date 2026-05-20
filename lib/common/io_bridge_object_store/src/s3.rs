//! S3 backend on top of `object_store::aws`. Provides a single concrete
//! [`AsyncRead`] implementation; sync access lands through [`BlobFile<S3Source>`].

// We map `object_store::Error::NotFound` specifically and intentionally bucket
// every other variant into `UniversalIoError::s3(other)`. Enumerating every
// variant just to silence the lint would couple us to upstream's variant set
// with no real benefit.
#![allow(clippy::wildcard_enum_match_arm)]

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::config::{S3Config, build_object_store};
use crate::file::BlobFile;
use crate::read::{AsyncRead, resolve_runtime};
use crate::runtime::BridgeRuntime;

/// Per-object handle held inside a [`BlobFile`]: store, object key, cached
/// length. The cached length is populated by HEAD inside [`Self::open`].
#[derive(Clone)]
pub struct S3Source {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) key: object_store::path::Path,
    pub(crate) len_bytes: u64,
}

impl S3Source {
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn key(&self) -> &object_store::path::Path {
        &self.key
    }
}

impl AsyncRead for S3Source {
    type Config = S3Config;

    fn open(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        key: &Path,
    ) -> Result<BlobFile<Self>> {
        let runtime = resolve_runtime(runtime);
        let store = build_object_store(config)?;
        let key = build_key(key);
        let meta = runtime
            .block_on(async { store.head(&key).await })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })?;
        Ok(BlobFile::new(
            Self {
                store,
                key,
                len_bytes: meta.size,
            },
            runtime,
        ))
    }

    fn list_files(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        prefix: &Path,
    ) -> Result<Vec<PathBuf>> {
        let runtime = resolve_runtime(runtime);
        let store = build_object_store(config)?;
        let prefix_path = prefix.to_path_buf();
        let prefix = build_key(prefix);
        let entries: Vec<object_store::ObjectMeta> = runtime
            .block_on(async {
                use futures::TryStreamExt;
                store.list(Some(&prefix)).try_collect().await
            })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => {
                    UniversalIoError::NotFound { path: prefix_path }
                }
                other => UniversalIoError::s3(other),
            })?;
        Ok(entries
            .into_iter()
            .map(|e| PathBuf::from(e.location.to_string()))
            .collect())
    }

    fn exists(runtime: Option<BridgeRuntime>, config: &Self::Config, path: &Path) -> Result<bool> {
        let runtime = resolve_runtime(runtime);
        let store = build_object_store(config)?;
        let key = build_key(path);
        match runtime.block_on(async { store.head(&key).await }) {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(other) => Err(UniversalIoError::s3(other)),
        }
    }

    fn read_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> impl Future<Output = Result<Bytes>> + Send + 'static {
        let store = self.store.clone();
        let key = self.key.clone();
        async move {
            store.get_range(&key, range).await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })
        }
    }

    fn len(&self) -> u64 {
        self.len_bytes
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}

fn build_key(path: &Path) -> object_store::path::Path {
    object_store::path::Path::from(path.to_string_lossy().as_ref())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use common::universal_io::{ReadRange, UniversalRead};
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt as _};

    use super::*;

    fn make_file(
        runtime: BridgeRuntime,
        store: Arc<dyn ObjectStore>,
        key: &str,
    ) -> BlobFile<S3Source> {
        let key = object_store::path::Path::from(key);
        let meta = runtime
            .block_on(async { store.head(&key).await })
            .expect("head");
        BlobFile::new(
            S3Source {
                store,
                key,
                len_bytes: meta.size,
            },
            runtime,
        )
    }

    fn inmemory_with(
        runtime: &BridgeRuntime,
        objects: &[(&str, &'static [u8])],
    ) -> Arc<dyn ObjectStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
    fn kind_is_s3() {
        assert_eq!(<S3Source as AsyncRead>::kind(), UniversalKind::S3);
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
        let len: u64 = <BlobFile<S3Source> as UniversalRead>::len::<u16>(&file).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn read_only_wrapper_compiles_with_blob_file_s3() {
        use common::universal_io::ReadOnly;
        fn assert_universal_read<R: UniversalRead>() {}
        assert_universal_read::<ReadOnly<BlobFile<S3Source>>>();
    }
}
