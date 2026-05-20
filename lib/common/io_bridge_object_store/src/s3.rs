use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::{ObjectStore, ObjectStoreExt};

use crate::file::BlobFile;
use crate::read::{BlobRead, resolve_runtime};
use crate::config::{S3Config, build_object_store};
use crate::runtime::BridgeRuntime;

#[derive(Clone)]
pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    key: object_store::path::Path,
    len_bytes: u64,
}

impl S3Source {
    pub fn new(store: Arc<dyn ObjectStore>, key: object_store::path::Path, len_bytes: u64) -> Self {
        Self {
            store,
            key,
            len_bytes,
        }
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn key(&self) -> &object_store::path::Path {
        &self.key
    }

    /// Open against a pre-built [`Arc<dyn ObjectStore>`]. Useful for tests with
    /// `InMemory` and for callers that want to share a store across many
    /// `BlobFile` instances without re-running the AWS builder.
    pub fn open_with_store(
        runtime: Option<BridgeRuntime>,
        store: Arc<dyn ObjectStore>,
        key: &Path,
    ) -> Result<BlobFile<Self>> {
        let runtime = resolve_runtime(runtime);
        let key = build_key(key);
        let meta = runtime
            .block_on(async { store.head(&key).await })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })?;
        let source = Self::new(store, key, meta.size);
        Ok(BlobFile::new(source, runtime))
    }

    pub fn list_files_with_store(
        runtime: Option<BridgeRuntime>,
        store: Arc<dyn ObjectStore>,
        prefix: &Path,
    ) -> Result<Vec<PathBuf>> {
        let runtime = resolve_runtime(runtime);
        let prefix_path = prefix.to_path_buf();
        let prefix = build_key(prefix);
        let entries: Vec<object_store::ObjectMeta> = runtime
            .block_on(async {
                use futures::TryStreamExt;
                store.list(Some(&prefix)).try_collect().await
            })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: prefix_path,
                },
                other => UniversalIoError::s3(other),
            })?;
        Ok(entries
            .into_iter()
            .map(|e| PathBuf::from(e.location.to_string()))
            .collect())
    }

    pub fn exists_with_store(
        runtime: Option<BridgeRuntime>,
        store: Arc<dyn ObjectStore>,
        path: &Path,
    ) -> Result<bool> {
        let runtime = resolve_runtime(runtime);
        let key = build_key(path);
        match runtime.block_on(async { store.head(&key).await }) {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(other) => Err(UniversalIoError::s3(other)),
        }
    }
}

impl BlobRead for S3Source {
    type Config = S3Config;

    fn open(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        key: &Path,
    ) -> Result<BlobFile<Self>> {
        let store = build_object_store(config)?;
        Self::open_with_store(runtime, store, key)
    }

    fn list_files(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        prefix: &Path,
    ) -> Result<Vec<PathBuf>> {
        let store = build_object_store(config)?;
        Self::list_files_with_store(runtime, store, prefix)
    }

    fn exists(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        path: &Path,
    ) -> Result<bool> {
        let store = build_object_store(config)?;
        Self::exists_with_store(runtime, store, path)
    }

    fn read_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'static>> {
        let store = self.store.clone();
        let key = self.key.clone();
        Box::pin(async move {
            store.get_range(&key, range).await.map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })
        })
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
    fn open_with_store_then_read_full() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"hello world")]);
        let file = S3Source::open_with_store(Some(runtime), store, Path::new("obj")).expect("open");
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 11))
            .expect("read");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn open_with_store_then_read_subrange() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("obj", b"hello world")]);
        let file = S3Source::open_with_store(Some(runtime), store, Path::new("obj")).expect("open");
        let cow = file
            .read::<common::generic_consts::Random, u8>(ReadRange::new(6, 5))
            .expect("read");
        assert_eq!(&cow[..], b"world");
    }

    #[test]
    fn open_with_store_uses_global_when_runtime_none() {
        let global = BridgeRuntime::global();
        let store = inmemory_with(&global, &[("obj", b"abc")]);
        let file = S3Source::open_with_store(None, store, Path::new("obj")).expect("open");
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 3))
            .expect("read");
        assert_eq!(&cow[..], b"abc");
    }

    #[test]
    fn open_with_store_not_found_maps_to_not_found_error() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[]);
        let err = S3Source::open_with_store(Some(runtime), store, Path::new("missing")).unwrap_err();
        assert!(matches!(err, UniversalIoError::NotFound { .. }));
    }

    #[test]
    fn read_batch_returns_all_pairs() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("merged", b"helloWORLDxyz")]);
        let file = S3Source::open_with_store(Some(runtime), store, Path::new("merged")).expect("open");

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
    fn read_batch_uses_explicit_runtime_when_given() {
        let custom = BridgeRuntime::new().expect("custom runtime");
        let store = inmemory_with(&custom, &[("x", b"AAAAA")]);
        let file = S3Source::open_with_store(Some(custom), store, Path::new("x")).expect("open");
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 5))
            .expect("read");
        assert_eq!(&cow[..], b"AAAAA");
    }

    #[test]
    fn list_files_with_store_lists_matching_prefix() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(
            &runtime,
            &[
                ("listed/a", b"x"),
                ("listed/b", b"x"),
                ("other/z", b"x"),
            ],
        );
        let files = S3Source::list_files_with_store(Some(runtime), store, Path::new("listed"))
            .expect("list");
        assert_eq!(files.len(), 2);
        for f in &files {
            assert!(f.to_string_lossy().starts_with("listed/"));
        }
    }

    #[test]
    fn exists_with_store_true_and_false() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("here", b"x")]);
        assert!(
            S3Source::exists_with_store(Some(runtime.clone()), store.clone(), Path::new("here"))
                .unwrap()
        );
        assert!(
            !S3Source::exists_with_store(Some(runtime), store, Path::new("not-here")).unwrap()
        );
    }

    #[test]
    fn s3_source_kind_is_s3() {
        assert_eq!(<S3Source as BlobRead>::kind(), UniversalKind::S3);
    }

    #[test]
    fn populate_and_clear_are_noops() {
        let runtime = BridgeRuntime::global();
        let store = inmemory_with(&runtime, &[("o", b"x")]);
        let file = S3Source::open_with_store(Some(runtime), store, Path::new("o")).expect("open");
        file.populate().unwrap();
        file.clear_ram_cache().unwrap();
    }

    #[test]
    fn read_only_wrapper_compiles_with_bridge_s3() {
        use common::universal_io::ReadOnly;
        fn assert_universal_read<R: UniversalRead>() {}
        assert_universal_read::<ReadOnly<BlobFile<S3Source>>>();
    }
}
