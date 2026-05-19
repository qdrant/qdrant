use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};

use common::generic_consts::AccessPattern;
use common::universal_io::{
    OpenOptions, ReadRange, Result, UniversalIoError, UniversalKind, UniversalRead,
    UniversalReadFileOps,
};

use crate::config::build_object_store;
use crate::pipeline::{BorrowedS3ReadPipeline, OwnedS3ReadPipeline};
use crate::runtime::S3RuntimeHandle;
use crate::{S3Context, current_s3_context};

#[derive(Debug)]
pub struct S3File {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) key: object_store::path::Path,
    pub(crate) len_bytes: u64,
    pub(crate) runtime: S3RuntimeHandle,
}

impl S3File {
    fn ctx() -> Result<S3Context> {
        current_s3_context().ok_or_else(|| UniversalIoError::S3Config {
            description: "S3Context not set; call push_s3_context() before invoking this S3 operation".into(),
        })
    }

    fn build_key(path: &Path) -> object_store::path::Path {
        object_store::path::Path::from(path.to_string_lossy().as_ref())
    }
}

impl UniversalReadFileOps for S3File {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        let ctx = Self::ctx()?;
        let runtime = ctx.runtime.unwrap_or_else(S3RuntimeHandle::global);
        let store = build_object_store(&ctx.config)?;
        let prefix = Self::build_key(prefix_path);
        let entries: Vec<object_store::ObjectMeta> = runtime
            .block_on(async {
                use futures::TryStreamExt;
                store.list(Some(&prefix)).try_collect().await
            })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: prefix_path.to_path_buf(),
                },
                other => UniversalIoError::s3(other),
            })?;
        Ok(entries
            .into_iter()
            .map(|e| PathBuf::from(e.location.to_string()))
            .collect())
    }

    fn exists(path: &Path) -> Result<bool> {
        let ctx = Self::ctx()?;
        let runtime = ctx.runtime.unwrap_or_else(S3RuntimeHandle::global);
        let store = build_object_store(&ctx.config)?;
        let key = Self::build_key(path);
        match runtime.block_on(async { store.head(&key).await }) {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(other) => Err(UniversalIoError::s3(other)),
        }
    }
}

impl UniversalRead for S3File {
    type BorrowedReadPipeline<'a, T, U>
        = BorrowedS3ReadPipeline<'a, T, U>
    where
        T: bytemuck::Pod,
        U: common::universal_io::UserData;

    type OwnedReadPipeline<T, U>
        = OwnedS3ReadPipeline<T, U>
    where
        T: bytemuck::Pod,
        U: common::universal_io::UserData;

    fn open(path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self> {
        let ctx = Self::ctx()?;
        let runtime = ctx.runtime.unwrap_or_else(S3RuntimeHandle::global);
        let store = build_object_store(&ctx.config)?;
        let key = Self::build_key(path.as_ref());
        let meta = runtime
            .block_on(async { store.head(&key).await })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: path.as_ref().to_path_buf(),
                },
                other => UniversalIoError::s3(other),
            })?;
        Ok(Self {
            store,
            key,
            len_bytes: meta.size,
            runtime,
        })
    }

    fn read<P: AccessPattern, T: bytemuck::Pod>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;
        let bytes = self
            .runtime
            .block_on(async { self.store.get_range(&self.key, start..end).await })
            .map_err(|err| match err {
                object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                    path: std::path::PathBuf::from(self.key.to_string()),
                },
                other => UniversalIoError::s3(other),
            })?;
        let items: &[T] = bytemuck::try_cast_slice(&bytes)?;
        Ok(Cow::Owned(items.to_vec()))
    }

    fn len<T>(&self) -> Result<u64> {
        let item_size = size_of::<T>() as u64;
        debug_assert_eq!(self.len_bytes % item_size, 0);
        Ok(self.len_bytes / item_size)
    }

    fn populate(&self) -> Result<()> {
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{S3Config, S3Credentials};
    use bytes::Bytes;
    use object_store::memory::InMemory;

    fn make_inmemory_store(
        runtime: &S3RuntimeHandle,
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

    #[allow(dead_code)]
    fn dummy_config() -> S3Config {
        S3Config {
            bucket: "dummy".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://localhost:9000".into()),
            credentials: S3Credentials::Static {
                access_key_id: "ak".into(),
                secret_access_key: "sk".into(),
                session_token: None,
            },
        }
    }

    /// Test helper: bypass the build_object_store path by constructing S3File directly
    /// with an InMemory store. Useful for unit tests that exercise read paths without
    /// touching the AWS builder.
    fn make_file(runtime: S3RuntimeHandle, store: Arc<dyn ObjectStore>, key: &str) -> S3File {
        let meta = runtime.block_on(async {
            store
                .head(&object_store::path::Path::from(key))
                .await
                .unwrap()
        });
        S3File {
            store,
            key: object_store::path::Path::from(key),
            len_bytes: meta.size,
            runtime,
        }
    }

    #[test]
    fn read_full_range_returns_bytes() {
        let runtime = S3RuntimeHandle::global();
        let store = make_inmemory_store(&runtime, &[("obj", b"hello world")]);
        let file = make_file(runtime, store, "obj");
        let cow = file
            .read::<common::generic_consts::Sequential, u8>(ReadRange::new(0, 11))
            .expect("read");
        assert_eq!(&cow[..], b"hello world");
    }

    #[test]
    fn read_subrange_returns_slice() {
        let runtime = S3RuntimeHandle::global();
        let store = make_inmemory_store(&runtime, &[("obj", b"hello world")]);
        let file = make_file(runtime, store, "obj");
        let cow = file
            .read::<common::generic_consts::Random, u8>(ReadRange::new(6, 5))
            .expect("read");
        assert_eq!(&cow[..], b"world");
    }

    #[test]
    fn len_divides_by_type_size() {
        let runtime = S3RuntimeHandle::global();
        let store = make_inmemory_store(&runtime, &[("obj", b"\x01\x00\x02\x00")]);
        let file = make_file(runtime, store, "obj");
        let len: u64 = <S3File as UniversalRead>::len::<u16>(&file).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn populate_and_clear_are_noops() {
        let runtime = S3RuntimeHandle::global();
        let store = make_inmemory_store(&runtime, &[("obj", b"x")]);
        let file = make_file(runtime, store, "obj");
        file.populate().unwrap();
        file.clear_ram_cache().unwrap();
    }

    #[test]
    fn kind_is_s3() {
        assert_eq!(<S3File as UniversalRead>::kind(), UniversalKind::S3);
    }

    #[test]
    fn open_without_context_errs() {
        let result = S3File::open("k", OpenOptions::default());
        assert!(matches!(
            result.unwrap_err(),
            UniversalIoError::S3Config { .. }
        ));
    }

    #[test]
    fn list_files_without_context_errs() {
        let res = <S3File as UniversalReadFileOps>::list_files(Path::new("prefix"));
        assert!(matches!(
            res.unwrap_err(),
            UniversalIoError::S3Config { .. }
        ));
    }

    #[test]
    fn read_batch_returns_all_pairs() {
        let runtime = S3RuntimeHandle::global();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        runtime.block_on(async {
            store
                .put(&object_store::path::Path::from("merged"), Bytes::from_static(b"helloWORLDxyz").into())
                .await
                .unwrap();
        });
        let meta = runtime.block_on(async {
            store.head(&object_store::path::Path::from("merged")).await
        }).unwrap();
        let file = S3File {
            store,
            key: object_store::path::Path::from("merged"),
            len_bytes: meta.size,
            runtime: runtime.clone(),
        };

        let inputs = vec![
            (1u32, ReadRange::new(0, 5)),   // "hello"
            (2u32, ReadRange::new(5, 5)),   // "WORLD"
            (3u32, ReadRange::new(10, 3)),  // "xyz"
        ];
        let mut got: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        file.read_batch::<common::generic_consts::Random, u8, _>(
            inputs,
            |user_data, slice| {
                got.insert(user_data, slice.to_vec());
                Ok(())
            },
        )
        .expect("read_batch");

        assert_eq!(got[&1], b"hello");
        assert_eq!(got[&2], b"WORLD");
        assert_eq!(got[&3], b"xyz");
    }

    #[test]
    fn read_only_wrapper_compiles_with_s3file() {
        use common::universal_io::ReadOnly;
        fn assert_universal_read<R: common::universal_io::UniversalRead>() {}
        assert_universal_read::<ReadOnly<S3File>>();
    }

}
