//! Backend-specific tests for [`UniversalAppend`]; the backend-generic
//! battery lives in [`conformance`].

use std::path::Path;

use super::conformance::{open_options, run_append_conformance};
use super::*;

/// Appending grows the underlying regular file itself, preserving existing
/// content — verified with plain fs reads, on both local backends.
#[test]
fn append_grows_regular_file() {
    fn check<Fs>(fs: &Fs, path: &Path)
    where
        Fs: UniversalWriteFs,
        Fs::File: UniversalAppend,
        Fs::OpenExtra: Default,
    {
        fs_err::write(path, b"existing ").unwrap();

        let mut file = fs
            .open(path, open_options(true), Fs::OpenExtra::default())
            .unwrap();
        file.append(9, b"appended".as_slice()).unwrap();

        assert_eq!(fs_err::read(path).unwrap(), b"existing appended".as_slice());
    }

    let dir = tempfile::tempdir().unwrap();
    check(&MmapFs, &dir.path().join("mmap.dat"));
    #[cfg(target_os = "linux")]
    check(
        &IoUringFs::from_context(Default::default()).unwrap(),
        &dir.path().join("uring.dat"),
    );
}

#[test]
fn mmap_append_conformance() {
    let dir = tempfile::tempdir().unwrap();
    run_append_conformance(&MmapFs, dir.path());
}

#[cfg(target_os = "linux")]
#[test]
fn io_uring_append_conformance() {
    let dir = tempfile::tempdir().unwrap();
    let fs = IoUringFs::from_context(Default::default()).unwrap();
    run_append_conformance(&fs, dir.path());
}

/// The durability flusher is reachable through `TypedStorage` for
/// append-only storages too (`S: UniversalFlush`, not just `UniversalWrite`).
#[test]
fn typed_storage_forwards_flusher_for_append_only_storages() {
    fn flusher_of<S: UniversalAppend>(storage: &TypedStorage<S, u8>) -> Flusher {
        storage.flusher()
    }
    // Compiling against the append-only bound is the assertion.
    let _ = flusher_of::<MmapFile>;
}

#[test]
fn mmap_append_requires_writeable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read_only.dat");
    MmapFs.create(&path, 0).unwrap();

    let mut file = MmapFs.open(&path, open_options(false), ()).unwrap();
    assert!(file.append(0, b"x".as_slice()).is_err());
}

/// Append is the only growth path: positioned writes beyond the end-of-file
/// keep failing with `OutOfBounds` after the file has grown via appends, on
/// both local backends.
#[test]
fn write_beyond_eof_still_errors() {
    fn check<Fs>(fs: &Fs, path: &Path)
    where
        Fs: UniversalWriteFs,
        Fs::File: UniversalAppend + UniversalWrite,
        Fs::OpenExtra: Default,
    {
        fs.create(path, 0).unwrap();
        let mut file = fs
            .open(path, open_options(true), Fs::OpenExtra::default())
            .unwrap();
        file.append(0, b"abc".as_slice()).unwrap();

        // Within bounds: fine.
        file.write(0, b"xyz".as_slice()).unwrap();
        // Straddling and past the end-of-file: rejected.
        let err = file.write(2, b"xy".as_slice()).unwrap_err();
        assert!(matches!(err, UniversalIoError::OutOfBounds { .. }));
        let err = file.write(3, b"x".as_slice()).unwrap_err();
        assert!(matches!(err, UniversalIoError::OutOfBounds { .. }));
        // Batched writes are bounds-checked too.
        let err = file.write_batch([(2u64, b"xy".as_slice())]).unwrap_err();
        assert!(matches!(err, UniversalIoError::OutOfBounds { .. }));
    }

    let dir = tempfile::tempdir().unwrap();
    check(&MmapFs, &dir.path().join("mmap.dat"));
    #[cfg(target_os = "linux")]
    check(
        &IoUringFs::from_context(Default::default()).unwrap(),
        &dir.path().join("uring.dat"),
    );
}

/// `O_DIRECT` handles have block-aligned I/O requirements that appends of
/// arbitrary sizes cannot satisfy.
#[cfg(target_os = "linux")]
#[test]
fn io_uring_append_rejects_direct_io() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("direct.dat");

    let fs = IoUringFs::from_context(Default::default()).unwrap();
    fs.create(&path, 0).unwrap();

    let extra = IoUringOpenExtra {
        prevent_caching: true,
    };
    // Some filesystems (e.g. tmpfs) reject `O_DIRECT` opens altogether;
    // nothing to test there.
    let Ok(mut file) = fs.open(&path, open_options(true), extra) else {
        return;
    };

    let err = file.append(0, b"x".as_slice()).unwrap_err();
    let UniversalIoError::Io(err) = err else {
        panic!("expected io error, got {err:?}");
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn mmap_select_files_async() {
    let dir = tempfile::tempdir().unwrap();
    let file1 = dir.path().join("a.dat");
    let file2 = dir.path().join("b.dat");
    let missing = dir.path().join("c.dat");

    fs_err::write(&file1, b"hello").unwrap();
    fs_err::write(&file2, b"world!").unwrap();

    let selected = MmapFs
        .select_files_async(&[file1.as_path(), file2.as_path(), missing.as_path()])
        .await
        .unwrap();

    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].path, file1);
    assert_eq!(selected[0].size, 5);
    assert_eq!(selected[1].path, file2);
    assert_eq!(selected[1].size, 6);

    let empty = MmapFs.select_files_async::<&Path>(&[]).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn cached_fs_select_files_async() {
    let dir = tempfile::tempdir().unwrap();
    let file1 = dir.path().join("f1.dat");
    let file2 = dir.path().join("f2.dat");
    let missing = dir.path().join("f3.dat");

    fs_err::write(&file1, b"123").unwrap();
    fs_err::write(&file2, b"4567").unwrap();

    let mut cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();

    // Before caching file info: uncached pass-through
    let selected = cached_fs
        .select_files_async(&[file1.clone(), file2.clone(), missing.clone()])
        .await
        .unwrap();
    assert_eq!(selected.len(), 2);

    // Populate file info via select_cache_file_info_async
    cached_fs
        .select_cache_file_info_async(&[file1.clone(), missing.clone()])
        .await
        .unwrap();
    assert!(cached_fs.file_info(&file1).is_some());
    assert!(cached_fs.file_info(&file2).is_none());

    // After caching file info: answered from cache
    let selected = cached_fs
        .select_files_async(&[file1.clone(), file2.clone()])
        .await
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].path, file1);
    assert_eq!(selected[0].size, 3);
}
