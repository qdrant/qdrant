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
        Fs: UniversalReadFs + UniversalWriteFileOps,
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
        Fs: UniversalReadFs + UniversalWriteFileOps,
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
