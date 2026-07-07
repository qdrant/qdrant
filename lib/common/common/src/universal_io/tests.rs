//! Backend-generic conformance tests for [`UniversalAppend`].

use std::path::Path;

use super::*;
use crate::generic_consts::Random;
use crate::mmap::AdviceSetting;

fn open_options(writeable: bool) -> OpenOptions {
    OpenOptions {
        writeable,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    }
}

/// Exercise the [`UniversalAppend`] contract against a backend.
fn run_append_conformance<Fs>(fs: &Fs, dir: &Path)
where
    Fs: UniversalReadFs + UniversalWriteFileOps,
    Fs::File: UniversalAppend,
    Fs::OpenExtra: Default,
{
    let path = dir.join("append.dat");
    fs.create(&path, 0).unwrap();

    let mut file = fs
        .open(&path, open_options(true), Fs::OpenExtra::default())
        .unwrap();

    // Empty append on an empty file is a no-op returning the EOF offset.
    assert_eq!(file.append::<u8>(&[]).unwrap(), 0);
    assert_eq!(file.len::<u8>().unwrap(), 0);

    // Sequential appends return the offset at which each buffer landed.
    assert_eq!(file.append(b"hello ".as_slice()).unwrap(), 0);
    assert_eq!(file.append(b"world".as_slice()).unwrap(), 6);
    assert_eq!(file.len::<u8>().unwrap(), 11);

    // Reads through the same handle observe the appended bytes.
    assert_eq!(
        file.read_whole::<u8>().unwrap().as_ref(),
        b"hello world".as_slice(),
    );
    assert_eq!(
        file.read::<Random, u8>(ReadRange::new(6, 5))
            .unwrap()
            .as_ref(),
        b"world".as_slice(),
    );

    // A batch lands contiguously, empty buffers are skipped, and the
    // returned offset is that of the first appended byte.
    let batch: [&[u8]; 4] = [b"ab", b"", b"cde", b"f"];
    assert_eq!(file.append_batch(batch).unwrap(), 11);
    assert_eq!(file.len::<u8>().unwrap(), 17);
    assert_eq!(
        file.read::<Random, u8>(ReadRange::new(11, 6))
            .unwrap()
            .as_ref(),
        b"abcdef".as_slice(),
    );

    // An empty batch is a no-op returning the EOF offset.
    assert_eq!(file.append_batch::<u8>(std::iter::empty()).unwrap(), 17);

    // Batches larger than IOV_MAX (1024) still land contiguously and in
    // order across the multiple underlying operations.
    let buffers: Vec<Vec<u8>> = (0..1500u32).map(|i| i.to_le_bytes().to_vec()).collect();
    let expected: Vec<u8> = buffers.concat();
    assert_eq!(
        file.append_batch(buffers.iter().map(Vec::as_slice))
            .unwrap(),
        17,
    );
    assert_eq!(
        file.read::<Random, u8>(ReadRange::new(17, expected.len() as u64))
            .unwrap()
            .as_ref(),
        expected.as_slice(),
    );
    let eof = 17 + expected.len() as u64;

    // A freshly opened handle sees the current size; afterwards it observes
    // further growth after `reopen()`.
    let mut reader = fs
        .open(&path, open_options(false), Fs::OpenExtra::default())
        .unwrap();
    assert_eq!(reader.len::<u8>().unwrap(), eof);
    assert_eq!(file.append(b"tail".as_slice()).unwrap(), eof);
    reader.reopen().unwrap();
    assert_eq!(reader.len::<u8>().unwrap(), eof + 4);
    assert_eq!(
        reader
            .read::<Random, u8>(ReadRange::new(eof, 4))
            .unwrap()
            .as_ref(),
        b"tail".as_slice(),
    );

    // The durability hook runs cleanly after appends.
    (file.flusher())().unwrap();
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

#[test]
fn mmap_append_requires_writeable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read_only.dat");
    MmapFs.create(&path, 0).unwrap();

    let mut file = MmapFs.open(&path, open_options(false), ()).unwrap();
    assert!(file.append(b"x".as_slice()).is_err());
}

/// Append is the only growth path: positioned writes beyond the end-of-file
/// keep failing with `OutOfBounds` after the file has grown via appends.
#[test]
fn mmap_write_beyond_eof_still_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("write_oob.dat");
    MmapFs.create(&path, 0).unwrap();

    let mut file = MmapFs.open(&path, open_options(true), ()).unwrap();
    assert_eq!(file.append(b"abc".as_slice()).unwrap(), 0);

    // Within bounds: fine.
    file.write(0, b"xyz".as_slice()).unwrap();
    // Straddling and past the end-of-file: rejected.
    let err = file.write(2, b"xy".as_slice()).unwrap_err();
    assert!(matches!(err, UniversalIoError::OutOfBounds { .. }));
    let err = file.write(3, b"x".as_slice()).unwrap_err();
    assert!(matches!(err, UniversalIoError::OutOfBounds { .. }));
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

    let err = file.append(b"x".as_slice()).unwrap_err();
    let UniversalIoError::Io(err) = err else {
        panic!("expected io error, got {err:?}");
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
