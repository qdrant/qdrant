//! Backend-generic conformance suite for [`UniversalAppend`], exposed under
//! the `testing` feature so backend crates outside this one (e.g. the
//! object-store bridge) can run the same battery against their handles.

use std::path::Path;

use crate::generic_consts::Random;
use crate::mmap::AdviceSetting;
use crate::universal_io::{
    OpenOptions, Populate, ReadRange, UniversalAppend, UniversalFlush as _, UniversalIoError,
    UniversalRead as _, UniversalReadFs, UniversalWriteFileOps,
};

/// [`OpenOptions`] for conformance runs.
pub fn open_options(writeable: bool) -> OpenOptions {
    OpenOptions {
        writeable,
        need_sequential: false,
        populate: Populate::No,
        advice: AdviceSetting::Global,
    }
}

/// Exercise the [`UniversalAppend`] contract against a backend.
///
/// `dir` is a directory (or key prefix, for object stores) the suite may
/// create files under.
pub fn run_append_conformance<Fs>(fs: &Fs, dir: &Path)
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

    // Appending no bytes trivially succeeds, without touching the file.
    file.append::<u8>(0, &[]).unwrap();
    assert_eq!(file.len::<u8>().unwrap(), 0);

    // Appends land at exactly the provided offset.
    file.append(0, b"hello ".as_slice()).unwrap();
    file.append(6, b"world".as_slice()).unwrap();
    assert_eq!(file.len::<u8>().unwrap(), 11);

    // Reads through the same handle observe the appended bytes.
    assert_eq!(
        file.read_whole::<u8>().unwrap().as_ref(),
        b"hello world".as_slice(),
    );
    assert_eq!(
        file.read::<_, u8>(ReadRange::new(6, 5), Random)
            .unwrap()
            .as_ref(),
        b"world".as_slice(),
    );

    // A wrong offset is rejected outright and writes nothing — so a
    // duplicate of an already-landed append conflicts instead of appending
    // twice (idempotency).
    let err = file.append(6, b"world".as_slice()).unwrap_err();
    assert!(matches!(err, UniversalIoError::AppendOffsetConflict { .. }));
    let err = file.append(100, b"x".as_slice()).unwrap_err();
    assert!(matches!(err, UniversalIoError::AppendOffsetConflict { .. }));
    assert_eq!(file.len::<u8>().unwrap(), 11);
    assert_eq!(
        file.read_whole::<u8>().unwrap().as_ref(),
        b"hello world".as_slice(),
    );

    // A batch lands contiguously at the provided offset; empty buffers are
    // skipped.
    let batch: [&[u8]; 4] = [b"ab", b"", b"cde", b"f"];
    file.append_batch(11, batch).unwrap();
    assert_eq!(file.len::<u8>().unwrap(), 17);
    assert_eq!(
        file.read::<_, u8>(ReadRange::new(11, 6), Random)
            .unwrap()
            .as_ref(),
        b"abcdef".as_slice(),
    );

    // An empty batch trivially succeeds.
    file.append_batch::<u8>(17, std::iter::empty()).unwrap();

    // Batches larger than IOV_MAX (1024) still land contiguously and in
    // order across the multiple underlying operations.
    let buffers: Vec<Vec<u8>> = (0..1500u32).map(|i| i.to_le_bytes().to_vec()).collect();
    let expected: Vec<u8> = buffers.concat();
    file.append_batch(17, buffers.iter().map(Vec::as_slice))
        .unwrap();
    assert_eq!(
        file.read::<_, u8>(ReadRange::new(17, expected.len() as u64), Random)
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
    file.append(eof, b"tail".as_slice()).unwrap();
    reader.reopen().unwrap();
    assert_eq!(reader.len::<u8>().unwrap(), eof + 4);
    assert_eq!(
        reader
            .read::<_, u8>(ReadRange::new(eof, 4), Random)
            .unwrap()
            .as_ref(),
        b"tail".as_slice(),
    );

    // The durability hook runs cleanly after appends.
    (file.flusher())().unwrap();

    // Two writeable handles on the same file: the handle holding a stale
    // offset gets a clean conflict and writes nothing — the check runs
    // against the file, not the handle's possibly-stale view.
    let conflict_path = dir.join("append_conflict.dat");
    fs.create(&conflict_path, 0).unwrap();
    let mut first = fs
        .open(&conflict_path, open_options(true), Fs::OpenExtra::default())
        .unwrap();
    let mut second = fs
        .open(&conflict_path, open_options(true), Fs::OpenExtra::default())
        .unwrap();

    first.append(0, b"aaa".as_slice()).unwrap();
    second.append(3, b"bbb".as_slice()).unwrap();

    // `first` still believes the file ends at 3.
    let err = first.append(3, b"ccc".as_slice()).unwrap_err();
    assert!(matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 3, .. }
    ));

    // Recovery per the contract: reopen the stale handle, re-check the
    // length to learn the true end of file, and append there.
    first.reopen().unwrap();
    let eof = first.len::<u8>().unwrap();
    assert_eq!(eof, 6);
    first.append(eof, b"ccc".as_slice()).unwrap();
    assert_eq!(
        first.read_whole::<u8>().unwrap().as_ref(),
        b"aaabbbccc".as_slice(),
    );
}
