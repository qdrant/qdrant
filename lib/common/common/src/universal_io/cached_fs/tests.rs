use std::path::PathBuf;

use fs_err as fs;

use crate::universal_io::conformance::open_options;
use crate::universal_io::{
    CachedFs, CachedReadFs, MmapFs, OkUnchanged, UniversalIoError, UniversalRead, UniversalReadFs,
};

fn fixture(content: &[u8]) -> (tempfile::TempDir, PathBuf, CachedFs<MmapFs>) {
    let dir = tempfile::Builder::new()
        .prefix("cached_fs")
        .tempdir()
        .unwrap();
    let path = dir.path().join("a.dat");
    fs::write(&path, content).unwrap();
    let cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();
    (dir, path, cached_fs)
}

/// Stamp an etag onto the current snapshot's entries. Local listings carry
/// no etag, and `full_eq` treats a missing one as changed — unchanged
/// detection needs one on both snapshots.
fn stamp_etag(cached_fs: &mut CachedFs<MmapFs>, etag: &str) {
    for info in cached_fs.files_info.as_mut().unwrap().values_mut() {
        info.etag = Some(etag.to_string());
    }
}

/// A rescheduled prefetch whose file info did not change between two
/// snapshots parks the unchanged sentinel: the consuming open reports
/// `UnchangedOpen`, which `ok_unchanged` maps to keep-current-handle.
#[test]
fn reschedule_prefetch_unchanged_parks_sentinel() {
    let (_dir, path, mut cached_fs) = fixture(b"hello");

    // First cycle: no previous snapshot -> a fresh handle is parked.
    cached_fs.cache_file_info().unwrap();
    stamp_etag(&mut cached_fs, "v1");
    cached_fs
        .reschedule_prefetch(&path, Some(open_options(false)), None)
        .unwrap();
    cached_fs.open(&path, open_options(false), ()).unwrap();

    // Second cycle without writes: identical file info -> sentinel.
    cached_fs.cache_file_info().unwrap();
    stamp_etag(&mut cached_fs, "v1");
    cached_fs
        .reschedule_prefetch(&path, Some(open_options(false)), None)
        .unwrap();
    assert!(matches!(
        cached_fs.open(&path, open_options(false), ()),
        Err(UniversalIoError::UnchangedOpen { .. }),
    ));

    // Re-park and consume through the helper: keep-current-handle.
    cached_fs
        .reschedule_prefetch(&path, Some(open_options(false)), None)
        .unwrap();
    let kept = cached_fs
        .open(&path, open_options(false), ())
        .ok_unchanged()
        .unwrap();
    assert!(kept.is_none());
}

/// A rescheduled prefetch over a changed file parks a fresh handle serving
/// the new bytes.
#[test]
fn reschedule_prefetch_changed_parks_fresh_handle() {
    let (_dir, path, mut cached_fs) = fixture(b"hello");

    cached_fs.cache_file_info().unwrap();
    stamp_etag(&mut cached_fs, "v1");
    cached_fs
        .reschedule_prefetch(&path, Some(open_options(false)), None)
        .unwrap();
    cached_fs.open(&path, open_options(false), ()).unwrap();

    fs::write(&path, b"hello, world").unwrap();

    cached_fs.cache_file_info().unwrap();
    stamp_etag(&mut cached_fs, "v2");
    cached_fs
        .reschedule_prefetch(&path, Some(open_options(false)), None)
        .unwrap();
    let fresh = cached_fs
        .open(&path, open_options(false), ())
        .ok_unchanged()
        .unwrap()
        .expect("changed file must open a fresh handle");
    assert_eq!(fresh.len::<u8>().unwrap(), b"hello, world".len() as u64);
}

/// Once a snapshot exists, opens of unlisted paths fail locally with
/// `NotFound` — even if the file appeared on disk after the snapshot.
#[test]
fn snapshot_miss_fails_not_found_locally() {
    let (dir, _path, mut cached_fs) = fixture(b"hello");

    cached_fs.cache_file_info().unwrap();

    let late = dir.path().join("late.dat");
    fs::write(&late, b"created after the snapshot").unwrap();

    assert!(matches!(
        cached_fs.open(&late, open_options(false), ()),
        Err(UniversalIoError::NotFound { .. }),
    ));
}

/// Taking a new snapshot drops unconsumed prefetches: deleting the file
/// afterwards makes the fallback open fail, proving the parked handle from
/// before the snapshot is gone.
#[test]
fn new_snapshot_clears_unconsumed_prefetches() {
    let (_dir, path, mut cached_fs) = fixture(b"hello");

    cached_fs.cache_file_info().unwrap();
    cached_fs
        .schedule_prefetch(&path, Some(open_options(false)), None)
        .unwrap();

    cached_fs.cache_file_info().unwrap();
    fs::remove_file(&path).unwrap();

    assert!(
        cached_fs.open(&path, open_options(false), ()).is_err(),
        "parked handle must not survive a new snapshot",
    );
}
