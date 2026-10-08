//! Read-only view over a chunked-vectors directory.
//!
//! Guarantees:
//!
//! - nothing in the directory is created, written or resized;
//! - the vector count and the set of open chunk handles are fixed at open time
//!   and only change on [`live_reload`](crate::common::live_reload::LiveReload::live_reload);
//! - a failed reload leaves the view serving its pre-reload state.
//!
//! The impls are split across sibling modules:
//!
//! - [`lifecycle`]: prefetch, open, file listing and cache control.
//! - [`read_ops`]: the read path — single and batched vector reads, plus the
//!   chunk indexing they rest on.
//! - [`live_reload`]: picking up vectors appended by an external writer.

mod lifecycle;
mod live_reload;
mod read_ops;

use std::path::PathBuf;

use common::mmap::AdviceSetting;
use common::universal_io::{Populate, TypedStorage, UniversalRead};

use super::config::ChunkedVectorsConfig;

/// Read-only view over a chunked-vectors directory.
///
/// Holds the indexing logic (chunks, config, vector count) and exposes only the
/// read-side API. Mutating storage uses [`ChunkedVectors`](super::ChunkedVectors)
/// which wraps this and adds a writable status mmap.
#[derive(Debug)]
pub struct ReadOnlyChunkedVectors<T: bytemuck::Pod + Send, S: UniversalRead> {
    pub(super) config: ChunkedVectorsConfig,
    /// Number of vectors currently stored. Snapshot for read-only mode; for
    /// [`ChunkedVectors`](super::ChunkedVectors) this is kept in sync with the
    /// writable status mmap.
    pub(super) len: usize,
    pub(super) chunks: Vec<TypedStorage<S, T>>,
    pub(super) directory: PathBuf,
    /// Open-time chunk settings, reused by live-reload to open new chunks.
    pub(super) advice: AdviceSetting,
    pub(super) populate: Populate,
}

#[cfg(test)]
mod tests {
    use common::ambient;
    use common::generic_consts::Random;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs};
    use tempfile::Builder;

    use super::super::chunks::chunk_name;
    use super::super::test_utils::{append_range, make_vec};
    use super::super::update_only::UpdateOnlyChunkedVectors;
    use super::*;
    use crate::common::live_reload::LiveReload;

    /// The offsets an id tracker delta publishes for `range`.
    fn published(range: std::ops::Range<PointOffsetType>) -> Vec<PointOffsetType> {
        range.collect()
    }

    /// A read-only view picks up writer-appended vectors after `live_reload`.
    #[test]
    fn live_reload_picks_up_appended_vectors() {
        const DIM: usize = 32;
        let dir = Builder::new().prefix("chunked_reload").tempdir().unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 100);

        // Append more through the writer, then reload the read-only view.
        append_range(&mut writer, 100, 100..250, DIM);

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(100..250);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&MmapFs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 250);
        let got = reader.get::<Random>(100).unwrap();
        assert_eq!(got.as_ref(), make_vec(100, DIM).as_slice());
    }

    /// Preload must stage every file the reload opens: after the preload the
    /// backing files are deleted, so the reload can only succeed from the
    /// prefetch pool (parked mmap handles keep reading deleted files on unix).
    #[cfg(unix)]
    #[test]
    fn live_preload_then_reload_sees_appended_vectors() {
        use common::universal_io::{CachedFs, CachedReadFs};

        const DIM: usize = 32;
        let dir = Builder::new().prefix("chunked_preload").tempdir().unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 100);

        append_range(&mut writer, 100, 100..250, DIM);
        drop(writer);

        let mut cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();
        cached_fs.cache_file_info().unwrap();
        LiveReload::live_preload(&reader, &cached_fs).unwrap();
        futures::executor::block_on(cached_fs.wait_all());

        for file in fs_err::read_dir(dir.path()).unwrap() {
            fs_err::remove_file(file.unwrap().path()).unwrap();
        }

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(100..250);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&cached_fs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 250);
        let got = reader.get::<Random>(100).unwrap();
        assert_eq!(got.as_ref(), make_vec(100, DIM).as_slice());
    }

    /// Growth starting exactly at a chunk boundary leaves the last held chunk
    /// untouched while the length changed: it is fully committed, so preload
    /// and reload skip it, keeping the current handle and adopting only the
    /// new chunk.
    #[test]
    fn live_preload_unchanged_last_chunk_keeps_handle() {
        use common::universal_io::{CachedFs, CachedReadFs};

        const DIM: usize = 32; // 4096 vectors per test chunk
        let dir = Builder::new().prefix("chunked_boundary").tempdir().unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..4096, DIM);

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 4096);

        let empty = SortedSlice::new(&[]).unwrap();
        let mut cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();

        // First cycle: no previous snapshot, staging parks fresh handles.
        cached_fs.cache_file_info().unwrap();
        LiveReload::live_preload(&reader, &cached_fs).unwrap();
        reader.live_reload(&cached_fs, &empty, &empty).unwrap();

        // Growth lands entirely in a new chunk; chunk 0 stays untouched.
        append_range(&mut writer, 4096, 4096..4196, DIM);

        // Second cycle: chunk 0 is rescheduled and unchanged -> sentinel.
        cached_fs.cache_file_info().unwrap();
        LiveReload::live_preload(&reader, &cached_fs).unwrap();
        let new = published(4096..4196);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&cached_fs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 4196);
        for offset in [0, 4095, 4096, 4195] {
            assert_eq!(
                reader.get::<Random>(offset).unwrap().as_ref(),
                make_vec(offset, DIM).as_slice(),
                "vector {offset} mismatch after reload",
            );
        }
    }

    /// Writer recovery (`ensure_chunk_lengths`) can remove uncommitted
    /// trailing chunk files: a reader that opened while such a file existed
    /// must drop its handle on reload instead of re-opening the deleted file,
    /// and refresh the chunk holding the watermark rather than the last held.
    #[cfg(unix)] // recovery deletes a chunk file the reader holds mapped
    #[test]
    fn live_reload_drops_chunks_removed_by_writer_recovery() {
        use common::universal_io::{CachedFs, CachedReadFs};

        const DIM: usize = 32;
        let dir = Builder::new().prefix("chunked_shrink").tempdir().unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        // Crash leftover: a chunk file past the committed watermark.
        fs_err::write(chunk_name(dir.path(), 1), vec![7u8; 128]).unwrap();

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 100);
        assert_eq!(reader.chunks.len(), 2, "leftover chunk is listed and held");

        // The next batch trusts the watermark: it removes the leftover chunk,
        // then lands in chunk 0.
        append_range(&mut writer, 100, 100..150, DIM);
        drop(writer);

        let mut cached_fs = CachedFs::new(MmapFs, dir.path()).unwrap();
        cached_fs.cache_file_info().unwrap();
        LiveReload::live_preload(&reader, &cached_fs).unwrap();

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(100..150);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&cached_fs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 150);
        assert_eq!(reader.chunks.len(), 1, "removed trailing chunk is dropped");
        for offset in [0, 99, 100, 149] {
            assert_eq!(
                reader.get::<Random>(offset).unwrap().as_ref(),
                make_vec(offset, DIM).as_slice(),
                "vector {offset} mismatch after reload",
            );
        }
    }

    /// Case-5 regression of the live-reload staleness audit: a reader over a
    /// caching backend that fetched a block straddling the old tail (any read
    /// near the tail pulls a 16KiB block covering space appended into later)
    /// would keep serving those stale bytes for vectors landing in that block —
    /// `live_reload` must re-open the last held chunk, not keep the handle.
    /// This drives it over `DiskCacheFs`, where the failure actually
    /// reproduces (mmap readers are read-through and can't catch it).
    #[test]
    fn live_reload_over_disk_cache_sees_in_place_appends() {
        use std::sync::Arc;

        use common::universal_io::{
            DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFs,
        };

        const DIM: usize = 32;
        let tmp = Builder::new().prefix("chunked_reload").tempdir().unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("vectors");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        let _hw = ambient::test_guard();

        // The writer works on the "remote" directly; the reader mirrors it
        // into `local_root` through the disk cache.
        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, &dir, DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();
        let mut reader = ReadOnlyChunkedVectors::<f32, DiskCache<MmapFile>>::open(
            &cache_fs,
            &dir,
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 100);

        // Read the tail vector: the fetched block ends at the old tail —
        // the stale bytes this test must escape are now in the reader's
        // local cache.
        let got = reader.get::<Random>(99).unwrap();
        assert_eq!(got.as_ref(), make_vec(99, DIM).as_slice());

        // Append into that same block region, then reload.
        append_range(&mut writer, 100, 100..150, DIM);

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(100..150);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&cache_fs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 150);
        for offset in [0, 99, 100, 149] {
            assert_eq!(
                reader.get::<Random>(offset).unwrap().as_ref(),
                make_vec(offset, DIM).as_slice(),
                "vector {offset} mismatch after reload",
            );
        }
    }

    type CachedDiskCacheFs =
        common::universal_io::CachedFs<common::universal_io::DiskCacheFs<MmapFile>>;
    type DiskCacheReader = ReadOnlyChunkedVectors<f32, common::universal_io::DiskCache<MmapFile>>;

    /// A writer on a "remote" directory and a reader mirroring it through a
    /// disk cache behind `CachedFs`. The reader opens at 100 vectors, the
    /// listing snapshot is taken at 150, and the writer is at 200 by the time
    /// the reload runs: the status file reports 200, the listed chunk 150.
    fn status_newer_than_snapshot(
        dim: usize,
    ) -> (
        tempfile::TempDir,
        UpdateOnlyChunkedVectors<f32>,
        CachedDiskCacheFs,
        DiskCacheReader,
    ) {
        use std::sync::Arc;

        use common::universal_io::{
            CachedFs, CachedReadFs, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext,
            UniversalReadFs,
        };

        let tmp = Builder::new().prefix("chunked_race").tempdir().unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("vectors");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, &dir, dim).unwrap();
        append_range(&mut writer, 0, 0..100, dim);

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();

        let mut cached_fs = CachedFs::new(cache_fs, &dir).unwrap();
        cached_fs.cache_file_info().unwrap();

        let reader = ReadOnlyChunkedVectors::open(
            &cached_fs,
            &dir,
            dim,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 100);

        append_range(&mut writer, 100, 100..150, dim);
        cached_fs.rotate_cache_file_info();
        cached_fs.cache_file_info().unwrap();
        append_range(&mut writer, 150, 150..200, dim);

        (tmp, writer, cached_fs, reader)
    }

    fn assert_vectors(reader: &DiskCacheReader, offsets: &[usize], dim: usize) {
        for &offset in offsets {
            assert_eq!(
                reader
                    .get::<Random>(offset)
                    .unwrap_or_else(|| panic!("vector should exist at offset {offset}"))
                    .as_ref(),
                make_vec(offset, dim).as_slice(),
                "vector {offset} mismatch after reload",
            );
        }
    }

    /// The slot-indexed reload sizes from the id tracker delta, which is capped
    /// at the same listing snapshot as the chunks, so a status file served
    /// newer than that snapshot cannot make the view claim vectors its chunk
    /// handles do not hold.
    #[test]
    fn live_reload_sizes_from_published_points_not_newer_status() {
        use common::universal_io::CachedReadFs;

        const DIM: usize = 32;
        let _hw = ambient::test_guard();
        let (_tmp, _writer, mut cached_fs, mut reader) = status_newer_than_snapshot(DIM);
        let empty = SortedSlice::new(&[]).unwrap();

        // The snapshot publishes up to 150.
        let futs = LiveReload::live_preload(&reader, &cached_fs).unwrap();
        futures::executor::block_on(futures::future::join(
            futures::future::join_all(futs),
            cached_fs.wait_all(),
        ));
        let new = published(100..150);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&cached_fs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 150);
        assert_vectors(&reader, &[0, 99, 100, 149], DIM);

        // The next snapshot sees the rest.
        cached_fs.rotate_cache_file_info();
        cached_fs.cache_file_info().unwrap();
        let futs = LiveReload::live_preload(&reader, &cached_fs).unwrap();
        futures::executor::block_on(futures::future::join(
            futures::future::join_all(futs),
            cached_fs.wait_all(),
        ));
        let new = published(150..200);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&cached_fs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 200);
        assert_vectors(&reader, &[149, 150, 199], DIM);
    }

    /// `open` caps the status file at the chunk lengths too.
    #[test]
    fn open_caps_newer_status_at_chunk_capacity() {
        use std::sync::Arc;

        use common::universal_io::{
            CachedFs, CachedReadFs, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext,
            UniversalReadFs,
        };

        const DIM: usize = 32;
        let tmp = Builder::new()
            .prefix("chunked_open_race")
            .tempdir()
            .unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("vectors");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, &dir, DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();
        let mut cached_fs = CachedFs::new(cache_fs, &dir).unwrap();
        cached_fs.cache_file_info().unwrap();

        append_range(&mut writer, 100, 100..150, DIM);

        let reader =
            DiskCacheReader::open(&cached_fs, &dir, DIM, AdviceSetting::Global, Populate::No)
                .unwrap();

        assert_eq!(reader.len(), 100);
        assert_vectors(&reader, &[0, 99], DIM);
    }

    /// A published offset the chunks do not hold means the writer broke its
    /// ordering: the reload fails and the view keeps its previous state.
    #[test]
    fn live_reload_fails_when_published_points_exceed_chunks() {
        const DIM: usize = 32;
        let dir = Builder::new()
            .prefix("chunked_reload_short")
            .tempdir()
            .unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(100..150);
        let new = SortedSlice::new(&new).unwrap();
        assert!(reader.live_reload(&MmapFs, &empty, &new).is_err());

        assert_eq!(reader.len(), 100);
        assert_eq!(
            reader.get::<Random>(99).as_deref(),
            Some(make_vec(99, DIM).as_slice()),
        );
    }

    /// `live_reload` re-opens the last held chunk (the only one that can have
    /// gained vectors) and adopts chunk files created since the last load;
    /// fully-loaded earlier chunks are kept as-is.
    #[test]
    fn live_reload_adopts_only_new_chunks() {
        const DIM: usize = 32; // 4096 vectors per test chunk
        let dir = Builder::new()
            .prefix("chunked_reload_grow")
            .tempdir()
            .unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..4000, DIM);

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 4000);
        assert_eq!(reader.chunks.len(), 1);

        // Straddles two chunk boundaries: fills chunk 0, spans 1, starts 2.
        append_range(&mut writer, 4000, 4000..9000, DIM);

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(4000..9000);
        let new = SortedSlice::new(&new).unwrap();
        reader.live_reload(&MmapFs, &empty, &new).unwrap();

        assert_eq!(reader.len(), 9000);
        assert_eq!(reader.chunks.len(), 3, "two new chunk files adopted");

        // 4050 was appended into the already-open first chunk; 5000/8999 are new chunks.
        for offset in [3999, 4050, 5000, 8999] {
            assert_eq!(
                reader.get::<Random>(offset).unwrap().as_ref(),
                make_vec(offset, DIM).as_slice(),
            );
        }
    }

    /// A `live_reload` that fails mid-way (transient I/O error re-opening the
    /// last chunk) must leave the reader serving its pre-refresh state: the
    /// old chunk handle stays live, so no previously-readable vector vanishes.
    #[cfg(unix)]
    #[test]
    fn failed_live_reload_keeps_serving_pre_refresh_state() {
        use std::os::unix::fs::PermissionsExt;

        const DIM: usize = 32;
        let dir = Builder::new()
            .prefix("chunked_reload_err")
            .tempdir()
            .unwrap();
        let _hw = ambient::test_guard();

        let mut writer = UpdateOnlyChunkedVectors::<f32>::open(&MmapFs, dir.path(), DIM).unwrap();
        append_range(&mut writer, 0, 0..100, DIM);

        let mut reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();
        assert_eq!(reader.len(), 100);
        assert_eq!(
            reader.get::<Random>(99).unwrap().as_ref(),
            make_vec(99, DIM).as_slice(),
        );

        // Grow within the same chunk so the reload takes the slow path.
        append_range(&mut writer, 100, 100..150, DIM);

        // Inject a transient error: chunk 0 still exists but cannot be opened.
        let chunk_file = chunk_name(dir.path(), 0);
        fs_err::set_permissions(&chunk_file, std::fs::Permissions::from_mode(0o000)).unwrap();

        let empty = SortedSlice::new(&[]).unwrap();
        let new = published(100..150);
        let new = SortedSlice::new(&new).unwrap();
        let reloaded = reader.live_reload(&MmapFs, &empty, &new);

        // Restore before asserting, so a failure leaves the tempdir removable.
        fs_err::set_permissions(&chunk_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            reloaded.is_err(),
            "reload must fail while chunk is unreadable"
        );

        // The failed reload must not have torn the pre-refresh state.
        assert_eq!(reader.len(), 100);
        assert_eq!(
            reader.get::<Random>(99).as_deref(),
            Some(make_vec(99, DIM).as_slice()),
            "vector 99 must survive a failed reload",
        );
    }
}
