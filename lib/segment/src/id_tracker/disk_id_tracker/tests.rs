use common::types::DeferredBehavior;
use common::universal_io::{MmapFile, MmapFs};
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use tempfile::Builder;

use super::{DiskIdTracker, ReadOnlyDiskIdTracker};
use crate::id_tracker::compressed::compressed_point_mappings::CompressedPointMappings;
use crate::id_tracker::immutable_id_tracker::ImmutableIdTracker;
use crate::id_tracker::in_memory_id_tracker::InMemoryIdTracker;
use crate::id_tracker::read_only_tracker_enum::ReadOnlyIdTrackerEnum;
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

/// Random data source shared by both trackers so parity can be asserted.
fn make_data(seed: u64) -> (Vec<SeqNumberType>, CompressedPointMappings) {
    let mut rng = StdRng::seed_from_u64(seed);
    let in_memory = InMemoryIdTracker::random(&mut rng, 5_000, 4_200, 32);
    let (versions, mappings) = in_memory.into_internal();
    (versions, CompressedPointMappings::from_mappings(mappings))
}

fn build_immutable(
    versions: &[SeqNumberType],
    mappings: CompressedPointMappings,
) -> ImmutableIdTracker<MmapFile> {
    let dir = Builder::new().prefix("imm").tempdir().unwrap();
    let tracker = ImmutableIdTracker::new(&MmapFs, dir.path(), versions, mappings).unwrap();
    // Keep the dir alive for the tracker's lifetime by leaking it (test-only).
    std::mem::forget(dir);
    tracker
}

/// Assert every read-path method agrees between two trackers built from the
/// same data.
fn assert_read_parity<A: IdTrackerRead, B: IdTrackerRead>(reference: &A, candidate: &B) {
    assert_eq!(reference.total_point_count(), candidate.total_point_count());
    assert_eq!(
        reference.deleted_point_count(),
        candidate.deleted_point_count()
    );
    assert_eq!(
        reference.available_point_count(),
        candidate.available_point_count()
    );

    let reference_iter: Vec<_> = reference.point_mappings().iter_from(None).collect();
    let candidate_iter: Vec<_> = candidate.point_mappings().iter_from(None).collect();
    assert_eq!(reference_iter, candidate_iter, "iter_from(None) mismatch");

    for (external_id, offset) in &reference_iter {
        assert_eq!(
            candidate.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            Some(*offset),
        );
        assert_eq!(candidate.external_id(*offset), Some(*external_id));
    }

    // Cover every offset, including build-deleted ones.
    for offset in 0..reference.total_point_count() as u32 {
        assert_eq!(
            reference.external_id(offset),
            candidate.external_id(offset),
            "external_id mismatch at {offset}",
        );
        assert_eq!(
            reference.is_deleted_point(offset),
            candidate.is_deleted_point(offset),
            "is_deleted mismatch at {offset}",
        );
        assert_eq!(
            reference.internal_version(offset),
            candidate.internal_version(offset),
            "version mismatch at {offset}",
        );
    }
}

#[test]
fn disk_matches_immutable() {
    let (versions, mappings) = make_data(1);
    let immutable = build_immutable(&versions, mappings.clone());

    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    assert_read_parity(&immutable, &disk);
}

#[test]
fn read_only_matches_immutable() {
    let (versions, mappings) = make_data(2);
    let immutable = build_immutable(&versions, mappings.clone());

    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    // Writing the files also validates the on-disk format round-trips.
    let _disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    let read_only = ReadOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    assert_read_parity(&immutable, &read_only);
}

#[test]
fn iter_from_boundaries() {
    let (versions, mappings) = make_data(3);
    let immutable = build_immutable(&versions, mappings.clone());

    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    let starts = [
        None,
        Some(PointIdType::NumId(0)),
        Some(PointIdType::NumId(u64::MAX)),
        Some(PointIdType::Uuid(uuid::Uuid::from_u128(0))),
        Some(PointIdType::Uuid(uuid::Uuid::from_u128(u128::MAX))),
    ];
    for start in starts {
        let expected: Vec<_> = immutable.point_mappings().iter_from(start).collect();
        let actual: Vec<_> = disk.point_mappings().iter_from(start).collect();
        assert_eq!(expected, actual, "iter_from({start:?}) mismatch");
    }
}

#[test]
fn detect_and_load_selects_disk_format() {
    let (versions, mappings) = make_data(6);
    let immutable = build_immutable(&versions, mappings.clone());

    // A disk-format segment.
    let disk_dir = Builder::new().prefix("disk").tempdir().unwrap();
    let _disk =
        DiskIdTracker::<MmapFile>::new(&MmapFs, disk_dir.path(), &versions, mappings).unwrap();
    let loaded =
        ReadOnlyIdTrackerEnum::<MmapFile>::detect_and_load(&MmapFs, &MmapFs, disk_dir.path(), None)
            .unwrap();
    assert_eq!(loaded.name(), "read-only disk id tracker");
    assert_read_parity(&immutable, &loaded);

    // An immutable-format segment loads as the immutable reader.
    let (versions2, mappings2) = make_data(7);
    let imm_dir = Builder::new().prefix("imm").tempdir().unwrap();
    let _imm = ImmutableIdTracker::<MmapFile>::new(&MmapFs, imm_dir.path(), &versions2, mappings2)
        .unwrap();
    let loaded =
        ReadOnlyIdTrackerEnum::<MmapFile>::detect_and_load(&MmapFs, &MmapFs, imm_dir.path(), None)
            .unwrap();
    assert_eq!(loaded.name(), "read-only immutable id tracker");

    // An empty segment (no mapping files) falls back to the appendable reader.
    let empty_dir = Builder::new().prefix("empty").tempdir().unwrap();
    let loaded = ReadOnlyIdTrackerEnum::<MmapFile>::detect_and_load(
        &MmapFs,
        &MmapFs,
        empty_dir.path(),
        None,
    )
    .unwrap();
    assert_eq!(loaded.name(), "read-only appendable id tracker");
}

#[test]
fn iter_random_yields_all_live_points() {
    use std::collections::HashSet;

    let (versions, mappings) = make_data(9);
    let immutable = build_immutable(&versions, mappings.clone());
    let expected: HashSet<(PointIdType, u32)> =
        immutable.point_mappings().iter_from(None).collect();

    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let disk = DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    // A random-order full drain must cover exactly the live set, once each.
    let random: Vec<(PointIdType, u32)> = disk.point_mappings().iter_random_visible().collect();
    let random_set: HashSet<(PointIdType, u32)> = random.iter().copied().collect();
    assert_eq!(
        random.len(),
        random_set.len(),
        "iter_random yielded duplicates"
    );
    assert_eq!(random_set, expected, "iter_random must cover the live set");
    // It should genuinely be reordered, not the sorted iter_from sequence.
    let ordered: Vec<(PointIdType, u32)> = disk.point_mappings().iter_from(None).collect();
    assert_ne!(random, ordered, "iter_random should not be in sorted order");
}

#[test]
fn read_by_id_does_not_materialize_deleted_set() {
    let (versions, mappings) = make_data(4);
    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let live: Vec<_> = {
        let disk =
            DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();
        disk.point_mappings().iter_from(None).collect()
    };

    let read_only = ReadOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();

    // Point lookups must not trigger the full deleted-set materialization.
    for (external_id, offset) in live.iter().take(200) {
        assert_eq!(
            read_only.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            Some(*offset),
        );
        assert_eq!(read_only.external_id(*offset), Some(*external_id));
        let _ = read_only.internal_version(*offset);
        let _ = read_only.is_deleted_point(*offset);
    }
    assert!(
        !read_only.deleted_full_materialized(),
        "read-by-id lookups must not materialize the full deleted set",
    );

    // A search-style call (whole-slice access) does materialize it.
    let _ = read_only.deleted_point_bitslice();
    assert!(read_only.deleted_full_materialized());
}

#[test]
fn deletion_and_live_reload() {
    let (versions, mappings) = make_data(5);
    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let mut disk =
        DiskIdTracker::<MmapFile>::new(&MmapFs, dir.path(), &versions, mappings).unwrap();

    // A reader opened before the deletions; it will pick them up via live_reload.
    let mut read_only = ReadOnlyDiskIdTracker::<MmapFile>::open(&MmapFs, dir.path()).unwrap();
    // Establish the diff baseline (a search-style access) so the next reload
    // reports only the incremental deletions, not every build-time deletion.
    let _ = read_only.deleted_point_bitslice();

    let to_delete: Vec<(PointIdType, u32)> =
        disk.point_mappings().iter_from(None).take(50).collect();

    for (external_id, _) in &to_delete {
        disk.drop(*external_id).unwrap();
    }
    // Writable tracker: deletions are hidden immediately.
    for (external_id, offset) in &to_delete {
        assert_eq!(
            disk.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            None,
        );
        assert!(disk.is_deleted_point(*offset));
        assert_eq!(disk.external_id(*offset), None);
    }
    // Persist deletions so the reader can observe them.
    disk.mapping_flusher()().unwrap();
    disk.versions_flusher()().unwrap();

    let result = read_only.live_reload(&MmapFs).unwrap();
    let mut reported = result.deleted.clone();
    reported.sort_unstable();
    let mut expected: Vec<u32> = to_delete.iter().map(|(_, offset)| *offset).collect();
    expected.sort_unstable();
    assert_eq!(reported, expected, "live_reload delta mismatch");
    assert!(result.inserted.is_empty());

    // After reload, the reader hides the deleted points on every path.
    for (external_id, offset) in &to_delete {
        assert_eq!(
            read_only.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            None,
        );
        assert!(read_only.is_deleted_point(*offset));
        assert_eq!(read_only.external_id(*offset), None);
    }
}

/// Cases 1+2 regression of the live-reload staleness audit: both trackers'
/// `deleted` file is a fixed-size bitmap whose bits the writer flips in
/// place — its length never changes — so a reader over a caching backend
/// must not rely on `reopen()` (append-only-growth contract): the
/// pre-deletion state, cached when first scanned, would be served forever
/// and `live_reload` would never report the deletions. `live_reload` opens a
/// fresh handle instead; this drives it over `DiskCacheFs`, where the
/// stale-cache failure actually reproduces (mmap readers are read-through
/// and can't catch it).
#[test]
fn deletion_and_live_reload_disk_cache() {
    use std::sync::Arc;

    use common::universal_io::{
        DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
    };

    use crate::id_tracker::immutable_id_tracker::read_only::ReadOnlyImmutableIdTracker;

    let dir = Builder::new().prefix("disk").tempdir().unwrap();
    let remote_root = dir.path().join("remote");
    let local_root = dir.path().join("local");
    let immutable_path = remote_root.join("immutable_tracker");
    let disk_path = remote_root.join("disk_tracker");
    fs_err::create_dir_all(&immutable_path).unwrap();
    fs_err::create_dir_all(&disk_path).unwrap();
    fs_err::create_dir_all(&local_root).unwrap();

    // The writers work on the "remote" directly; the readers mirror it into
    // `local_root` through the disk cache.
    let (versions, mappings) = make_data(6);
    let mut immutable =
        ImmutableIdTracker::<MmapFile>::new(&MmapFs, &immutable_path, &versions, mappings.clone())
            .unwrap();
    let mut disk =
        DiskIdTracker::<MmapFile>::new(&MmapFs, &disk_path, &versions, mappings).unwrap();

    let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
        config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
        remote: Default::default(),
    })
    .unwrap();
    // The immutable tracker's `open` reads the whole pre-deletion deleted
    // bitmap; the disk tracker caches it on the baseline materialization
    // below — either way, the state this test must escape ends up in the
    // readers' local caches.
    let mut read_only_immutable =
        ReadOnlyImmutableIdTracker::<DiskCache<MmapFile>>::open(&cache_fs, &immutable_path)
            .unwrap();
    let mut read_only_disk =
        ReadOnlyDiskIdTracker::<DiskCache<MmapFile>>::open(&cache_fs, &disk_path).unwrap();
    // Establish the diff baseline (a search-style access) so the reload
    // reports only the incremental deletions, not every build-time deletion.
    let _ = read_only_disk.deleted_point_bitslice();

    let to_delete: Vec<(PointIdType, u32)> = immutable
        .point_mappings()
        .iter_from(None)
        .take(50)
        .collect();
    for (external_id, _) in &to_delete {
        immutable.drop(*external_id).unwrap();
        disk.drop(*external_id).unwrap();
    }
    immutable.mapping_flusher()().unwrap();
    immutable.versions_flusher()().unwrap();
    disk.mapping_flusher()().unwrap();
    disk.versions_flusher()().unwrap();

    let mut expected: Vec<u32> = to_delete.iter().map(|(_, offset)| *offset).collect();
    expected.sort_unstable();

    for result in [
        read_only_immutable.live_reload(&cache_fs).unwrap(),
        read_only_disk.live_reload(&cache_fs).unwrap(),
    ] {
        assert_eq!(result.deleted, expected, "live_reload delta mismatch");
        assert!(result.inserted.is_empty());
    }

    // After reload, both readers hide the deleted points on every path.
    for (external_id, offset) in &to_delete {
        assert_eq!(
            read_only_immutable
                .internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            None,
        );
        assert!(read_only_immutable.is_deleted_point(*offset));
        assert_eq!(read_only_immutable.external_id(*offset), None);

        assert_eq!(
            read_only_disk.internal_id_with_behavior(*external_id, DeferredBehavior::VisibleOnly),
            None,
        );
        assert!(read_only_disk.is_deleted_point(*offset));
        assert_eq!(read_only_disk.external_id(*offset), None);
    }
}

/// The on-disk layout must keep headers and every section start aligned to
/// `SECTION_ALIGN`, so the files stay mmap+transmute-friendly (`u128` requires
/// 16-byte alignment). Also pins the store/parse padding agreement: parsed
/// offsets must land exactly at the section ends implied by the written bytes.
#[test]
fn on_disk_sections_are_aligned() {
    use super::on_disk_format::{
        E2I_HEADER_SIZE, E2iHeader, I2E_HEADER_SIZE, I2eHeader, NUM_ENTRY_SIZE, SECTION_ALIGN,
        UUID_ENTRY_SIZE, store_e2i, store_i2e,
    };

    assert_eq!(I2E_HEADER_SIZE % SECTION_ALIGN, 0);
    assert_eq!(E2I_HEADER_SIZE % SECTION_ALIGN, 0);

    // Several seeds so both runs hit block-count/entry-count parities that
    // require actual padding bytes.
    for seed in [1, 2, 3] {
        let (_versions, mappings) = make_data(seed);

        let mut i2e_bytes = Vec::new();
        store_i2e(&mappings, &mut i2e_bytes).unwrap();
        let i2e = I2eHeader::parse(&i2e_bytes).unwrap();
        assert_eq!(i2e.data_offset % SECTION_ALIGN, 0);
        assert_eq!(i2e.is_uuid_offset % SECTION_ALIGN, 0);
        assert_eq!(
            i2e_bytes.len() as u64,
            i2e.is_uuid_offset + i2e.total.div_ceil(8),
            "i2e file length must match the parsed layout",
        );

        let mut e2i_bytes = Vec::new();
        store_e2i(&mappings, &mut e2i_bytes).unwrap();
        let e2i = E2iHeader::parse(&e2i_bytes).unwrap();
        assert_eq!(e2i.num_sparse_offset % SECTION_ALIGN, 0);
        assert_eq!(e2i.uuid_sparse_offset % SECTION_ALIGN, 0);
        assert_eq!(e2i.num_run_offset % SECTION_ALIGN, 0);
        assert_eq!(e2i.uuid_run_offset % SECTION_ALIGN, 0);
        assert_eq!(
            e2i_bytes.len() as u64,
            e2i.uuid_run_offset + e2i.uuid_count * UUID_ENTRY_SIZE,
            "e2i file length must match the parsed layout",
        );
        // The parsed offsets must also cover the written sections exactly.
        assert!(e2i.uuid_sparse_offset >= e2i.num_sparse_offset + e2i.num_blocks() * 8);
        assert!(e2i.num_run_offset >= e2i.uuid_sparse_offset + e2i.uuid_blocks() * 16);
        assert!(e2i.uuid_run_offset >= e2i.num_run_offset + e2i.num_count * NUM_ENTRY_SIZE);
    }
}
