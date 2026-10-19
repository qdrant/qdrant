use std::borrow::Cow;

use bytemuck::PodCastError;

use crate::generic_consts::Random;
use crate::universal_io::{
    DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, MmapFile, OpenOptions,
    OwnedPipeline, Populate, ReadPipeline, ReadRange, UioResult, UniversalIoError, UniversalRead,
    UniversalReadFs,
};

struct CachedFile {
    file: DiskCache<MmapFile>,
    _directory: tempfile::TempDir,
}

impl CachedFile {
    fn new(bytes: &[u8]) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let remote = directory.path().join("remote");
        let local = directory.path().join("local");
        fs_err::create_dir_all(&remote).unwrap();
        fs_err::create_dir_all(&local).unwrap();
        let path = remote.join("data.bin");
        fs_err::write(&path, bytes).unwrap();
        let config = DiskCacheConfig::new(remote, local).unwrap();
        let fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: config.into(),
            remote: (),
        })
        .unwrap();
        let file = fs
            .open(
                path,
                OpenOptions {
                    writeable: false,
                    populate: Populate::No,
                    ..OpenOptions::new_for_test()
                },
                Default::default(),
            )
            .unwrap();
        Self {
            file,
            _directory: directory,
        }
    }
}

const BYTES: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

#[test]
fn empty_cached_read_returns_typed_empty_slice() {
    let fixture = CachedFile::new(BYTES);
    for offset in [0, 1, 12] {
        let items = fixture
            .file
            .read::<_, u32>(ReadRange::new(offset, 0), Random)
            .unwrap();
        assert!(items.is_empty());
        assert!(matches!(items, Cow::Borrowed(_)));
    }
    let empty = CachedFile::new(&[]);
    assert!(empty.file.read_whole::<u64>().unwrap().is_empty());
}

#[test]
fn unaligned_cached_read_returns_cast_error() {
    let fixture = CachedFile::new(BYTES);
    for _ in 0..2 {
        let result = fixture.file.read::<_, u32>(ReadRange::new(1, 2), Random);
        assert!(matches!(
            result,
            Err(UniversalIoError::BytemuckCast(
                PodCastError::TargetAlignmentGreaterAndInputNotAligned
            ))
        ));
    }
}

#[test]
fn aligned_cached_read_borrows_mmap() {
    let fixture = CachedFile::new(BYTES);
    let bytes = fixture.file.read_bytes(0..8, Random, 1).unwrap();
    let items = fixture
        .file
        .read::<_, u32>(ReadRange::new(0, 2), Random)
        .unwrap();
    assert_eq!(items.as_ptr().cast::<u8>(), bytes.as_ptr());
    assert!(matches!(items, Cow::Borrowed(_)));
    assert_eq!(items[0], u32::from_ne_bytes([0, 1, 2, 3]));
}

#[test]
fn cached_batch_reports_unaligned_read_error() {
    let fixture = CachedFile::new(BYTES);
    let mut count = 0;
    let result = fixture.file.read_batch(
        [(0, ReadRange::new(1, 0)), (1, ReadRange::new(1, 2))],
        Random,
        |id, items: &[u32]| {
            assert_eq!(id, 0);
            assert!(items.is_empty());
            count += 1;
            UioResult::Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(UniversalIoError::BytemuckCast(
            PodCastError::TargetAlignmentGreaterAndInputNotAligned
        ))
    ));
    assert_eq!(count, 1);
}

#[test]
fn cached_iterator_reports_unaligned_read_error() {
    let fixture = CachedFile::new(BYTES);
    let mut iter = fixture
        .file
        .read_iter::<_, u32, _>(
            [(0, ReadRange::new(1, 0)), (1, ReadRange::new(1, 2))],
            Random,
        )
        .unwrap();
    let (id, items) = iter.next().unwrap().unwrap();
    assert_eq!(id, 0);
    assert!(items.is_empty());
    assert!(matches!(
        iter.next().unwrap(),
        Err(UniversalIoError::BytemuckCast(
            PodCastError::TargetAlignmentGreaterAndInputNotAligned
        ))
    ));
    assert!(iter.next().is_none());
}

#[test]
fn owned_cached_pipeline_reports_unaligned_read_error() {
    let fixture = CachedFile::new(BYTES);
    let mut pipeline = OwnedPipeline::<_, ()>::new(fixture.file).unwrap();
    pipeline.schedule::<Random>((), 1..1, 4).unwrap();
    let ((), items) = pipeline.wait_bytemuck::<u32>().unwrap().unwrap();
    assert!(items.is_empty());
    pipeline.schedule::<Random>((), 1..9, 4).unwrap();
    assert!(matches!(
        pipeline.wait_bytemuck::<u32>(),
        Err(UniversalIoError::BytemuckCast(
            PodCastError::TargetAlignmentGreaterAndInputNotAligned
        ))
    ));
    assert!(pipeline.wait_bytemuck::<u32>().unwrap().is_none());
}

#[test]
fn cached_pipeline_rejects_partial_typed_items() {
    let fixture = CachedFile::new(BYTES);
    let mut pipeline =
        <DiskCache<MmapFile> as UniversalRead>::ReadPipeline::<'_, ()>::new().unwrap();
    for range in [0..3, 1..4] {
        pipeline
            .schedule::<Random>((), &fixture.file, range, 4)
            .unwrap();
        assert!(matches!(
            pipeline.wait_bytemuck::<u32>(),
            Err(UniversalIoError::BytemuckCast(_))
        ));
    }
}

#[test]
fn owned_cached_pipeline_rejects_partial_typed_items() {
    let fixture = CachedFile::new(BYTES);
    let mut pipeline = OwnedPipeline::<_, ()>::new(fixture.file).unwrap();
    pipeline.schedule::<Random>((), 1..4, 4).unwrap();
    assert!(matches!(
        pipeline.wait_bytemuck::<u32>(),
        Err(UniversalIoError::BytemuckCast(_))
    ));
}
