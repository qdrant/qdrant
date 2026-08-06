use std::path::Path;

use common::universal_io::{MmapFile, MmapFs, OpenOptions};
use rstest::rstest;
use tempfile::TempDir;

use super::*;

type TestBitSlice = BufferedUpdateBitSlice<MmapFile>;

/// 300 flags: not a whole number of `u64` words, so the raw format pads.
const BIT_LEN: usize = 300;

fn paths(dir: &Path) -> BitmaskPaths {
    BitmaskPaths::new(dir.join("flags.bin"), dir.join("flags.mask"))
}

fn create(dir: &Path, format: BitmaskFormat, ones: impl IntoIterator<Item = u64>) -> TestBitSlice {
    BufferedUpdateBitSlice::create(
        &MmapFs,
        &paths(dir),
        OpenOptions::new_for_test(),
        format,
        BIT_LEN,
        ones,
    )
    .unwrap()
}

fn open(dir: &Path) -> TestBitSlice {
    BufferedUpdateBitSlice::open(&MmapFs, &paths(dir), OpenOptions::new_for_test()).unwrap()
}

fn ones_of(flags: &TestBitSlice) -> Vec<usize> {
    ones_of_range(flags, BIT_LEN)
}

fn ones_of_range(flags: &TestBitSlice, len: usize) -> Vec<usize> {
    (0..len)
        .filter(|index| flags.get(*index).unwrap())
        .collect()
}

#[rstest]
#[case(BitmaskFormat::Raw)]
#[case(BitmaskFormat::Compact)]
fn create_persists_initial_ones(#[case] format: BitmaskFormat) {
    let dir = TempDir::new().unwrap();
    let flags = create(dir.path(), format, [3, 64, 299]);

    assert_eq!(flags.format(), format);
    assert_eq!(flags.path(), *paths(dir.path()).of(format));
    assert_eq!(ones_of(&flags), [3, 64, 299]);
    drop(flags);

    // Reopening finds the format it was created in, by file name.
    let reopened = open(dir.path());
    assert_eq!(reopened.format(), format);
    assert_eq!(ones_of(&reopened), [3, 64, 299]);
}

#[rstest]
#[case(BitmaskFormat::Raw)]
#[case(BitmaskFormat::Compact)]
fn updates_are_buffered_until_flushed(#[case] format: BitmaskFormat) {
    let dir = TempDir::new().unwrap();
    let flags = create(dir.path(), format, [3, 64, 299]);

    flags.set(3, false);
    flags.set(5, true);
    flags.set(298, true);

    // Visible through the wrapper right away...
    assert_eq!(ones_of(&flags), [5, 64, 298, 299]);
    // ...but not on disk yet.
    assert_eq!(ones_of(&open(dir.path())), [3, 64, 299]);

    flags.flusher()().unwrap();
    assert_eq!(ones_of(&open(dir.path())), [5, 64, 298, 299]);
}

#[rstest]
#[case(BitmaskFormat::Raw)]
#[case(BitmaskFormat::Compact)]
fn repeated_flushes_accumulate(#[case] format: BitmaskFormat) {
    let dir = TempDir::new().unwrap();
    let flags = create(dir.path(), format, []);

    for index in [10, 200, 299] {
        flags.set(index, true);
        flags.flusher()().unwrap();
    }
    flags.set(200, false);
    flags.flusher()().unwrap();

    assert_eq!(ones_of(&open(dir.path())), [10, 299]);
    // The wrapper agrees with what it just wrote.
    assert_eq!(ones_of(&flags), [10, 299]);
}

#[rstest]
#[case(BitmaskFormat::Raw)]
#[case(BitmaskFormat::Compact)]
fn read_all_covers_pending_updates(#[case] format: BitmaskFormat) {
    let dir = TempDir::new().unwrap();
    let flags = create(dir.path(), format, [7]);

    flags.set(7, false);
    flags.set(8, true);

    let bits = flags.read_all().unwrap();
    assert_eq!(bits.len(), flags.len());
    assert!(!bits[7]);
    assert!(bits[8]);
    assert_eq!(bits.count_ones(), 1);
}

/// The raw format can only report the padded length its file has; the compact
/// one records the exact flag count.
#[test]
fn len_is_padded_for_raw_only() {
    let raw_dir = TempDir::new().unwrap();
    let compact_dir = TempDir::new().unwrap();
    let raw = create(raw_dir.path(), BitmaskFormat::Raw, []);
    let compact = create(compact_dir.path(), BitmaskFormat::Compact, []);

    assert_eq!(raw.len(), BIT_LEN.next_multiple_of(u64::BITS as usize));
    assert_eq!(compact.len(), BIT_LEN);
}

/// Creating replaces both files, so no flag of the previous mask survives and
/// the format that was just written is the one a reopen finds.
#[rstest]
#[case(BitmaskFormat::Raw, BitmaskFormat::Compact)]
#[case(BitmaskFormat::Compact, BitmaskFormat::Raw)]
#[case(BitmaskFormat::Raw, BitmaskFormat::Raw)]
#[case(BitmaskFormat::Compact, BitmaskFormat::Compact)]
fn create_replaces_an_existing_mask(#[case] from: BitmaskFormat, #[case] to: BitmaskFormat) {
    let dir = TempDir::new().unwrap();

    drop(create(dir.path(), from, [1, 2, 3, 250]));
    drop(create(dir.path(), to, [9]));

    let paths = paths(dir.path());
    assert!(paths.of(to).exists());
    if from != to {
        assert!(
            !paths.of(from).exists(),
            "{} was left behind",
            paths.of(from).display(),
        );
    }

    let reopened = open(dir.path());
    assert_eq!(reopened.format(), to);
    assert_eq!(ones_of(&reopened), [9]);
}

/// A sparse compact mask stays far below the dense size, and a flush keeps it
/// that way instead of growing the file.
#[test]
fn compact_file_stays_small() {
    const MANY: usize = 1_000_000;

    let dir = TempDir::new().unwrap();
    let paths = paths(dir.path());
    let flags = BufferedUpdateBitSlice::<MmapFile>::create(
        &MmapFs,
        &paths,
        OpenOptions::new_for_test(),
        BitmaskFormat::Compact,
        MANY,
        [42],
    )
    .unwrap();

    let dense_len = MANY as u64 / u64::from(u8::BITS);
    let created_len = fs_err::metadata(&paths.compact).unwrap().len();
    assert!(created_len < dense_len / 100, "{created_len} bytes");

    flags.set(MANY - 1, true);
    flags.flusher()().unwrap();

    let flushed_len = fs_err::metadata(&paths.compact).unwrap().len();
    assert!(flushed_len < dense_len / 100, "{flushed_len} bytes");

    let reopened =
        BufferedUpdateBitSlice::<MmapFile>::open(&MmapFs, &paths, OpenOptions::new_for_test())
            .unwrap();
    assert_eq!(ones_of_range(&reopened, MANY), [42, MANY - 1]);
}

#[rstest]
#[case(BitmaskFormat::Raw)]
#[case(BitmaskFormat::Compact)]
fn create_rejects_out_of_range_ones(#[case] format: BitmaskFormat) {
    let dir = TempDir::new().unwrap();
    let result = BufferedUpdateBitSlice::<MmapFile>::create(
        &MmapFs,
        &paths(dir.path()),
        OpenOptions::new_for_test(),
        format,
        BIT_LEN,
        [BIT_LEN as u64],
    );
    assert!(result.is_err(), "{:?}", result.map(|flags| flags.len()));
}

/// A segment written before the compact format existed has only the raw file.
#[test]
fn legacy_dense_file_opens_as_raw() {
    let dir = TempDir::new().unwrap();
    let paths = paths(dir.path());
    // Two `u64` words with bits 1 and 64 set, as a raw bitslice stores them.
    fs_err::write(
        &paths.raw,
        [0x02, 0, 0, 0, 0, 0, 0, 0, 0x01, 0, 0, 0, 0, 0, 0, 0],
    )
    .unwrap();

    let flags =
        BufferedUpdateBitSlice::<MmapFile>::open(&MmapFs, &paths, OpenOptions::new_for_test())
            .unwrap();
    assert_eq!(flags.format(), BitmaskFormat::Raw);
    assert_eq!(flags.len(), 128);
    assert_eq!(ones_of_range(&flags, 128), [1, 64]);
}
