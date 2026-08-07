use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::{MmapFile, MmapFs, Populate};
use tempfile::{Builder, TempDir};

use super::UpdateOnlyChunkedVectors;
use crate::vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::chunked_vectors::chunks::chunk_name;
use crate::vector_storage::chunked_vectors::config::{config_file, status_file};
use crate::vector_storage::chunked_vectors::read_only::ReadOnlyChunkedVectors;
use crate::vector_storage::chunked_vectors::test_utils::{append_range, make_vec};

const DIM: usize = 32;
/// Spans three test chunks (4096 vectors each), ending mid-chunk.
const COUNT: usize = 9000;

/// Write the same `COUNT` vectors through both writers: `ChunkedVectors` into
/// the first directory, `UpdateOnlyChunkedVectors` into the second — the
/// latter over two sessions to also exercise reopening mid-chunk.
fn write_both() -> (TempDir, TempDir) {
    let hw = HardwareCounterCell::disposable();
    let plain_dir = Builder::new().prefix("chunked_plain").tempdir().unwrap();
    let appended_dir = Builder::new().prefix("chunked_appended").tempdir().unwrap();

    let mut plain = ChunkedVectors::<f32, MmapFile>::open(
        MmapFs,
        plain_dir.path(),
        DIM,
        AdviceSetting::Global,
        Populate::No,
    )
    .unwrap();
    for seed in 0..COUNT {
        plain.push(make_vec(seed, DIM).as_slice(), &hw).unwrap();
    }
    plain.flusher()().unwrap();

    for range in [0..COUNT / 2, COUNT / 2..COUNT] {
        let mut writer =
            UpdateOnlyChunkedVectors::<f32, MmapFile>::open(MmapFs, appended_dir.path(), DIM)
                .unwrap();
        append_range(&mut writer, range, DIM, &hw);
    }

    (plain_dir, appended_dir)
}

/// Both writers produce the same on-disk format: identical config and status
/// files, and byte-identical chunk data — an appended chunk is the
/// preallocated chunk minus the not-yet-written tail.
#[test]
fn writes_congruent_format() {
    let (plain_dir, appended_dir) = write_both();

    assert_eq!(
        fs_err::read(config_file(plain_dir.path())).unwrap(),
        fs_err::read(config_file(appended_dir.path())).unwrap(),
        "config files differ",
    );
    assert_eq!(
        fs_err::read(status_file(plain_dir.path())).unwrap(),
        fs_err::read(status_file(appended_dir.path())).unwrap(),
        "status files differ",
    );

    let mut remaining = COUNT * DIM * size_of::<f32>();
    let mut chunk_id = 0;
    while remaining > 0 {
        let plain = fs_err::read(chunk_name(plain_dir.path(), chunk_id)).unwrap();
        let appended = fs_err::read(chunk_name(appended_dir.path(), chunk_id)).unwrap();

        let data_len = remaining.min(plain.len());
        assert_eq!(appended.len(), data_len, "chunk {chunk_id} length");
        assert!(appended[..] == plain[..data_len], "chunk {chunk_id} data");

        remaining -= data_len;
        chunk_id += 1;
    }
    assert!(chunk_id > 1, "the data must span multiple chunks");

    // Neither directory has chunk files past the data
    assert!(!chunk_name(plain_dir.path(), chunk_id).exists());
    assert!(!chunk_name(appended_dir.path(), chunk_id).exists());
}

/// A reader over an append-written directory serves exactly what one over a
/// `ChunkedVectors`-written directory does.
#[test]
fn directory_reads_congruently() {
    let (plain_dir, appended_dir) = write_both();

    let open_reader = |dir: &std::path::Path| {
        ReadOnlyChunkedVectors::<f32, MmapFile>::open(
            &MmapFs,
            dir,
            DIM,
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap()
    };
    let plain = open_reader(plain_dir.path());
    let appended = open_reader(appended_dir.path());

    assert_eq!(plain.len(), COUNT);
    assert_eq!(appended.len(), COUNT);
    for key in 0..COUNT {
        let expected = make_vec(key, DIM);
        let key = key as VectorOffsetType;
        assert_eq!(
            plain.get::<Random>(key).unwrap().as_ref(),
            expected.as_slice(),
        );
        assert_eq!(
            appended.get::<Random>(key).unwrap().as_ref(),
            expected.as_slice(),
        );
    }
}

/// A preallocated (`ChunkedVectors`-written) directory has chunk files longer
/// than the stored vector count implies. `open` no longer inspects chunk
/// files at all, so it succeeds regardless; the next `append_many` call
/// reconciles the oversized chunk down to the persisted watermark before
/// appending, same as it would for a crashed writer's leftover bytes.
#[test]
fn repairs_preallocated_chunks_on_next_append() {
    let hw = HardwareCounterCell::disposable();
    let dir = Builder::new().prefix("chunked_prealloc").tempdir().unwrap();

    let mut plain = ChunkedVectors::<f32, MmapFile>::open(
        MmapFs,
        dir.path(),
        DIM,
        AdviceSetting::Global,
        Populate::No,
    )
    .unwrap();
    plain.push(make_vec(0, DIM).as_slice(), &hw).unwrap();
    plain.flusher()().unwrap();
    drop(plain);

    // The chunk file is preallocated to a full chunk, way past the single
    // stored vector the status file reports.
    let preallocated_size = fs_err::metadata(chunk_name(dir.path(), 0)).unwrap().len();
    assert!(preallocated_size > (DIM * size_of::<f32>()) as u64);

    let mut writer =
        UpdateOnlyChunkedVectors::<f32, MmapFile>::open(MmapFs, dir.path(), DIM).unwrap();
    append_range(&mut writer, 1..2, DIM, &hw);

    // The chunk shrank to exactly what the two vectors need — the
    // preallocated tail is gone.
    let repaired_size = fs_err::metadata(chunk_name(dir.path(), 0)).unwrap().len();
    assert_eq!(repaired_size, (2 * DIM * size_of::<f32>()) as u64);

    let reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
        &MmapFs,
        dir.path(),
        DIM,
        AdviceSetting::Global,
        Populate::No,
    )
    .unwrap();
    assert_eq!(reader.len(), 2);
    for key in 0..2 {
        assert_eq!(
            reader
                .get::<Random>(key as VectorOffsetType)
                .unwrap()
                .as_ref(),
            make_vec(key, DIM).as_slice(),
        );
    }
}

/// A batch whose first offset lands *behind* the persisted watermark is a
/// replay of an already-applied range (e.g. a WAL resending a batch after a
/// crash that happened before the outer commit pointer advanced, but after
/// this writer's data was durable). `append_many` must shrink the chunk back
/// to that offset and overwrite it, rather than blindly appending after the
/// existing data and corrupting the offset-to-vector mapping.
#[test]
fn replaying_an_already_applied_range_overwrites_it() {
    let hw = HardwareCounterCell::disposable();
    let dir = Builder::new().prefix("chunked_replay").tempdir().unwrap();

    let mut writer =
        UpdateOnlyChunkedVectors::<f32, MmapFile>::open(MmapFs, dir.path(), DIM).unwrap();
    append_range(&mut writer, 0..100, DIM, &hw);

    // Replay offsets 50..100 with different vectors than landed the first
    // time, so the overwrite is observable.
    let replay: Vec<(VectorOffsetType, Vec<f32>)> = (50..100)
        .map(|seed| (seed as VectorOffsetType, make_vec(seed + 1000, DIM)))
        .collect();
    writer
        .append_many(
            replay
                .iter()
                .map(|(offset, vector)| (*offset, vector.as_slice())),
            &hw,
        )
        .unwrap();

    let reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
        &MmapFs,
        dir.path(),
        DIM,
        AdviceSetting::Global,
        Populate::No,
    )
    .unwrap();
    // Not doubled: the watermark lands back at 100, not 150.
    assert_eq!(reader.len(), 100);
    for key in 0..50 {
        assert_eq!(
            reader
                .get::<Random>(key as VectorOffsetType)
                .unwrap()
                .as_ref(),
            make_vec(key, DIM).as_slice(),
            "untouched prefix should be unchanged",
        );
    }
    for key in 50..100 {
        assert_eq!(
            reader
                .get::<Random>(key as VectorOffsetType)
                .unwrap()
                .as_ref(),
            make_vec(key + 1000, DIM).as_slice(),
            "replayed range should reflect the newer batch",
        );
    }
}

/// A batch whose first offset lands *ahead* of the persisted watermark
/// skips a range (e.g. points deleted before ever getting a vector).
/// `append_many` pads the gap with zero vectors instead of shifting later
/// offsets down to close it.
#[test]
fn extends_across_a_gap_with_zeroes() {
    let hw = HardwareCounterCell::disposable();
    let dir = Builder::new().prefix("chunked_gap").tempdir().unwrap();

    let mut writer =
        UpdateOnlyChunkedVectors::<f32, MmapFile>::open(MmapFs, dir.path(), DIM).unwrap();
    append_range(&mut writer, 0..10, DIM, &hw);
    // Offsets 10..15 are skipped; the next batch picks up at 15.
    append_range(&mut writer, 15..20, DIM, &hw);

    let reader = ReadOnlyChunkedVectors::<f32, MmapFile>::open(
        &MmapFs,
        dir.path(),
        DIM,
        AdviceSetting::Global,
        Populate::No,
    )
    .unwrap();
    assert_eq!(reader.len(), 20);
    for key in 0..10 {
        assert_eq!(
            reader
                .get::<Random>(key as VectorOffsetType)
                .unwrap()
                .as_ref(),
            make_vec(key, DIM).as_slice(),
        );
    }
    for key in 10..15 {
        assert_eq!(
            reader
                .get::<Random>(key as VectorOffsetType)
                .unwrap()
                .as_ref(),
            vec![0.0f32; DIM].as_slice(),
            "skipped offset {key} should read back as zeroes",
        );
    }
    for key in 15..20 {
        assert_eq!(
            reader
                .get::<Random>(key as VectorOffsetType)
                .unwrap()
                .as_ref(),
            make_vec(key, DIM).as_slice(),
        );
    }
}
