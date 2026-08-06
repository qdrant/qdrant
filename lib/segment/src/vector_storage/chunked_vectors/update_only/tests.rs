use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::Random;
use common::mmap::AdviceSetting;
use common::universal_io::{MmapFile, MmapFs, Populate};
use tempfile::{Builder, TempDir};

use super::UpdateOnlyChunkedVectors;
use crate::common::operation_error::OperationError;
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

/// A preallocated (`ChunkedVectors`-written) directory is not appendable-to:
/// its chunk files are longer than the stored vector count implies, which the
/// update-only writer rejects instead of adopting.
#[test]
fn rejects_preallocated_chunks() {
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

    let err = UpdateOnlyChunkedVectors::<f32, MmapFile>::open(MmapFs, dir.path(), DIM).unwrap_err();
    assert!(
        matches!(err, OperationError::InconsistentStorage { description: _ }),
        "{err}",
    );
}
