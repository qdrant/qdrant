use super::super::*;
use super::*;
use crate::generic_consts::Sequential;
use crate::universal_io::read::UniversalRead;

#[test]
fn test_io_uring_file_for_u64() -> Result<()> {
    // 1. Write some u64 binary data to a file using regular std::fs APIs
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_u64.bin");

    let data: Vec<u64> = (0..128).collect();
    let bytes = bytemuck::cast_slice(&data);
    fs_err::write(&path, bytes).unwrap();

    // 2. Read data back using `IoUringFile` and verify it matches what was written
    let file = TypedStorage::<IoUringFile, u64>::open(&path, OpenOptions::default())?;

    // Read all elements
    let read_back = file.read::<Sequential>(ReadRange {
        byte_offset: 0,
        length: data.len() as u64,
    })?;
    assert_eq!(read_back.as_ref(), &data);

    // Read a sub-range (start at element 10, byte offset = 10 * size_of::<u64>())
    let read_sub = file.read::<Sequential>(ReadRange {
        byte_offset: 10 * size_of::<u64>() as u64,
        length: 20,
    })?;
    assert_eq!(read_sub.as_ref(), &data[10..30]);

    // Verify len()
    let len = file.len()?;
    assert_eq!(len, 128);

    Ok(())
}

#[test]
fn test_io_uring_read_batch() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test_batch.bin");

    let data: Vec<u64> = (0..256).collect();
    fs_err::write(&path, bytemuck::cast_slice(&data)).unwrap();

    let file = TypedStorage::<IoUringFile, u64>::open(&path, OpenOptions::default())?;
    let elem = size_of::<u64>() as u64;

    // Non-contiguous ranges across the file.
    #[rustfmt::skip]
    let ranges = vec![
        ReadRange { byte_offset: 0,          length: 10 }, // [0..10]
        ReadRange { byte_offset: 50 * elem,  length: 20 }, // [50..70]
        ReadRange { byte_offset: 100 * elem, length: 5  }, // [100..105]
        ReadRange { byte_offset: 200 * elem, length: 56 }, // [200..256]
    ];

    let expected: Vec<&[u64]> = vec![
        &data[0..10],
        &data[50..70],
        &data[100..105],
        &data[200..256],
    ];

    // --- read_batch (callback API) ---
    let mut batch_results: Vec<(usize, Vec<u64>)> = Vec::new();
    file.read_batch::<Sequential>(ranges.clone(), |idx, slice| {
        batch_results.push((idx, slice.to_vec()));
        Ok(())
    })?;

    batch_results.sort_by_key(|(idx, _)| *idx);
    for (idx, items) in &batch_results {
        assert_eq!(
            items.as_slice(),
            expected[*idx],
            "read_batch mismatch at index {idx}"
        );
    }

    // --- read_iter (iterator API) ---
    let mut iter_results: Vec<(usize, Vec<u64>)> = Vec::new();
    for record in file.read_iter::<Sequential>(ranges.clone()) {
        let (idx, cow) = record?;
        iter_results.push((idx, cow.into_owned()));
    }

    iter_results.sort_by_key(|(idx, _)| *idx);
    for (idx, items) in &iter_results {
        assert_eq!(
            items.as_slice(),
            expected[*idx],
            "read_iter mismatch at index {idx}"
        );
    }

    // --- read_iter with more ranges than the io_uring queue depth (64 > 16) ---
    let many_ranges: Vec<ReadRange> = (0..64)
        .map(|i| ReadRange {
            byte_offset: i * elem,
            length: 1,
        })
        .collect();

    let mut count = 0;
    for record in file.read_iter::<Sequential>(many_ranges) {
        let (idx, cow) = record?;
        assert_eq!(
            cow.as_ref(),
            &[data[idx]],
            "many-ranges mismatch at index {idx}"
        );
        count += 1;
    }
    assert_eq!(count, 64);

    Ok(())
}

#[test]
fn test_io_uring_concurrent_read_iter() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let elem = size_of::<u64>() as u64;

    // Large enough to span many io_uring batches (64 ranges, queue depth 16).
    const NUM_ELEMENTS: u64 = 6400;
    const NUM_RANGES: u64 = 64;
    const CHUNK: u64 = NUM_ELEMENTS / NUM_RANGES; // 100 elements per range

    // File A: 0..NUM_ELEMENTS
    let path_a = dir.path().join("a.bin");
    let data_a: Vec<u64> = (0..NUM_ELEMENTS).collect();
    fs_err::write(&path_a, bytemuck::cast_slice(&data_a)).unwrap();

    // File B: offset so values never overlap with A.
    let path_b = dir.path().join("b.bin");
    let data_b: Vec<u64> = (1_000_000..1_000_000 + NUM_ELEMENTS).collect();
    fs_err::write(&path_b, bytemuck::cast_slice(&data_b)).unwrap();

    let opts = OpenOptions {
        prevent_caching: Some(false), // should be true
        ..Default::default()
    };
    let file_a = TypedStorage::<IoUringFile, u64>::open(&path_a, opts)?;
    let file_b = TypedStorage::<IoUringFile, u64>::open(&path_b, opts)?;

    // NUM_RANGES ranges, each reading CHUNK elements — well over the queue depth.
    let ranges_a: Vec<ReadRange> = (0..NUM_RANGES)
        .map(|i| ReadRange {
            byte_offset: i * CHUNK * elem,
            length: CHUNK,
        })
        .collect();
    let ranges_b: Vec<ReadRange> = ranges_a.clone();

    let iter_a = file_a.read_iter::<Sequential>(ranges_a);
    let iter_b = file_b.read_iter::<Sequential>(ranges_b);

    // Zip alternates next() calls between the two iterators on the same
    // thread-local io_uring ring. With in-flight operations left across
    // next() calls, one iterator can reap the other's CQEs.
    let mut count = 0u64;
    for (rec_a, rec_b) in iter_a.zip(iter_b) {
        let (idx_a, cow_a) = rec_a?;
        let (idx_b, cow_b) = rec_b?;

        let start_a = idx_a as u64 * CHUNK;
        assert_eq!(
            cow_a.as_ref(),
            &data_a[start_a as usize..(start_a + CHUNK) as usize],
            "file A mismatch at range index {idx_a}"
        );

        let start_b = idx_b as u64 * CHUNK;
        assert_eq!(
            cow_b.as_ref(),
            &data_b[start_b as usize..(start_b + CHUNK) as usize],
            "file B mismatch at range index {idx_b}"
        );
        count += 1;
    }
    assert_eq!(count, NUM_RANGES);

    Ok(())
}

#[test]
fn test_io_uring_read_multi_iter_basic() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let elem = size_of::<u64>() as u64;

    // File 0: 0..128
    let path_0 = dir.path().join("f0.bin");
    let data_0: Vec<u64> = (0..128).collect();
    fs_err::write(&path_0, bytemuck::cast_slice(&data_0)).unwrap();

    // File 1: 1000..1128
    let path_1 = dir.path().join("f1.bin");
    let data_1: Vec<u64> = (1000..1128).collect();
    fs_err::write(&path_1, bytemuck::cast_slice(&data_1)).unwrap();

    let opts = OpenOptions::default();
    let file_0 = <IoUringFile as UniversalRead<u64>>::open(&path_0, opts)?;
    let file_1 = <IoUringFile as UniversalRead<u64>>::open(&path_1, opts)?;
    let files = [file_0, file_1];

    // Interleaved reads across both files.
    #[rustfmt::skip]
    let reads = vec![
        (0, ReadRange { byte_offset: 0,         length: 10 }), // f0[0..10]
        (1, ReadRange { byte_offset: 20 * elem,  length: 5 }),  // f1[20..25]
        (0, ReadRange { byte_offset: 50 * elem, length: 20 }), // f0[50..70]
        (1, ReadRange { byte_offset: 0,         length: 10 }), // f1[0..10]
    ];

    let expected: Vec<(FileIndex, &[u64])> = vec![
        (0, &data_0[0..10]),
        (1, &data_1[20..25]),
        (0, &data_0[50..70]),
        (1, &data_1[0..10]),
    ];

    let mut results: Vec<(usize, FileIndex, Vec<u64>)> = Vec::new();
    for record in IoUringFile::read_multi_iter::<Sequential>(&files, reads) {
        let (idx, file_idx, cow) = record?;
        results.push((idx, file_idx, cow.into_owned()));
    }

    results.sort_by_key(|(idx, _, _)| *idx);
    for (idx, file_idx, items) in &results {
        let (expected_file, expected_data) = expected[*idx];
        assert_eq!(*file_idx, expected_file, "file index mismatch at op {idx}");
        assert_eq!(items.as_slice(), expected_data, "data mismatch at op {idx}");
    }

    Ok(())
}

#[test]
fn test_io_uring_read_multi_iter_many_ranges() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let elem = size_of::<u64>() as u64;

    const NUM_FILES: usize = 4;
    const ELEMENTS_PER_FILE: u64 = 256;
    const RANGES_PER_FILE: u64 = 20; // 80 total > queue depth of 16

    let mut all_data: Vec<Vec<u64>> = Vec::new();
    let mut files: Vec<IoUringFile> = Vec::new();

    for i in 0..NUM_FILES {
        let base = (i as u64) * 10_000;
        let data: Vec<u64> = (base..base + ELEMENTS_PER_FILE).collect();
        let path = dir.path().join(format!("f{i}.bin"));
        fs_err::write(&path, bytemuck::cast_slice(&data)).unwrap();

        let file = <IoUringFile as UniversalRead<u64>>::open(&path, OpenOptions::default())?;
        files.push(file);
        all_data.push(data);
    }

    // Generate reads: round-robin across files, each reading a small chunk.
    let reads: Vec<(FileIndex, ReadRange)> = (0..NUM_FILES as u64 * RANGES_PER_FILE)
        .map(|i| {
            let file_idx = (i as usize) % NUM_FILES;
            let range_idx = i / NUM_FILES as u64;
            let offset = range_idx * 10; // non-overlapping chunks of 10
            (
                file_idx,
                ReadRange {
                    byte_offset: offset * elem,
                    length: 10,
                },
            )
        })
        .collect();

    let mut results: Vec<(usize, FileIndex, Vec<u64>)> = Vec::new();
    for record in IoUringFile::read_multi_iter::<Sequential>(&files, reads.clone()) {
        let (idx, file_idx, cow) = record?;
        results.push((idx, file_idx, cow.into_owned()));
    }

    assert_eq!(results.len(), reads.len());

    results.sort_by_key(|(idx, _, _)| *idx);
    for (idx, file_idx, items) in &results {
        let (expected_file, expected_range) = &reads[*idx];
        assert_eq!(file_idx, expected_file);
        let start = (expected_range.byte_offset / elem) as usize;
        let end = start + expected_range.length as usize;
        assert_eq!(
            items.as_slice(),
            &all_data[*file_idx][start..end],
            "data mismatch at op {idx}, file {file_idx}"
        );
    }

    Ok(())
}

/// Verify that `read_multi` (callback API) and `read_multi_iter` produce identical
/// results, confirming the callback version correctly delegates to the iterator.
#[test]
fn test_io_uring_read_multi_callback_matches_iter() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let elem = size_of::<u64>() as u64;

    let path_a = dir.path().join("a.bin");
    let data_a: Vec<u64> = (0..200).collect();
    fs_err::write(&path_a, bytemuck::cast_slice(&data_a)).unwrap();

    let path_b = dir.path().join("b.bin");
    let data_b: Vec<u64> = (5000..5200).collect();
    fs_err::write(&path_b, bytemuck::cast_slice(&data_b)).unwrap();

    let opts = OpenOptions::default();
    let file_a: IoUringFile = UniversalRead::<u64>::open(&path_a, opts)?;
    let file_b: IoUringFile = UniversalRead::<u64>::open(&path_b, opts)?;
    let files = [file_a, file_b];

    #[rustfmt::skip]
    let reads: Vec<(FileIndex, ReadRange)> = vec![
        (0, ReadRange { byte_offset: 0,           length: 50  }),
        (1, ReadRange { byte_offset: 10 * elem,   length: 30  }),
        (0, ReadRange { byte_offset: 100 * elem,  length: 50  }),
        (1, ReadRange { byte_offset: 0,           length: 100 }),
        (0, ReadRange { byte_offset: 150 * elem,  length: 50  }),
    ];

    // Collect via callback.
    let mut callback_results: Vec<(usize, FileIndex, Vec<u64>)> = Vec::new();
    IoUringFile::read_multi::<Sequential>(&files, reads.clone(), |idx, file_idx, data| {
        callback_results.push((idx, file_idx, data.to_vec()));
        Ok(())
    })?;

    // Collect via iterator.
    let mut iter_results: Vec<(usize, FileIndex, Vec<u64>)> = Vec::new();
    for record in IoUringFile::read_multi_iter::<Sequential>(&files, reads) {
        let (idx, file_idx, cow) = record?;
        iter_results.push((idx, file_idx, cow.into_owned()));
    }

    callback_results.sort_by_key(|(idx, _, _)| *idx);
    iter_results.sort_by_key(|(idx, _, _)| *idx);

    assert_eq!(callback_results.len(), iter_results.len());
    for (cb, it) in callback_results.iter().zip(iter_results.iter()) {
        assert_eq!(cb.0, it.0, "operation index mismatch");
        assert_eq!(cb.1, it.1, "file index mismatch");
        assert_eq!(cb.2, it.2, "data mismatch at op {}", cb.0);
    }

    Ok(())
}
