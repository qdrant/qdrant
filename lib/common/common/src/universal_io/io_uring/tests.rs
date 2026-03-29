use super::super::*;
use super::*;
use crate::generic_consts::Sequential;

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
    let ranges = vec![
        ReadRange {
            byte_offset: 0,
            length: 10,
        }, // [0..10]
        ReadRange {
            byte_offset: 50 * elem,
            length: 20,
        }, // [50..70]
        ReadRange {
            byte_offset: 100 * elem,
            length: 5,
        }, // [100..105]
        ReadRange {
            byte_offset: 200 * elem,
            length: 56,
        }, // [200..256]
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
