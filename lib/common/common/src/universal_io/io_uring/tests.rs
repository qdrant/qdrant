use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use nix::libc;

use super::super::*;
use super::*;
use crate::generic_consts::Sequential;
use crate::universal_io::UioResult;

/// Create `path`, populate it with the binary representation of `data`,
/// then open and return it.
fn test_file<T: bytemuck::Pod>(path: &Path, data: &[T], direct_io: bool) -> UioResult<IoUringFile> {
    fs_err::write(path, bytemuck::cast_slice(data))?;

    let fs = IoUringFs::from_context(Default::default())?;

    fs.open(
        path,
        OpenOptions::new_for_test(),
        IoUringOpenExtra {
            prevent_caching: direct_io,
        },
    )
}

/// Build a [`ReadRange`] for type `T` spanning `length` elements starting at
/// element `offset`. E.g. `read_range::<u64>(2, 10)` covers `[2_u64..12_u64]`
/// from the start of the file.
fn read_range<T>(offset: usize, length: usize) -> ReadRange {
    ReadRange {
        byte_offset: (offset * size_of::<T>()) as u64,
        length: length as u64,
    }
}

#[test]
fn test_io_uring_read() -> UioResult<()> {
    // 1. Populate test file with u64 binary data
    let dir = tempfile::tempdir().unwrap();

    let data: Vec<u64> = (0..128).collect();
    let file = test_file(&dir.path().join("test_u64.bin"), &data, false)?;
    let file = TypedStorage::<_, u64>::new(file);

    // 2. Read data back and verify it matches what was written

    // Read all elements
    let full = file.read::<Sequential>(read_range::<u64>(0, data.len()))?;
    assert_eq!(full.as_ref(), &data);

    // Read a sub-range (elements 10..30)
    let sub = file.read::<Sequential>(read_range::<u64>(10, 20))?;
    assert_eq!(sub.as_ref(), &data[10..30]);

    // Verify len()
    let len = file.len()?;
    assert_eq!(len, 128);

    Ok(())
}

#[test]
fn test_io_uring_read_batch_read_iter() -> UioResult<()> {
    let dir = tempfile::tempdir().unwrap();

    let data: Vec<u64> = (0..256).collect();
    let file = test_file(&dir.path().join("test_batch.bin"), &data, false)?;
    let file = TypedStorage::<_, u64>::new(file);

    // Non-contiguous ranges across the file (element offset, elements count)
    let ranges = [
        read_range::<u64>(0, 10),   // [0..10]
        read_range::<u64>(50, 20),  // [50..70]
        read_range::<u64>(100, 5),  // [100..105]
        read_range::<u64>(200, 56), // [200..256]
    ];

    let expected = [
        &data[0..10],
        &data[50..70],
        &data[100..105],
        &data[200..256],
    ];

    // --- read_batch (callback API) ---
    let mut batch_results = Vec::new();

    file.read_batch(ranges.into_iter().enumerate(), Sequential, |idx, items| {
        batch_results.push((idx, items.to_vec()));
        UioResult::Ok(())
    })?;

    batch_results.sort_by_key(|&(idx, _)| idx);

    for (idx, items) in batch_results {
        assert_eq!(
            items.as_slice(),
            expected[idx],
            "read_batch mismatch at index {idx}"
        );
    }

    // --- read_iter (iterator API) ---
    let read_iter = file.read_iter(ranges.into_iter().enumerate(), Sequential)?;

    let mut iter_results: Vec<_> = read_iter.collect::<UioResult<Vec<_>>>()?;
    iter_results.sort_by_key(|&(idx, _)| idx);

    for (idx, items) in iter_results {
        assert_eq!(
            items.as_ref(),
            expected[idx],
            "read_iter mismatch at index {idx}"
        );
    }

    // --- read_iter with more ranges than the io_uring queue depth (64 > 16) ---
    let many_ranges = (0..64).map(|i| read_range::<u64>(i, 1)).enumerate();

    let mut count = 0;
    for record in file.read_iter(many_ranges, Sequential)? {
        let (idx, items) = record?;

        assert_eq!(
            items.as_ref(),
            &[data[idx]],
            "many-ranges mismatch at index {idx}"
        );

        count += 1;
    }

    assert_eq!(count, 64);

    Ok(())
}

#[test]
fn test_io_uring_read_iter_concurrent() -> UioResult<()> {
    let dir = tempfile::tempdir().unwrap();

    // Large enough to span many io_uring batches (64 ranges, queue depth 16).
    const NUM_ELEMENTS: u64 = 6400;
    const NUM_RANGES: u64 = 64;
    const CHUNK: u64 = NUM_ELEMENTS / NUM_RANGES; // 100 elements per range

    // File A: 0..NUM_ELEMENTS
    let data_a: Vec<u64> = (0..NUM_ELEMENTS).collect();
    let file_a = test_file(&dir.path().join("a.bin"), &data_a, false)?;
    let file_a = TypedStorage::<_, u64>::new(file_a);

    // File B: offset so values never overlap with A.
    let data_b: Vec<u64> = (1_000_000..1_000_000 + NUM_ELEMENTS).collect();
    let file_b = test_file(&dir.path().join("b.bin"), &data_b, false)?;
    let file_b = TypedStorage::<_, u64>::new(file_b);

    // NUM_RANGES ranges, each reading CHUNK elements — well over the queue depth.
    let ranges_a = (0..NUM_RANGES).map(|i| read_range::<u64>((i * CHUNK) as usize, CHUNK as usize));
    let ranges_b = (0..NUM_RANGES).map(|i| read_range::<u64>((i * CHUNK) as usize, CHUNK as usize));

    let iter_a = file_a.read_iter(ranges_a.enumerate(), Sequential)?;
    let iter_b = file_b.read_iter(ranges_b.enumerate(), Sequential)?;

    // Zip alternates next() calls between the two iterators on the same
    // thread-local io_uring ring. With in-flight operations left across
    // next() calls, one iterator can reap the other's CQEs.
    let mut count = 0u64;

    for (rec_a, rec_b) in iter_a.zip(iter_b) {
        let (idx_a, cow_a) = rec_a?;
        let (idx_b, cow_b) = rec_b?;

        let chunk_len = CHUNK as usize;

        let offset_a = idx_a * chunk_len;
        let data_a = &data_a[offset_a..offset_a + chunk_len];
        assert_eq!(
            cow_a.as_ref(),
            data_a,
            "file A mismatch at range index {idx_a}"
        );

        let offset_b = idx_b * chunk_len;
        let data_b = &data_b[offset_b..offset_b + chunk_len];
        assert_eq!(
            cow_b.as_ref(),
            data_b,
            "file B mismatch at range index {idx_b}"
        );

        count += 1;
    }

    assert_eq!(count, NUM_RANGES);

    Ok(())
}

extern "C" fn noop_signal_handler(_sig: libc::c_int) {}

/// Asserts that `read_iter` handles `EINTR` transparently by retrying
/// `submit_and_wait`. Under signal bombardment with cold page cache,
/// no errors or panics should surface to the caller.
#[test]
fn test_io_uring_eintr_handling() -> UioResult<()> {
    // Install a no-op SIGUSR1 handler *without* SA_RESTART so that
    // io_uring_enter() receives EINTR instead of auto-restarting.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = noop_signal_handler as *const () as usize;
        sa.sa_flags = 0;
        libc::sigemptyset(&mut sa.sa_mask);

        let ret = libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
        assert_eq!(ret, 0, "failed to install SIGUSR1 handler");
    }

    // 32 MB file — large enough that reads from cold cache block
    // inside io_uring_enter, giving signals a window to cause EINTR.
    const NUM_ELEMENTS: u64 = 4 * 1024 * 1024;
    const RANGES_PER_ROUND: u64 = 128;
    const TEST_DURATION_SECS: u64 = 10;

    let dir = tempfile::tempdir().unwrap();

    let data: Vec<u64> = (0..NUM_ELEMENTS).collect();
    let file = test_file(&dir.path().join("eintr_test.bin"), &data, false)?;
    let file = TypedStorage::<_, u64>::new(file);

    let stop = Arc::new(AtomicBool::new(false));
    let signals_sent = Arc::new(AtomicU64::new(0));
    let target_thread: libc::pthread_t = unsafe { libc::pthread_self() };

    let signal_thread = {
        let stop = stop.clone();
        let sent = signals_sent.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                unsafe { libc::pthread_kill(target_thread, libc::SIGUSR1) };
                sent.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        })
    };

    let chunk_size = NUM_ELEMENTS / RANGES_PER_ROUND;
    let mut eintr_errors = 0u64;
    let mut panics = 0u64;
    let mut rounds = 0u64;
    let start = std::time::Instant::now();

    while start.elapsed().as_secs() < TEST_DURATION_SECS {
        rounds += 1;

        // Evict pages so reads actually block in io_uring_enter.
        file.clear_ram_cache().ok();

        let ranges = (0..RANGES_PER_ROUND).map(|i| {
            let range = read_range::<u64>((i * chunk_size) as usize, chunk_size as usize);
            (i, range)
        });

        // catch_unwind: the Drop path has debug_assert!(self.is_empty())
        // which panics when in-flight requests leak due to EINTR.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut errors = 0u64;

            let Ok(iter) = file.read_iter(ranges, Sequential) else {
                return 1;
            };

            for record in iter {
                if record.is_err() {
                    errors += 1;
                    break;
                }
            }

            errors
        }));

        match result {
            Ok(n) => eintr_errors += n,
            Err(_) => panics += 1,
        }

        if eintr_errors > 0 || panics > 0 {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    signal_thread.join().unwrap();

    let total_signals = signals_sent.load(Ordering::Relaxed);
    let failures = eintr_errors + panics;

    assert_eq!(
        failures, 0,
        "io_uring submit_and_wait does not retry on EINTR: \
         {eintr_errors} errors, {panics} panics in {rounds} rounds ({total_signals} signals). \
         Fix: retry submit_and_wait when it returns io::ErrorKind::Interrupted."
    );

    Ok(())
}

/// Reads an `O_DIRECT` file via [`IoUringFile::read_bytes`] and [`BorrowedIoUringPipeline`].
///
/// Every read is `KERNEL_PAGE_SIZE` aligned on both ends, with `align` set to `KERNEL_PAGE_SIZE`.
/// The last block extends past EOF, so its read returns a truncated tail of valid bytes.
#[test]
fn test_io_uring_direct_io() -> UioResult<()> {
    use super::pipeline::IoUringPipeline;

    let dir = tempfile::tempdir().unwrap();

    // 2 + some pages of data
    let data: Vec<u8> = (0..KERNEL_PAGE_SIZE * 2 + 1337)
        .map(|idx| (idx % 256) as u8)
        .collect();

    let file = test_file(&dir.path().join("o_direct.bin"), &data, true)?;

    // Read each page-aligned block. Both ends of the range and `align` are `KERNEL_PAGE_SIZE`
    // aligned, as `O_DIRECT` requires.

    // --- via `read_bytes` ---
    for (idx, expected) in data.chunks(KERNEL_PAGE_SIZE).enumerate() {
        let start = idx * KERNEL_PAGE_SIZE;
        let end = start + expected.len();

        let range = start as u64..end as u64;
        let bytes = file.read_bytes::<Sequential>(range, KERNEL_PAGE_SIZE)?;

        assert_eq!(bytes.as_ref(), expected, "O_DIRECT block {idx} mismatch");
    }

    // --- via read pipeline ---
    let mut pipeline = IoUringPipeline::new()?;

    for (idx, expected) in data.chunks(KERNEL_PAGE_SIZE).enumerate() {
        let start = idx * KERNEL_PAGE_SIZE;
        let end = start + expected.len();

        let range = start as u64..end as u64;
        pipeline.schedule::<Sequential>((idx, expected), &file, range, KERNEL_PAGE_SIZE)?;
    }

    let mut count = 0;
    while let Some(((idx, expected), bytes)) = pipeline.wait()? {
        assert_eq!(
            bytes.as_ref(),
            expected,
            "O_DIRECT pipeline block {idx} mismatch",
        );

        count += 1;
    }

    let num_blocks = data.len().div_ceil(KERNEL_PAGE_SIZE);
    assert_eq!(count, num_blocks);

    Ok(())
}
