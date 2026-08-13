//! Diagnostic test (deliberately NOT part of the normal `cargo test` sweep — `#[ignore]`d, run
//! explicitly) probing whether a large/failed GPU vector-storage allocation leaves
//! `GpuDevicesMaganer`'s device lock unusable for subsequent callers. Uses the REAL production
//! code paths (`GpuDevicesMaganer`, `GpuVectorStorage`) — no reinvented Vulkan primitives — so
//! this exercises exactly what `/index/page`'s HNSW GPU build path does in production.
//!
//! Context (see project_qdrant_gpu_indexing_xid109 memory, 2026-08-05/06 investigation): a
//! multi-hour silent optimizer stall was traced to `gpu_devices_manager.rs`'s `lock_device()`
//! having NO timeout at all — a bare `try_lock()` + `sleep(100ms)` retry loop, forever, if no
//! device ever frees up. The open question this diagnostic targets: does a failed/oversized
//! allocation (VRAM exhaustion) or contention while a device is locked leave that device
//! permanently unable to be re-locked, even after the failing call returns?

use std::sync::atomic::AtomicBool;
use std::time::Instant;

use common::counter::hardware_counter::HardwareCounterCell;

use super::gpu_devices_manager::GpuDevicesMaganer;
use super::gpu_vector_storage::GpuVectorStorage;
use crate::data_types::vectors::VectorRef;
use crate::types::Distance;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

/// Builds a dense f32 vector storage of approximately `target_gb` GB total size. Fixed, small
/// per-vector dim (8192 floats = 32KB/vector) — `VolatileChunkedVectors::new()` hard-asserts a
/// single vector's byte size must fit under `CHUNK_SIZE` (≤32MB depending on build config;
/// confirmed live: a few-huge-vectors approach with ~512MB/vector panicked immediately, before
/// ever touching the GPU). Many small vectors instead — content is irrelevant here, only the
/// total size (to force a real GPU allocation of that size).
fn build_storage_of_size(target_gb: f64) -> VectorStorageEnum {
    const DIM: usize = 8192;
    let total_floats = (target_gb * 1024.0 * 1024.0 * 1024.0 / 4.0) as usize;
    let num_vectors = (total_floats / DIM).max(1);
    let mut storage = new_volatile_dense_vector_storage(DIM, Distance::Cosine);
    let filler = vec![0.001f32; DIM];
    let hw_counter = HardwareCounterCell::new();
    for key in 0..num_vectors as u32 {
        storage
            .insert_vector(key, VectorRef::from(filler.as_slice()), &hw_counter)
            .unwrap();
    }
    storage
}

/// GPU vendor filter for these diagnostics, e.g. "nvidia" or "amd" — was previously hardcoded to
/// "nvidia" (fine on our own host, but the diagnostic couldn't run at all on a different vendor,
/// and would panic with a message pointing at GPU visibility rather than at the filter itself).
fn gpu_filter() -> String {
    std::env::var("QDRANT_GPU_DIAGNOSTIC_FILTER").unwrap_or_else(|_| "nvidia".to_owned())
}

/// This test's own oversized allocation is meant to exceed VRAM, not host RAM — but
/// `build_storage_of_size()` allocates the full target size in host memory first (it builds a
/// real `VectorStorageEnum` before ever touching the GPU). Reading `/proc/meminfo`'s
/// `MemAvailable` and capping the request below it (leaving real headroom for the OTHER threads'
/// concurrent allocations too) avoids the host OOM killer ending the diagnostic before it ever
/// reaches the GPU, on a host with less RAM than ours. Falls back to the original hardcoded size
/// if `/proc/meminfo` can't be read/parsed (non-Linux, or a sandboxed environment without it) —
/// same behavior as before this fix in that case, not a regression.
fn oversized_target_gb(preferred_gb: f64, headroom_fraction: f64) -> f64 {
    let available_gb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("MemAvailable:")
                    .and_then(|rest| rest.trim().split_whitespace().next())
                    .and_then(|kb| kb.parse::<f64>().ok())
                    .map(|kb| kb / (1024.0 * 1024.0))
            })
        });
    match available_gb {
        Some(available_gb) => preferred_gb.min(available_gb * headroom_fraction),
        None => preferred_gb,
    }
}

/// Run explicitly with:
///   cargo test --features gpu -p segment relock_after_pressure_diagnostic -- --ignored --nocapture
#[test]
#[ignore]
fn relock_after_pressure_diagnostic() {
    let stopped = AtomicBool::new(false);
    let manager = GpuDevicesMaganer::new(&gpu_filter(), None, false, false, true, 1)
        .expect("failed to init GpuDevicesMaganer — is a GPU actually visible on this host?");

    // Sizes chosen to straddle Qdrant's own documented ~16GB-per-segment-per-indexing-iteration
    // GPU limit (see running-with-gpu docs) — some should succeed cleanly, some should fail,
    // the interesting question is what RELOCKING looks like after each, not just whether the
    // allocation itself succeeds.
    for target_gb in [4.0_f64, 8.0, 16.0] {
        eprintln!("\n=== target size: {target_gb} GB ===");
        eprintln!("  building host-side vector storage ({target_gb} GB)...");
        let build_start = Instant::now();
        let storage = build_storage_of_size(target_gb);
        eprintln!("  built in {:?}", build_start.elapsed());

        eprintln!("  lock_device()...");
        let lock_start = Instant::now();
        let locked = manager.lock_device(&stopped);
        eprintln!(
            "  lock_device() -> ok={:?} (took {:?})",
            locked.is_ok(),
            lock_start.elapsed()
        );

        if let Ok(Some(locked_device)) = &locked {
            eprintln!("  GpuVectorStorage::new({target_gb} GB)...");
            let alloc_start = Instant::now();
            let result =
                GpuVectorStorage::new(locked_device.device(), &storage, None, false, &stopped);
            eprintln!(
                "  GpuVectorStorage::new({target_gb}GB) -> {} (took {:?})",
                if result.is_ok() { "OK" } else { "FAILED" },
                alloc_start.elapsed()
            );
            if let Err(e) = &result {
                eprintln!("    error detail: {e:?}");
            }
        } else {
            eprintln!("  (no locked device — skipping allocation attempt this round)");
        }
        // `locked` (and any LockedGpuDevice inside it) drops here, releasing the device lock.
        drop(locked);

        eprintln!("  RELOCK immediately after releasing...");
        let relock_start = Instant::now();
        let relocked = manager.lock_device(&stopped);
        let relock_elapsed = relock_start.elapsed();
        let slow_marker = if relock_elapsed > std::time::Duration::from_secs(5) {
            "  *** SLOW/STUCK RELOCK — this is the failure signature we're hunting ***"
        } else {
            ""
        };
        eprintln!(
            "  relock -> ok={:?} (took {:?}) {}",
            relocked.is_ok(),
            relock_elapsed,
            slow_marker
        );
        drop(relocked);
    }
    eprintln!("\n=== diagnostic complete — see above for any '*** SLOW/STUCK RELOCK ***' lines ===");
}

/// Companion test: instead of one thread doing allocate-then-immediately-relock, spawn several
/// threads hammering lock_device()/GpuVectorStorage concurrently (mirrors DIR_PARALLEL's real
/// fan-out shape more closely than the single-threaded test above) while one of them
/// deliberately attempts an oversized allocation. If a single stuck/oversized attempt can wedge
/// the shared device mutex, the OTHER threads' lock_device() calls should show it as unusually
/// long waits, not just the one that failed.
///
/// Run explicitly with:
///   cargo test --features gpu -p segment relock_under_concurrent_contention_diagnostic -- --ignored --nocapture
#[test]
#[ignore]
fn relock_under_concurrent_contention_diagnostic() {
    use std::sync::Arc;
    use std::thread;

    let stopped = Arc::new(AtomicBool::new(false));
    let manager = Arc::new(
        GpuDevicesMaganer::new(&gpu_filter(), None, false, false, true, 1)
            .expect("failed to init GpuDevicesMaganer — is a GPU actually visible on this host?"),
    );

    let mut handles = Vec::new();

    // Threads 0..2: normal-sized, repeated lock/alloc/release cycles — mimics ordinary
    // concurrent segment builds.
    for thread_id in 0..3 {
        let manager = manager.clone();
        let stopped = stopped.clone();
        handles.push(thread::spawn(move || {
            for round in 0..10 {
                let storage = build_storage_of_size(2.0);
                let lock_start = Instant::now();
                let locked = manager.lock_device(&stopped);
                let lock_elapsed = lock_start.elapsed();
                if let Ok(Some(locked_device)) = &locked {
                    let _ = GpuVectorStorage::new(
                        locked_device.device(),
                        &storage,
                        None,
                        false,
                        &stopped,
                    );
                }
                eprintln!(
                    "[normal thread {thread_id} round {round}] lock_device took {lock_elapsed:?}{}",
                    if lock_elapsed > std::time::Duration::from_secs(5) {
                        " *** SLOW ***"
                    } else {
                        ""
                    }
                );
                drop(locked);
            }
        }));
    }

    // Thread 3: the deliberate troublemaker — one oversized allocation attempt, then normal
    // cycles, to see if IT specifically has trouble recovering even if the others don't.
    {
        let manager = manager.clone();
        let stopped = stopped.clone();
        handles.push(thread::spawn(move || {
            let target_gb = oversized_target_gb(48.0, 0.5);
            eprintln!("[troublemaker] attempting oversized ({target_gb:.1}GB) allocation...");
            let storage = build_storage_of_size(target_gb);
            let lock_start = Instant::now();
            let locked = manager.lock_device(&stopped);
            eprintln!("[troublemaker] lock_device took {:?}", lock_start.elapsed());
            if let Ok(Some(locked_device)) = &locked {
                let alloc_start = Instant::now();
                let result =
                    GpuVectorStorage::new(locked_device.device(), &storage, None, false, &stopped);
                eprintln!(
                    "[troublemaker] {target_gb:.1}GB alloc -> {} (took {:?})",
                    if result.is_ok() { "OK" } else { "FAILED" },
                    alloc_start.elapsed()
                );
            }
            drop(locked);

            for round in 0..5 {
                let storage = build_storage_of_size(2.0);
                let lock_start = Instant::now();
                let locked = manager.lock_device(&stopped);
                let lock_elapsed = lock_start.elapsed();
                if let Ok(Some(locked_device)) = &locked {
                    let _ = GpuVectorStorage::new(
                        locked_device.device(),
                        &storage,
                        None,
                        false,
                        &stopped,
                    );
                }
                eprintln!(
                    "[troublemaker round {round}] lock_device took {lock_elapsed:?}{}",
                    if lock_elapsed > std::time::Duration::from_secs(5) {
                        " *** SLOW ***"
                    } else {
                        ""
                    }
                );
                drop(locked);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    eprintln!("\n=== concurrent contention diagnostic complete ===");
}
