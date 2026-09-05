//! Diagnostic test (`#[ignore]`d, run explicitly) probing whether a large/failed GPU
//! vector-storage allocation leaves `GpuDevicesMaganer`'s device lock unusable for subsequent
//! callers. Uses the real `GpuDevicesMaganer`/`GpuVectorStorage` code paths, not reinvented
//! Vulkan primitives.

use std::sync::atomic::AtomicBool;
use std::time::Instant;

use common::counter::hardware_counter::HardwareCounterCell;

use super::gpu_devices_manager::GpuDevicesMaganer;
use super::gpu_vector_storage::GpuVectorStorage;
use crate::data_types::vectors::VectorRef;
use crate::types::Distance;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

/// Builds a dense f32 vector storage of approximately `target_gb` GB total size. Many small
/// vectors (8192 floats each) rather than a few huge ones — `VolatileChunkedVectors::new()`
/// hard-asserts a single vector's byte size must fit under `CHUNK_SIZE` (≤32MB depending on
/// build config). Content is irrelevant, only the total size (to force a real GPU allocation).
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

/// GPU vendor filter for these diagnostics, e.g. "nvidia" or "amd".
fn gpu_filter() -> String {
    std::env::var("QDRANT_GPU_DIAGNOSTIC_FILTER").unwrap_or_else(|_| "nvidia".to_owned())
}

/// `build_storage_of_size()` allocates its full target size in host memory first (a real
/// `VectorStorageEnum`, before ever touching the GPU) — caps the request below available RAM
/// (via `/proc/meminfo`) to avoid the OOM killer ending the diagnostic before it reaches the
/// GPU. Falls back to the preferred size if `/proc/meminfo` can't be read.
fn oversized_target_gb(preferred_gb: f64, headroom_fraction: f64) -> f64 {
    let available_gb = fs_err::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("MemAvailable:")
                    .and_then(|rest| rest.split_whitespace().next())
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

    // Sizes chosen to straddle Qdrant's documented ~16GB-per-segment GPU limit — some should
    // succeed, some should fail; the interesting question is what relocking looks like after
    // each, not just whether the allocation itself succeeds.
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
    eprintln!(
        "\n=== diagnostic complete — see above for any '*** SLOW/STUCK RELOCK ***' lines ==="
    );
}

/// Companion test: spawns several threads hammering lock_device()/GpuVectorStorage
/// concurrently, one of them deliberately attempting an oversized allocation. If a single
/// stuck/oversized attempt can wedge the shared device mutex, the other threads' lock_device()
/// calls should show it as unusually long waits, not just the one that failed.
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
