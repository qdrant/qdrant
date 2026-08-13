use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard};

use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;

/// Simple non-invasive permits to use GPU devices.
pub struct GpuDevicesMaganer {
    devices: Vec<GpuDeviceSlot>,
    device_names: Vec<String>,
    wait_free: bool,
}

/// One GPU device slot, plus everything needed to recreate its `gpu::Device` in place if it
/// ever reports `DEVICE_LOST` — see `LockedGpuDevice::recreate_after_device_lost()`.
struct GpuDeviceSlot {
    device: Mutex<Arc<gpu::Device>>,
    instance: Arc<gpu::Instance>,
    physical_device: gpu::PhysicalDevice,
    queue_index: usize,
}

pub struct LockedGpuDevice<'a> {
    locked_device: MutexGuard<'a, Arc<gpu::Device>>,
    // Borrows the owning GpuDeviceSlot directly instead of cloning its instance/physical_device/
    // queue_index into a separate struct on every single lock acquisition — a reviewer correctly
    // flagged the clone-per-lock cost plus the field-duplication drift risk in an earlier version
    // of this patch. A plain shared reference coexists fine with the MutexGuard above: both are
    // non-exclusive borrows of (parts of) the same GpuDeviceSlot, and Mutex::lock() only needs
    // &self to produce the guard, so there's no aliasing conflict. `None` for devices locked via
    // the bare `LockedGpuDevice::new()` constructor (used directly by tests, bypassing
    // `GpuDevicesMaganer`) — those simply can't self-heal, same as before this patch.
    slot: Option<&'a GpuDeviceSlot>,
}

impl<'a> LockedGpuDevice<'a> {
    pub fn new(locked_device: MutexGuard<'a, Arc<gpu::Device>>) -> Self {
        Self {
            locked_device,
            slot: None,
        }
    }

    fn new_with_slot(
        locked_device: MutexGuard<'a, Arc<gpu::Device>>,
        slot: &'a GpuDeviceSlot,
    ) -> Self {
        Self {
            locked_device,
            slot: Some(slot),
        }
    }

    pub fn device(&self) -> Arc<gpu::Device> {
        self.locked_device.clone()
    }

    /// Call after any GPU operation using `self.device()` fails, passing the resulting error.
    /// If (and only if) the error indicates the underlying Vulkan device is now permanently
    /// lost, triggers `recreate_after_device_lost()`. No-op for any other error (timeout, OOM,
    /// etc) — those are already handled correctly by the existing fall-back-to-CPU-for-this-
    /// build behavior and don't imply the device itself is unusable going forward.
    ///
    /// Returns whether THIS error was a DEVICE_LOST (regardless of whether the recreation
    /// itself then succeeded or failed) — callers use this to discard any other GPU resource
    /// still bound to the now-dead device (e.g. `GpuVectorStorage`/`GpuInsertContext` built
    /// earlier in the same segment build), rather than reusing it and failing again against
    /// stale buffers. Confirmed live 2026-08-13 (CodeRabbit review, PR #10213): before this
    /// return value existed, `hnsw/build.rs` had no way to know a DEVICE_LOST happened during
    /// the main-graph GPU dispatch, and kept reusing that same (now stale) `gpu_vectors` for
    /// the additional-links dispatch right after — a second guaranteed failure plus another
    /// pointless recreation cycle, and the dead device's VRAM stayed allocated until that
    /// stale `Arc` eventually dropped.
    ///
    /// Takes `&mut self`: recreation replaces the `Arc<Device>` behind this guard in place.
    /// Callers thread `Option<&mut LockedGpuDevice>` through instead of `Option<&LockedGpuDevice>`
    /// (see `VectorIndexBuildArgs::gpu_device`) — a plain, ordinary Rust mutability requirement,
    /// not RefCell/interior-mutability, since this guard is only ever used by one thread at a
    /// time (whichever thread is building a given segment) anyway.
    pub fn recreate_if_device_lost(&mut self, err: &crate::common::operation_error::OperationError) -> bool {
        // By the time a GPU failure reaches call sites like `create_gpu_vectors()`, it has
        // already been converted from `gpu::GpuError` into `OperationError` (see
        // `impl From<gpu::GpuError> for OperationError` in `common/operation_error.rs`), which
        // has no dedicated DEVICE_LOST variant of its own either — `gpu::GpuError` itself
        // folds every Vulkan error into a catch-all `Other(String)` carrying the raw
        // stringified Vulkan result code, and that string survives into `OperationError`'s
        // Display (confirmed against real production log lines: "Service runtime error: GPU
        // error: Other(\"Vulkan API error: ERROR_DEVICE_LOST\")"). This string match is the
        // only signal currently available; giving DEVICE_LOST (and other Vulkan result codes
        // worth distinguishing) a proper enum variant in the `gpu` crate would be a cleaner
        // upstream follow-up, but is out of scope for this fix.
        if err.to_string().contains("DEVICE_LOST") {
            self.recreate_after_device_lost();
            true
        } else {
            false
        }
    }

    /// Recreates the underlying Vulkan device in place, replacing the device this guard is
    /// holding.
    ///
    /// Per the Vulkan spec, once a logical device reports `VK_ERROR_DEVICE_LOST`, that device
    /// is permanently unusable — the only valid recovery is destroying and recreating it.
    /// Before this fix, nothing in qdrant did that: `GpuDevicesMaganer` creates every
    /// `gpu::Device` exactly once at process startup and, on any GPU error including
    /// DEVICE_LOST, `create_gpu_vectors()` (`hnsw/gpu_build.rs`) only logged it and fell back
    /// to CPU *for that one build* — the same dead `Arc<gpu::Device>` went right back into the
    /// pool for the next caller. Confirmed live in production (kvark.rcp, 2026-08-07 14:12:43
    /// through 2026-08-10 14:50:42, one continuous qdrant process, zero restarts): 129
    /// DEVICE_LOST occurrences over 3+ days, and *zero* successful GPU builds logged anywhere
    /// in between — every single build silently ran CPU-only from the first loss onward, with
    /// nothing indicating this in collection/optimizer status (both reported healthy — CPU
    /// fallback working correctly is not the same as GPU actually being used).
    ///
    /// On success, the fresh device is swapped in and the very next `lock_device()` caller
    /// gets it — GPU indexing resumes without a qdrant restart. On failure (e.g. the GPU is
    /// genuinely gone, or the driver reset didn't complete), logs and leaves the dead device in
    /// place — identical to today's existing behavior, so this can never make things worse than
    /// before the fix, only better.
    pub fn recreate_after_device_lost(&mut self) {
        let Some(slot) = self.slot else {
            log::debug!(
                "GPU device lost, but this LockedGpuDevice has no recreate info (constructed \
                 directly, bypassing GpuDevicesMaganer — e.g. in tests). Cannot self-heal; \
                 falling back to CPU only, same as before this fix."
            );
            return;
        };
        match gpu::Device::new_with_params(
            slot.instance.clone(),
            &slot.physical_device,
            slot.queue_index,
            false,
        ) {
            Ok(new_device) => {
                log::warn!(
                    "GPU device {:?} reported DEVICE_LOST; reinitialized a fresh Vulkan device \
                     in its place so subsequent builds can use GPU again.",
                    slot.physical_device.name,
                );
                *self.locked_device = new_device;
            }
            Err(err) => {
                log::error!(
                    "GPU device {:?} reported DEVICE_LOST and could not be reinitialized: \
                     {err:?}. Falling back to CPU until qdrant is restarted.",
                    slot.physical_device.name,
                );
            }
        }
    }
}

impl GpuDevicesMaganer {
    pub fn new(
        filter: &str,
        device_indexes: Option<&[usize]>,
        allow_integrated: bool,
        allow_emulated: bool,
        wait_free: bool,
        parallel_indexes: usize,
    ) -> OperationResult<Self> {
        let instance = gpu::Instance::builder().build()?;

        // Device filter is case-insensitive and comma-separated.
        let filter = filter.to_lowercase();
        let filter = filter
            .split(",")
            .map(|s| s.trim().to_owned())
            .collect::<Vec<_>>();

        // Collect physical devices that match the filter.
        let filtered_physical_devices = instance
            .physical_devices()
            .iter()
            // Apply device name filter.
            .filter(|device| {
                let device_name = device.name.to_lowercase();
                filter.iter().any(|filter| device_name.contains(filter))
            })
            // Filter out integrated and emulated devices.
            .filter(|device| {
                device.device_type == gpu::PhysicalDeviceType::Discrete
                    || (allow_integrated
                        && device.device_type == gpu::PhysicalDeviceType::Integrated)
                    || (allow_emulated && device.device_type == gpu::PhysicalDeviceType::Other)
            })
            .collect::<Vec<_>>();

        // Collect device indexes to use.
        let device_indexes: Vec<_> = if let Some(device_indexes) = device_indexes {
            device_indexes.iter().copied().unique().collect()
        } else {
            (0..filtered_physical_devices.len()).collect()
        };

        let mut devices = Vec::new();
        for queue_index in 0..parallel_indexes {
            devices.extend(
                device_indexes
                    .iter()
                    // Get vk physical device. Filter out invalid device indexes.
                    .filter_map(|&device_index| filtered_physical_devices.get(device_index))
                    // Try to create a gpu device.
                    .filter_map(|physical_device| {
                        match gpu::Device::new_with_params(
                            instance.clone(),
                            physical_device,
                            queue_index,
                            false,
                        ) {
                            Ok(device) => {
                                log::info!("Initialized GPU device: {:?}", physical_device.name);
                                Some(GpuDeviceSlot {
                                    device: Mutex::new(device),
                                    instance: instance.clone(),
                                    physical_device: (*physical_device).clone(),
                                    queue_index,
                                })
                            }
                            Err(err) => {
                                log::error!(
                                    "Failed to create GPU device: {:?}, error: {:?}",
                                    physical_device.name,
                                    err
                                );
                                None
                            }
                        }
                    }),
            );
        }

        // All found devices to include it to the telemetry.
        let device_names = instance
            .physical_devices()
            .iter()
            .map(|device| device.name.clone())
            .collect();

        Ok(Self {
            devices,
            device_names,
            wait_free,
        })
    }

    /// Acquires a free GPU device, waiting (polling every 100ms) if none is immediately
    /// available and `wait_free` is set. Bounded by `super::GPU_LOCK_TIMEOUT` — was previously
    /// an unconditional `loop`, no timeout at all, with no exit besides successfully acquiring a
    /// device or the whole operation being externally cancelled (`stopped`).
    ///
    /// That mattered because of a real, confirmed production failure mode (2026-08-05/06,
    /// silent multi-hour optimizer stall under concurrent CUDA load — no error, no log line,
    /// only recoverable via a full qdrant restart): if the thread CURRENTLY holding a device's
    /// `Mutex` guard gets stuck inside a lower-level driver call that never returns — not a
    /// clean Vulkan error/`DEVICE_LOST` (those already propagate normally, the guard drops via
    /// normal Rust scope-exit, and the next caller acquires the device fine) but a genuine hang
    /// inside the driver itself (observed directly: a thread stuck in `poll()` inside
    /// `libnvidia-eglcore.so`, 0% GPU utilization, never returning) — the mutex is held forever.
    /// Every OTHER task calling `lock_device()` then spins in this loop indefinitely, since
    /// there is no way to detect or break a lock held by a permanently-stuck thread from here.
    ///
    /// Falling back to CPU after a bounded wait doesn't recover the stuck device (that
    /// underlying thread is still wedged, and Rust cannot forcibly reclaim a mutex held by a
    /// hung thread) — but it stops that one stuck device from taking down every other build
    /// task with it, and logs the failure so it's actually diagnosable instead of silent.
    ///
    /// Note this is a *different* failure mode from `DEVICE_LOST` (see
    /// `LockedGpuDevice::recreate_after_device_lost()`): a clean `DEVICE_LOST` error always
    /// releases this mutex normally (the guard drops via ordinary scope-exit), so the next
    /// caller acquires it here just fine — the bug was never here for that case, it was that
    /// the acquired device stayed permanently dead until this patch.
    pub fn lock_device(
        &self,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<LockedGpuDevice<'_>>> {
        if self.devices.is_empty() {
            return Ok(None);
        }
        let wait_start = std::time::Instant::now();
        loop {
            for slot in &self.devices {
                if let Some(guard) = slot.device.try_lock() {
                    return Ok(Some(LockedGpuDevice::new_with_slot(guard, slot)));
                }
            }

            if !self.wait_free {
                return Ok(None);
            }

            check_stopped(stopped)?;

            if wait_start.elapsed() > super::GPU_LOCK_TIMEOUT {
                // warn, not error: hitting this bound means either genuine contention (a build
                // legitimately holding the device for a long dispatch, expected under real
                // parallel load) or a wedged holder — this function has no way to tell the two
                // apart, and the graceful CPU fallback below handles both identically, so this
                // isn't itself a failure worth an error-level log.
                log::warn!(
                    "Timed out after {:?} waiting for a free GPU device ({} device(s) still \
                     busy). Falling back to CPU for this build.",
                    wait_start.elapsed(),
                    self.devices.len(),
                );
                return Ok(None);
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// Returns all found device names without filtering.
    pub fn all_found_device_names(&self) -> Vec<String> {
        self.device_names.clone()
    }
}
