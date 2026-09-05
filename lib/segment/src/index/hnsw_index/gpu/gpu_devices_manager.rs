use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard};
use rand::RngExt;

use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;

/// Simple non-invasive permits to use GPU devices.
pub struct GpuDevicesMaganer {
    devices: Vec<GpuDeviceSlot>,
    device_names: Vec<String>,
    wait_free: bool,
}

const RECREATE_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const RECREATE_BACKOFF_MAX: Duration = Duration::from_secs(60);
const RECREATE_BACKOFF_MULTIPLIER: u32 = 2;

/// Per-slot exponential backoff (with full jitter) on `recreate_after_device_lost()` attempts.
/// A permanently-dead GPU means every future segment build hits DEVICE_LOST again and retries
/// recreation — under concurrent/sustained indexing that's unbounded, repeated device-creation
/// calls against already-faulted hardware with no cooldown. Jitter matters here specifically:
/// `parallel_indexes > 1` creates multiple independent `GpuDeviceSlot`s backing the SAME
/// physical device, each with its own backoff state — without jitter, slots that hit
/// DEVICE_LOST around the same moment stay roughly synchronized through every doubling step,
/// periodically bursting simultaneous recreation attempts against the same dead hardware
/// instead of spreading them out. `current_backoff` tracks the deterministic ceiling (for the
/// next doubling); the actual wait is a random point within `[0, current_backoff]`.
struct RecreateBackoff {
    next_allowed_attempt: Instant,
    current_backoff: Duration,
}

impl Default for RecreateBackoff {
    fn default() -> Self {
        Self {
            next_allowed_attempt: Instant::now(),
            current_backoff: Duration::ZERO,
        }
    }
}

/// One GPU device slot, plus everything needed to recreate its `gpu::Device` in place if it
/// ever reports `DEVICE_LOST` — see `LockedGpuDevice::recreate_after_device_lost()`.
struct GpuDeviceSlot {
    device: Mutex<Arc<gpu::Device>>,
    instance: Arc<gpu::Instance>,
    physical_device: gpu::PhysicalDevice,
    queue_index: usize,
    recreate_backoff: Mutex<RecreateBackoff>,
}

pub struct LockedGpuDevice<'a> {
    locked_device: MutexGuard<'a, Arc<gpu::Device>>,
    // Reference into the owning GpuDeviceSlot (avoids cloning instance/physical_device/
    // queue_index per lock acquisition). Coexists fine with the MutexGuard above: both are
    // non-exclusive borrows. `None` for devices locked via the bare `LockedGpuDevice::new()`
    // constructor (used by tests) — those can't self-heal.
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

    /// Call after a GPU operation using `self.device()` fails. Recreates the device if `err`
    /// indicates it's permanently lost; a no-op for any other error (timeout, OOM, ...), which
    /// the existing fall-back-to-CPU-for-this-build behavior already handles fine.
    ///
    /// Returns whether this was a DEVICE_LOST, so callers can discard any other GPU resource
    /// still bound to the now-dead device (e.g. a `GpuVectorStorage`/`GpuInsertContext` built
    /// earlier in the same segment build) instead of reusing it and failing again.
    ///
    /// Takes `&mut self` since recreation replaces the `Arc<Device>` in place; callers thread
    /// `Option<&mut LockedGpuDevice>` through accordingly (see `VectorIndexBuildArgs::gpu_device`).
    pub fn recreate_if_device_lost(
        &mut self,
        err: &crate::common::operation_error::OperationError,
    ) -> bool {
        // OperationError has no dedicated DEVICE_LOST variant — `gpu::GpuError` folds every
        // Vulkan error into a stringified `Other(...)`, so string matching is the only signal
        // available today. A proper enum variant in the `gpu` crate would be cleaner.
        if err.to_string().contains("DEVICE_LOST") {
            self.recreate_after_device_lost();
            true
        } else {
            false
        }
    }

    /// Recreates the underlying Vulkan device in place, replacing the device this guard holds.
    ///
    /// Per the Vulkan spec, a device reporting `VK_ERROR_DEVICE_LOST` is permanently
    /// unusable — the only recovery is destroying and recreating it. Before this fix qdrant
    /// never did that: the same dead device just went back into the pool for the next caller,
    /// silently pinning all GPU indexing to CPU fallback until a full restart.
    ///
    /// On success, the fresh device is swapped in and the next `lock_device()` caller gets it —
    /// GPU indexing resumes without a restart. On failure, logs and leaves the dead device in
    /// place, same as before this fix.
    ///
    /// Only replaces the `Arc<gpu::Device>` held by the shared slot — an `Arc` clone a caller
    /// already obtained from `device()` earlier still points at the lost device until it drops.
    /// Callers holding other GPU resources across this call must discard them themselves
    /// instead of reusing them (see this method's return value and its callers).
    pub fn recreate_after_device_lost(&mut self) {
        let Some(slot) = self.slot else {
            log::debug!(
                "GPU device lost, but this LockedGpuDevice has no recreate info (constructed \
                 directly, bypassing GpuDevicesMaganer — e.g. in tests). Cannot self-heal; \
                 falling back to CPU only, same as before this fix."
            );
            return;
        };

        let now = Instant::now();
        {
            let backoff = slot.recreate_backoff.lock();
            if now < backoff.next_allowed_attempt {
                log::debug!(
                    "GPU device {:?} reported DEVICE_LOST but is still within its recreation \
                     backoff window ({:?} remaining) — falling back to CPU without retrying.",
                    slot.physical_device.name,
                    backoff.next_allowed_attempt - now,
                );
                return;
            }
        } // released before the (potentially slow) device-creation call below

        match gpu::Device::new_with_params(
            slot.instance.clone(),
            &slot.physical_device,
            slot.queue_index,
            false,
        ) {
            Ok(new_device) => {
                *slot.recreate_backoff.lock() = RecreateBackoff::default();
                log::warn!(
                    "GPU device {:?} reported DEVICE_LOST; reinitialized a fresh Vulkan device \
                     in its place so subsequent builds can use GPU again.",
                    slot.physical_device.name,
                );
                *self.locked_device = new_device;
            }
            Err(err) => {
                let jittered_wait = {
                    let mut backoff = slot.recreate_backoff.lock();
                    let ceiling = (backoff.current_backoff.max(RECREATE_BACKOFF_INITIAL)
                        * RECREATE_BACKOFF_MULTIPLIER)
                        .min(RECREATE_BACKOFF_MAX);
                    backoff.current_backoff = ceiling;
                    let jittered = Duration::from_secs_f64(
                        rand::rng().random_range(0.0..=ceiling.as_secs_f64()),
                    );
                    backoff.next_allowed_attempt = now + jittered;
                    jittered
                };
                log::error!(
                    "GPU device {:?} reported DEVICE_LOST and could not be reinitialized: \
                     {err:?}. Falling back to CPU; next recreation attempt in {jittered_wait:?}.",
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
                                    recreate_backoff: Mutex::new(RecreateBackoff::default()),
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
    /// an unconditional loop with no exit besides success or external cancellation (`stopped`).
    ///
    /// A device whose holder is stuck in a lower-level driver call that never returns (as
    /// opposed to a clean Vulkan error, which releases the mutex normally via scope-exit) would
    /// otherwise hold this lock forever, hanging every other caller with no way to detect or
    /// break it from here. Falling back to CPU after the bound doesn't recover the stuck
    /// device, but stops it from taking every other build down with it, and makes the failure
    /// diagnosable instead of silent.
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
