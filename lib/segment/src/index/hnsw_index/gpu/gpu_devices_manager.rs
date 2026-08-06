use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard};

use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;

/// Simple non-invasive permits to use GPU devices.
pub struct GpuDevicesMaganer {
    devices: Vec<Mutex<Arc<gpu::Device>>>,
    device_names: Vec<String>,
    wait_free: bool,
}

pub struct LockedGpuDevice<'a> {
    locked_device: MutexGuard<'a, Arc<gpu::Device>>,
}

impl<'a> LockedGpuDevice<'a> {
    pub fn new(locked_device: MutexGuard<'a, Arc<gpu::Device>>) -> Self {
        Self { locked_device }
    }

    pub fn device(&self) -> Arc<gpu::Device> {
        self.locked_device.clone()
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
                                Some(Mutex::new(device))
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
    pub fn lock_device(
        &self,
        stopped: &AtomicBool,
    ) -> OperationResult<Option<LockedGpuDevice<'_>>> {
        if self.devices.is_empty() {
            return Ok(None);
        }
        let wait_start = std::time::Instant::now();
        loop {
            for device in &self.devices {
                if let Some(guard) = device.try_lock() {
                    return Ok(Some(LockedGpuDevice::new(guard)));
                }
            }

            if !self.wait_free {
                return Ok(None);
            }

            check_stopped(stopped)?;

            if wait_start.elapsed() > super::GPU_LOCK_TIMEOUT {
                log::error!(
                    "Timed out after {:?} waiting for a free GPU device (all {} device(s) \
                     still busy — possibly one is permanently stuck, see lock_device()'s doc \
                     comment). Falling back to CPU for this build.",
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
