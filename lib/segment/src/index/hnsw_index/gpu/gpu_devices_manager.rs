use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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
        let instance = gpu::Instance::new(None, None, false)?;

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
                        match gpu::Device::new_with_queue_index(
                            instance.clone(),
                            physical_device,
                            queue_index,
                        ) {
                            Ok(device) => {
                                log::info!("Initialized GPU device: {:?}", &physical_device.name);
                                Some(Mutex::new(device))
                            }
                            Err(err) => {
                                log::error!(
                                    "Failed to create GPU device: {:?}, error: {:?}",
                                    &physical_device.name,
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

    pub fn lock_device(&self, stopped: &AtomicBool) -> OperationResult<Option<LockedGpuDevice>> {
        if self.devices.is_empty() {
            return Ok(None);
        }
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
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// Returns all found device names without filtering.
    pub fn all_found_device_names(&self) -> Vec<String> {
        self.device_names.clone()
    }
}
