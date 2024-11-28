use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;

/// Simple non-invasive permits to use GPU devices.
pub struct GpuDevicesMaganer {
    devices: Vec<Mutex<Arc<gpu::Device>>>,
    wait_free: bool,
}

pub struct LockedGpuDevice<'a> {
    locked_device: MutexGuard<'a, Arc<gpu::Device>>,
}

impl LockedGpuDevice<'_> {
    pub fn device(&self) -> Arc<gpu::Device> {
        self.locked_device.clone()
    }
}

impl GpuDevicesMaganer {
    pub fn new(
        filter: &str,
        start_index: usize,
        count: usize,
        allow_integrated: bool,
        allow_emulated: bool,
        wait_free: bool,
        parallel_indexes: usize,
    ) -> OperationResult<Self> {
        let instance = gpu::Instance::new(None, None, false)?;
        let filter = filter.to_lowercase();
        let filter = filter
            .split(",")
            .map(|s| s.trim().to_owned())
            .collect::<Vec<_>>();
        let mut devices = Vec::new();
        for queue_index in 0..parallel_indexes {
            devices.extend(
                instance
                    .physical_devices()
                    .iter()
                    .filter(|device| {
                        let device_name = device.name.to_lowercase();
                        filter.iter().any(|filter| device_name.contains(filter))
                    })
                    .filter(|device| {
                        device.device_type == gpu::PhysicalDeviceType::Discrete
                            || (allow_integrated
                                && device.device_type == gpu::PhysicalDeviceType::Integrated)
                            || (allow_emulated
                                && device.device_type == gpu::PhysicalDeviceType::Other)
                    })
                    .skip(start_index)
                    .take(count)
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
        Ok(Self { devices, wait_free })
    }

    pub fn lock_device(&self, stopped: &AtomicBool) -> OperationResult<Option<LockedGpuDevice>> {
        if self.devices.is_empty() {
            return Ok(None);
        }
        loop {
            for device in &self.devices {
                if let Some(guard) = device.try_lock() {
                    return Ok(Some(LockedGpuDevice {
                        locked_device: guard,
                    }));
                }
            }

            if !self.wait_free {
                return Ok(None);
            }

            check_stopped(stopped)?;
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}
