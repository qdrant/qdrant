use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use crate::common::operation_error::OperationResult;

pub struct DevicesMaganer {
    pub devices: Vec<Mutex<Arc<gpu::Device>>>,
    pub wait_free: bool,
}

pub struct LockedDevice<'a> {
    pub locked_device: MutexGuard<'a, Arc<gpu::Device>>,
}

impl DevicesMaganer {
    pub fn new(
        instance: Arc<gpu::Instance>,
        filter: &str,
        start_index: usize,
        count: usize,
        wait_free: bool,
        parallel_indexes: usize,
    ) -> OperationResult<Self> {
        let filter = filter.to_lowercase();
        let mut devices = Vec::new();
        for queue_index in 0..parallel_indexes {
            devices.extend(
                instance
                    .physical_devices()
                    .iter()
                    .filter(|device| {
                        let device_name = device.name.to_lowercase();
                        device_name.contains(&filter)
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

    pub fn lock_device(&self) -> Option<LockedDevice> {
        if self.devices.is_empty() {
            return None;
        }
        loop {
            // TODO(gpu): Add timeout
            for device in &self.devices {
                if let Some(guard) = device.try_lock() {
                    return Some(LockedDevice {
                        locked_device: guard,
                    });
                }
            }

            if !self.wait_free {
                return None;
            }

            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
}
