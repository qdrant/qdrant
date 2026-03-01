use std::sync::Arc;

use crate::{GpuError, GpuResult};
use super::device::CudaDevice;
use super::driver::{GpuDriver, Runtime};
use crate::vulkan::PhysicalDeviceType;

/// A physical CUDA/HIP device as seen from the instance level.
/// Holds everything needed to later call `CudaDevice::new`.
#[derive(Clone)]
pub struct CudaPhysicalDevice {
    pub name: String,
    pub device_type: PhysicalDeviceType,
    pub device_index: i32,
    pub(crate) driver: Arc<GpuDriver>,
    pub(crate) runtime: Runtime,
}

/// The CUDA/HIP instance: holds the loaded driver and enumerated devices.
pub struct CudaInstance {
    pub(crate) driver: Arc<GpuDriver>,
    pub(crate) runtime: Runtime,
    physical_devices: Vec<CudaPhysicalDevice>,
}

impl CudaInstance {
    /// Try to initialize the HIP runtime (AMD ROCm).
    pub fn try_hip() -> GpuResult<Arc<Self>> {
        let (driver, runtime) = GpuDriver::try_load_hip()?;
        Self::init(driver, runtime)
    }

    /// Try to initialize the CUDA runtime (NVIDIA).
    pub fn try_cuda() -> GpuResult<Arc<Self>> {
        let (driver, runtime) = GpuDriver::try_load_cuda()?;
        Self::init(driver, runtime)
    }

    fn init(driver: Arc<GpuDriver>, runtime: Runtime) -> GpuResult<Arc<Self>> {
        let cuda_devices = CudaDevice::enumerate(driver.clone(), runtime)?;
        if cuda_devices.is_empty() {
            return Err(GpuError::NotSupported(format!(
                "No {runtime:?} compute devices found"
            )));
        }

        let physical_devices = cuda_devices
            .iter()
            .enumerate()
            .map(|(i, dev)| CudaPhysicalDevice {
                name: dev.name().to_string(),
                device_type: PhysicalDeviceType::Discrete, // assume discrete for Instinct/data-center GPUs
                device_index: i as i32,
                driver: driver.clone(),
                runtime,
            })
            .collect();

        Ok(Arc::new(CudaInstance {
            driver,
            runtime,
            physical_devices,
        }))
    }

    pub fn physical_devices(&self) -> &[CudaPhysicalDevice] {
        &self.physical_devices
    }
}

unsafe impl Send for CudaInstance {}
unsafe impl Sync for CudaInstance {}
