use std::sync::Arc;

use zerocopy::{FromBytes, Immutable, IntoBytes};

use super::device::CudaDevice;
use crate::vulkan::BufferType;
use crate::{GpuError, GpuResult};

/// A GPU memory buffer backed by a CUDA/HIP device pointer.
pub struct CudaBuffer {
    device: Arc<CudaDevice>,

    /// Device pointer returned by cuMemAlloc / hipMalloc.
    pub(super) ptr: u64,

    /// Buffer size in bytes.
    size: usize,

    buffer_type: BufferType,

    /// For CpuToGpu / GpuToCpu: host-side allocation (system malloc).
    /// We use plain host memory for staging; memcpy is synchronous.
    host_ptr: Option<Vec<u8>>,
}

impl CudaBuffer {
    pub fn new(
        device: Arc<CudaDevice>,
        _name: impl AsRef<str>,
        buffer_type: BufferType,
        size: usize,
    ) -> GpuResult<Arc<Self>> {
        if size == 0 {
            return Err(GpuError::NotSupported(
                "Zero-sized GPU buffers are not supported".to_string(),
            ));
        }

        device.make_current()?;

        let (ptr, host_ptr) = match buffer_type {
            BufferType::Storage | BufferType::Uniform => {
                let ptr = device.driver().alloc(size)?;
                // Zero-initialize to match Vulkan behavior.
                device.driver().memset(ptr, 0, size)?;
                (ptr, None)
            }
            BufferType::CpuToGpu | BufferType::GpuToCpu => {
                // Use a device allocation + a host Vec for staging.
                let ptr = device.driver().alloc(size)?;
                device.driver().memset(ptr, 0, size)?;
                let host_ptr = vec![0u8; size];
                (ptr, Some(host_ptr))
            }
        };

        Ok(Arc::new(CudaBuffer {
            device,
            ptr,
            size,
            buffer_type,
            host_ptr,
        }))
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn buffer_type(&self) -> BufferType {
        self.buffer_type
    }

    /// The raw device pointer — used as BDA address.
    pub fn device_address(&self) -> u64 {
        self.ptr
    }

    /// Upload data from host to this buffer (for CpuToGpu buffers).
    pub fn upload<T>(&self, data: &T, offset: usize) -> GpuResult<()>
    where
        T: IntoBytes + Immutable + ?Sized,
    {
        if self.buffer_type != BufferType::CpuToGpu {
            return Err(GpuError::Other(
                "Upload is only allowed for CpuToGpu buffers".to_string(),
            ));
        }
        let bytes = data.as_bytes();
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| GpuError::OutOfBounds("Overflow in upload offset".to_string()))?;
        if end > self.size {
            return Err(GpuError::OutOfBounds(format!(
                "Upload range {offset}..{end} exceeds buffer size {}",
                self.size
            )));
        }
        self.device.make_current()?;
        self.device.driver().memcpy_htod(
            self.ptr + offset as u64,
            bytes.as_ptr() as *const std::ffi::c_void,
            bytes.len(),
        )
    }

    /// Download data from this buffer to host.
    /// Works for any buffer type on CUDA/HIP (device memory is always readable).
    pub fn download<T>(&self, data: &mut T, offset: usize) -> GpuResult<()>
    where
        T: FromBytes + IntoBytes + ?Sized,
    {
        let bytes = data.as_mut_bytes();
        let end = offset
            .checked_add(bytes.len())
            .ok_or_else(|| GpuError::OutOfBounds("Overflow in download offset".to_string()))?;
        if end > self.size {
            return Err(GpuError::OutOfBounds(format!(
                "Download range {offset}..{end} exceeds buffer size {}",
                self.size
            )));
        }
        self.device.make_current()?;
        self.device.driver().memcpy_dtoh(
            bytes.as_mut_ptr() as *mut std::ffi::c_void,
            self.ptr + offset as u64,
            bytes.len(),
        )
    }

    /// Download a `Vec<T>` of `len` elements (for GpuToCpu buffers).
    pub fn download_vec<T>(&self, offset: usize, len: usize) -> GpuResult<Vec<T>>
    where
        T: FromBytes + IntoBytes + Clone,
    {
        let byte_len = len
            .checked_mul(size_of::<T>())
            .ok_or_else(|| GpuError::OutOfBounds("Overflow in download_vec".to_string()))?;
        let end = offset
            .checked_add(byte_len)
            .ok_or_else(|| GpuError::OutOfBounds("Overflow in download_vec offset".to_string()))?;
        if end > self.size {
            return Err(GpuError::OutOfBounds(format!(
                "Download range {offset}..{end} exceeds buffer size {}",
                self.size
            )));
        }
        let mut result = vec![T::new_zeroed(); len];
        self.device.make_current()?;
        self.device.driver().memcpy_dtoh(
            result.as_mut_ptr() as *mut std::ffi::c_void,
            self.ptr + offset as u64,
            byte_len,
        )?;
        Ok(result)
    }
}

impl Drop for CudaBuffer {
    fn drop(&mut self) {
        if self.ptr != 0 {
            self.device.driver().free(self.ptr);
            self.ptr = 0;
        }
        self.host_ptr = None;
    }
}

// CudaBuffer contains a raw pointer but is accessed single-threadedly via Arc.
unsafe impl Send for CudaBuffer {}
unsafe impl Sync for CudaBuffer {}
