use std::sync::Arc;

use zerocopy::{FromBytes, Immutable, IntoBytes};

use super::device::CudaDevice;
use crate::vulkan::BufferType;
use crate::{GpuError, GpuResult};

/// A GPU memory buffer backed by a CUDA/HIP device pointer.
///
/// For staging buffers (CpuToGpu / GpuToCpu), pinned mapped host memory is
/// used so that `upload`/`download` are plain CPU memcpy (no driver calls)
/// and the GPU can access the same memory via its device pointer.
pub struct CudaBuffer {
    device: Arc<CudaDevice>,

    /// Device pointer (cuMemAlloc for Storage/Uniform, mapped pinned for staging).
    pub(super) ptr: u64,

    /// Buffer size in bytes.
    size: usize,

    buffer_type: BufferType,

    /// Pinned host pointer for staging buffers (CpuToGpu / GpuToCpu).
    /// CPU reads/writes go through this pointer; the GPU accesses via `ptr`.
    host_ptr: *mut u8,
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
                (ptr, std::ptr::null_mut())
            }
            BufferType::CpuToGpu | BufferType::GpuToCpu => {
                // Use pinned mapped host memory: CPU writes to host_ptr,
                // GPU reads from ptr (same physical memory, zero-copy).
                let (host, dev) = device.driver().alloc_host_mapped(size)?;
                // Zero-initialize.
                unsafe { std::ptr::write_bytes(host as *mut u8, 0, size) };
                (dev, host as *mut u8)
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

    /// The pinned host pointer for staging buffers, null for device buffers.
    pub(super) fn host_ptr(&self) -> *mut u8 {
        self.host_ptr
    }

    /// Upload data from host to this buffer (for CpuToGpu buffers).
    /// With pinned mapped memory this is a plain CPU memcpy — no driver call.
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
        debug_assert!(!self.host_ptr.is_null());
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.host_ptr.add(offset), bytes.len());
        }
        Ok(())
    }

    /// Download data from this buffer to host.
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
        if !self.host_ptr.is_null() {
            // Staging buffer: read from pinned mapped memory (no driver call).
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.host_ptr.add(offset),
                    bytes.as_mut_ptr(),
                    bytes.len(),
                );
            }
            Ok(())
        } else {
            // Device buffer: sync copy from device.
            self.device.make_current()?;
            self.device.driver().memcpy_dtoh(
                bytes.as_mut_ptr() as *mut std::ffi::c_void,
                self.ptr + offset as u64,
                bytes.len(),
            )
        }
    }

    /// Download a `Vec<T>` of `len` elements.
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
        if !self.host_ptr.is_null() {
            // Staging buffer: read from pinned mapped memory.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.host_ptr.add(offset),
                    result.as_mut_ptr() as *mut u8,
                    byte_len,
                );
            }
        } else {
            // Device buffer: sync copy from device.
            self.device.make_current()?;
            self.device.driver().memcpy_dtoh(
                result.as_mut_ptr() as *mut std::ffi::c_void,
                self.ptr + offset as u64,
                byte_len,
            )?;
        }
        Ok(result)
    }
}

impl Drop for CudaBuffer {
    fn drop(&mut self) {
        if !self.host_ptr.is_null() {
            // Pinned mapped memory — free with host free.
            self.device
                .driver()
                .free_host(self.host_ptr as *mut std::ffi::c_void);
            self.host_ptr = std::ptr::null_mut();
            self.ptr = 0;
        } else if self.ptr != 0 {
            self.device.driver().free(self.ptr);
            self.ptr = 0;
        }
    }
}

// CudaBuffer contains a raw pointer but is accessed single-threadedly via Arc.
unsafe impl Send for CudaBuffer {}
unsafe impl Sync for CudaBuffer {}
