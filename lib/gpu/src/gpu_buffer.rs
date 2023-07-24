use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use ash::vk;
use gpu_allocator::vulkan::{Allocation, AllocationCreateDesc};
use gpu_allocator::MemoryLocation;

use crate::{GpuDevice, GpuResource};

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum GpuBufferType {
    Uniform,
    Storage,
    CpuToGpu,
    GpuToCpu,
}

pub struct GpuBuffer {
    pub device: Arc<GpuDevice>,
    pub vk_buffer: vk::Buffer,
    pub buffer_type: GpuBufferType,
    pub size: usize,
    pub allocation: Allocation,
    pub upload_mapped_ptr: Option<Mutex<NonNull<c_void>>>,
}

unsafe impl Send for GpuBuffer {}
unsafe impl Sync for GpuBuffer {}

impl GpuResource for GpuBuffer {}

impl Drop for GpuBuffer {
    fn drop(&mut self) {
        self.upload_mapped_ptr = None;
        let mut allocation = Allocation::default();
        std::mem::swap(&mut allocation, &mut self.allocation);
        self.device.gpu_free(allocation);
        unsafe {
            self.device
                .vk_device
                .destroy_buffer(self.vk_buffer, self.device.alloc())
        };
        self.size = 0;
        self.vk_buffer = vk::Buffer::null();
    }
}

impl GpuBuffer {
    pub fn new(device: Arc<GpuDevice>, buffer_type: GpuBufferType, size: usize) -> Self {
        let (usage_flags, location) = match buffer_type {
            GpuBufferType::Uniform => (
                vk::BufferUsageFlags::UNIFORM_BUFFER
                    | vk::BufferUsageFlags::TRANSFER_DST
                    | vk::BufferUsageFlags::TRANSFER_SRC,
                MemoryLocation::GpuOnly,
            ),
            GpuBufferType::Storage => (
                vk::BufferUsageFlags::STORAGE_BUFFER
                    | vk::BufferUsageFlags::TRANSFER_DST
                    | vk::BufferUsageFlags::TRANSFER_SRC,
                MemoryLocation::GpuOnly,
            ),
            GpuBufferType::CpuToGpu => {
                (vk::BufferUsageFlags::TRANSFER_SRC, MemoryLocation::CpuToGpu)
            }
            GpuBufferType::GpuToCpu => {
                (vk::BufferUsageFlags::TRANSFER_DST, MemoryLocation::GpuToCpu)
            }
        };
        let vk_info = vk::BufferCreateInfo::builder()
            .size(size as vk::DeviceSize)
            .usage(usage_flags)
            .sharing_mode(vk::SharingMode::EXCLUSIVE);

        let vk_buffer;
        let allocation;
        {
            vk_buffer = unsafe { device.vk_device.create_buffer(&vk_info, None) }.unwrap();
            let requirements =
                unsafe { device.vk_device.get_buffer_memory_requirements(vk_buffer) };

            allocation = device.gpu_alloc(&AllocationCreateDesc {
                name: "",
                requirements,
                location,
                linear: true, // Buffers are always linear
            });

            unsafe {
                device
                    .vk_device
                    .bind_buffer_memory(vk_buffer, allocation.memory(), allocation.offset())
                    .unwrap()
            };
        }

        let upload_mapped_ptr = if buffer_type == GpuBufferType::CpuToGpu {
            Some(Mutex::new(allocation.mapped_ptr().unwrap()))
        } else {
            None
        };

        Self {
            device,
            vk_buffer,
            buffer_type,
            size,
            allocation,
            upload_mapped_ptr,
        }
    }

    pub fn download<T: Sized>(&self, data: &mut T, offset: usize) {
        if self.buffer_type != GpuBufferType::GpuToCpu {
            panic!("Download works only for buffers with GpuToCpu type");
        }
        unsafe {
            let bytes = std::slice::from_raw_parts_mut(
                (data as *mut T) as *mut u8,
                std::mem::size_of::<T>(),
            );
            self.download_bytes(bytes, offset);
        }
    }

    pub fn download_slice<T: Sized>(&self, data: &mut [T], offset: usize) {
        if self.buffer_type != GpuBufferType::GpuToCpu {
            panic!("Download works only for buffers with GpuToCpu type");
        }
        unsafe {
            let bytes = std::slice::from_raw_parts_mut(
                (data.as_ptr() as *mut T) as *mut u8,
                std::mem::size_of_val(data),
            );
            self.download_bytes(bytes, offset);
        }
    }

    pub fn download_bytes(&self, data: &mut [u8], offset: usize) {
        if self.buffer_type != GpuBufferType::GpuToCpu {
            panic!("Download works only for buffers with GpuToCpu type");
        }
        unsafe {
            let slice = self.allocation.mapped_slice().unwrap();
            let ptr = slice.as_ptr().add(offset);
            std::ptr::copy(ptr, data.as_mut_ptr(), data.len());
        }
    }

    pub fn upload<T: Sized>(&self, data: &T, offset: usize) {
        unsafe {
            let bytes = std::slice::from_raw_parts(
                (data as *const T) as *const u8,
                std::mem::size_of::<T>(),
            );
            self.upload_bytes(bytes, offset);
        }
    }

    pub fn upload_slice<T: Sized>(&self, data: &[T], offset: usize) {
        unsafe {
            let mapped_ptr = self.upload_mapped_ptr.as_ref().unwrap().lock().unwrap();
            let slice: &mut [u8] =
                std::slice::from_raw_parts_mut(mapped_ptr.cast().as_ptr(), self.size);
            let ptr = slice.as_mut_ptr().add(offset);
            std::ptr::copy(
                (data.as_ptr() as *const T) as *const u8,
                ptr,
                std::mem::size_of_val(data),
            );
        }
    }

    pub fn upload_bytes(&self, data: &[u8], offset: usize) {
        unsafe {
            let mapped_ptr = self.upload_mapped_ptr.as_ref().unwrap().lock().unwrap();
            let slice: &mut [u8] =
                std::slice::from_raw_parts_mut(mapped_ptr.cast().as_ptr(), self.size);
            let ptr = slice.as_mut_ptr().add(offset);
            std::ptr::copy(data.as_ptr(), ptr, data.len());
        }
    }
}
