use std::ops::Range;
use std::sync::Arc;

use ash::vk;
use gpu_allocator::MemoryLocation;
use gpu_allocator::vulkan::{Allocation, AllocationCreateDesc, AllocationScheme};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::*;

static DOWNLOAD_NOT_ALLOWED_ERROR: &str = "Download from the GPU buffer is not allowed";
static UPLOAD_NOT_ALLOWED_ERROR: &str = "Upload to the GPU buffer is not allowed";

/// Buffer is a GPU resource that represents a linear memory region.
pub struct Buffer {
    /// Device that owns the buffer.
    device: Arc<Device>,

    /// Vulkan buffer handle.
    vk_buffer: vk::Buffer,

    /// Buffer type. It defines how the buffer can be used.
    buffer_type: BufferType,

    /// Buffer size in bytes.
    size: usize,

    /// GPU memory allocation that backs the buffer.
    allocation: Mutex<Allocation>,
}

/// Buffer type defines how the buffer can be used.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum BufferType {
    /// Uniform data for a shader.
    Uniform,

    /// Storage buffer can be used as a large read/write buffer.
    Storage,

    /// CpuToGpu buffer can be used as a source for transfer operations only.
    CpuToGpu,

    /// GpuToCpu buffer can be used as a destination for transfer operations only.
    GpuToCpu,
}

// Mark `Buffer` as a GPU resource that should be kept alive while it's in use by the GPU context.
impl Resource for Buffer {}

impl Buffer {
    pub fn new(
        device: Arc<Device>,   // Device that owns the buffer.
        name: impl AsRef<str>, // Name of the buffer for tracking and debugging purposes.
        buffer_type: BufferType,
        size: usize,
    ) -> GpuResult<Arc<Self>> {
        if size == 0 {
            return Err(GpuError::NotSupported(
                "Zero-sized GPU buffers are not supported".to_string(),
            ));
        }

        // Vulkan API requires buffer usage flags to be specified during the buffer creation.
        let vk_usage_flags = match buffer_type {
            BufferType::Uniform => {
                vk::BufferUsageFlags::UNIFORM_BUFFER // mark as uniform buffer.
                    | vk::BufferUsageFlags::TRANSFER_DST // For uploading.
                    | vk::BufferUsageFlags::TRANSFER_SRC // For downloading.
            }
            BufferType::Storage => {
                vk::BufferUsageFlags::STORAGE_BUFFER // mark as storage buffer.
                    | vk::BufferUsageFlags::TRANSFER_DST // For uploading.
                    | vk::BufferUsageFlags::TRANSFER_SRC // For downloading.
            }
            // CpuToGpu buffer can be used as a source for transfer operations only.
            BufferType::CpuToGpu => vk::BufferUsageFlags::TRANSFER_SRC,
            // GpuToCpu buffer can be used as a destination for transfer operations only.
            BufferType::GpuToCpu => vk::BufferUsageFlags::TRANSFER_DST,
        };

        // Memory location depends on the buffer type.
        let location = match buffer_type {
            // Allocate Uniform/Storage buffers in GPU memory only.
            BufferType::Uniform => MemoryLocation::GpuOnly,
            BufferType::Storage => MemoryLocation::GpuOnly,
            // Transfer buffers will be visible to both CPU and GPU.
            BufferType::CpuToGpu => MemoryLocation::CpuToGpu,
            BufferType::GpuToCpu => MemoryLocation::GpuToCpu,
        };

        // Create a Vulkan buffer.
        let vk_create_buffer_info = vk::BufferCreateInfo::default()
            .size(size as vk::DeviceSize)
            .usage(vk_usage_flags)
            .sharing_mode(vk::SharingMode::EXCLUSIVE);
        let vk_buffer = unsafe {
            device
                .vk_device()
                .create_buffer(&vk_create_buffer_info, device.cpu_allocation_callbacks())
                .unwrap()
        };

        // Allocate memory for the buffer.
        let buffer_allocation_requirements =
            unsafe { device.vk_device().get_buffer_memory_requirements(vk_buffer) };
        let allocation_result = device.allocate(&AllocationCreateDesc {
            name: name.as_ref(),
            requirements: buffer_allocation_requirements,
            location,
            linear: true, // Buffers are always linear.
            allocation_scheme: AllocationScheme::GpuAllocatorManaged,
        });

        // Check if the allocation was successful.
        let allocation = match allocation_result {
            Ok(allocation) => allocation,
            Err(e) => {
                unsafe {
                    // Because vulkan buffers lifetime is managed manually,
                    // we need to destroy the buffer in case of an allocation error.
                    device
                        .vk_device()
                        .destroy_buffer(vk_buffer, device.cpu_allocation_callbacks());
                }
                return Err(e);
            }
        };

        // Bind the buffer to the allocated memory.
        let bind_result = unsafe {
            device.vk_device().bind_buffer_memory(
                vk_buffer,
                allocation.memory(),
                allocation.offset(),
            )
        };
        if let Err(e) = bind_result {
            // Free the allocated memory in case of an error.
            device.free(allocation);
            unsafe {
                // Destroy the buffer.
                device
                    .vk_device()
                    .destroy_buffer(vk_buffer, device.cpu_allocation_callbacks());
            }
            return Err(GpuError::from(e));
        }

        Ok(Arc::new(Self {
            device,
            vk_buffer,
            buffer_type,
            size,
            allocation: Mutex::new(allocation),
        }))
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn vk_buffer(&self) -> vk::Buffer {
        self.vk_buffer
    }

    pub fn buffer_type(&self) -> BufferType {
        self.buffer_type
    }

    /// Download a value from the buffer to the RAM.
    pub fn download<T>(&self, data: &mut T, offset: usize) -> GpuResult<()>
    where
        T: FromBytes + IntoBytes + ?Sized,
    {
        if self.buffer_type != BufferType::GpuToCpu {
            return Err(GpuError::Other(DOWNLOAD_NOT_ALLOWED_ERROR.to_string()));
        }
        let data_bytes = data.as_mut_bytes();
        let end = checked_add(offset, data_bytes.len())?;
        self.view(offset..end)?.copy_from_slice(data_bytes);
        Ok(())
    }

    /// Download a vector of `len` elements from the buffer to the RAM.
    pub fn download_vec<T>(&self, offset: usize, len: usize) -> GpuResult<Vec<T>>
    where
        T: FromBytes + IntoBytes + Clone,
    {
        if self.buffer_type != BufferType::GpuToCpu {
            return Err(GpuError::Other(DOWNLOAD_NOT_ALLOWED_ERROR.to_string()));
        }

        let end = len
            .checked_mul(size_of::<T>())
            .and_then(|total_bytes| offset.checked_add(total_bytes))
            .ok_or_else(|| {
                GpuError::OutOfBounds(format!(
                    "Size overflow while downloading from GPU: \
                     {len}*{} + {offset} overflows",
                    size_of::<T>()
                ))
            })?;

        let buffer_slice = self.view(offset..end)?;
        let mut result = vec![T::new_zeroed(); len];
        result.as_mut_bytes().copy_from_slice(&buffer_slice);
        Ok(result)
    }

    /// Upload a value from the RAM to the buffer.
    pub fn upload<T>(&self, data: &T, offset: usize) -> GpuResult<()>
    where
        T: IntoBytes + Immutable + ?Sized,
    {
        if self.buffer_type != BufferType::CpuToGpu {
            return Err(GpuError::Other(UPLOAD_NOT_ALLOWED_ERROR.to_string()));
        }
        let bytes = data.as_bytes();
        let end = checked_add(offset, bytes.len())?;
        self.view(offset..end)?.copy_from_slice(bytes);
        Ok(())
    }

    fn view(&self, range: Range<usize>) -> GpuResult<MappedMutexGuard<'_, [u8]>> {
        let slice = MutexGuard::try_map(self.allocation.lock(), |allocation| {
            allocation.mapped_slice_mut()
        })
        .map_err(|_| GpuError::Other("Accessing the GPU buffer is not allowed".to_string()))?;

        MappedMutexGuard::try_map(slice, |slice| slice.get_mut(range.clone())).map_err(|slice| {
            GpuError::OutOfBounds(format!(
                "Out of bounds while accessing the buffer: \
                 range {range:?} exceeds the buffer size of {} bytes.",
                slice.len()
            ))
        })
    }
}

fn checked_add(a: usize, b: usize) -> GpuResult<usize> {
    a.checked_add(b).ok_or_else(|| {
        GpuError::OutOfBounds(format!(
            "Size overflow while accessing the buffer: {a} + {b} overflows"
        ))
    })
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if self.vk_buffer != vk::Buffer::null() {
            // Drop the allocation and free the allocated memory.
            let mut allocation = Mutex::new(Allocation::default());
            std::mem::swap(&mut allocation, &mut self.allocation);
            let allocation = allocation.into_inner();
            self.device.free(allocation);

            // Destroy the buffer.
            unsafe {
                self.device
                    .vk_device()
                    .destroy_buffer(self.vk_buffer, self.device.cpu_allocation_callbacks())
            };
            self.vk_buffer = vk::Buffer::null();
        }
    }
}
