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

    /// Which queues may use the buffer.
    queues: BufferQueues,

    /// Sharing mode the Vulkan buffer was created with.
    vk_sharing_mode: vk::SharingMode,

    /// 64-bit device address, if the buffer was created with `DeviceAddress::Enabled`.
    device_address: Option<vk::DeviceAddress>,

    /// GPU memory allocation that backs the buffer.
    allocation: Mutex<Allocation>,
}

/// Options of `Buffer::new_with_options`. `Buffer::new` uses `BufferOptions::COMPUTE`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferOptions {
    /// Which queues may use the buffer.
    pub queues: BufferQueues,

    /// Whether shaders can access the buffer through its 64-bit device address.
    pub device_address: DeviceAddress,
}

impl BufferOptions {
    /// Used on the compute queue only, without a device address.
    pub const COMPUTE: Self = Self {
        queues: BufferQueues::Compute,
        device_address: DeviceAddress::Disabled,
    };
}

/// Which queues may use a buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferQueues {
    /// Only contexts on the compute queue (`ContextQueue::Compute`).
    Compute,

    /// Contexts on the compute queue and on the transfer queue (`ContextQueue::Transfer`).
    /// If the two queues are in different queue families, the buffer is shared by both families
    /// (`VK_SHARING_MODE_CONCURRENT`), so either can use it without queue family ownership
    /// transfers. A transfer context only accepts buffers created with this.
    ComputeAndTransfer,
}

/// Whether shaders can access a buffer through its 64-bit device address
/// (`VK_BUFFER_USAGE_SHADER_DEVICE_ADDRESS_BIT`, `GL_EXT_buffer_reference` in GLSL).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceAddress {
    Disabled,
    /// Needs `Device::has_buffer_device_address`.
    Enabled,
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
        Self::new_with_options(device, name, buffer_type, size, BufferOptions::COMPUTE)
    }

    /// Create a buffer that other queues may use too, or that shaders access through its
    /// device address, as `options` say.
    pub fn new_with_options(
        device: Arc<Device>,   // Device that owns the buffer.
        name: impl AsRef<str>, // Name of the buffer for tracking and debugging purposes.
        buffer_type: BufferType,
        size: usize,
        options: BufferOptions,
    ) -> GpuResult<Arc<Self>> {
        let BufferOptions {
            queues,
            device_address,
        } = options;

        if size == 0 {
            return Err(GpuError::NotSupported(
                "Zero-sized GPU buffers are not supported".to_string(),
            ));
        }

        let vk_device_address_usage = match device_address {
            DeviceAddress::Disabled => vk::BufferUsageFlags::empty(),
            DeviceAddress::Enabled => {
                if !device.has_buffer_device_address() {
                    return Err(GpuError::NotSupported(
                        "Buffer device addresses are not supported by the device".to_string(),
                    ));
                }
                vk::BufferUsageFlags::SHADER_DEVICE_ADDRESS
            }
        };

        // Vulkan API requires buffer usage flags to be specified during the buffer creation.
        let vk_usage_flags = vk_device_address_usage
            | match buffer_type {
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

        // A buffer of one queue family is exclusive to it. A buffer of the compute and the transfer
        // queue family is shared by both, so it needs no queue family ownership transfers.
        let compute_queue_family = device.compute_queue().vk_queue_family_index as u32;
        let transfer_queue_family = device.transfer_queue().vk_queue_family_index as u32;
        let vk_queue_family_indices = [compute_queue_family, transfer_queue_family];
        let vk_sharing_mode = match queues {
            BufferQueues::ComputeAndTransfer if compute_queue_family != transfer_queue_family => {
                vk::SharingMode::CONCURRENT
            }
            BufferQueues::Compute | BufferQueues::ComputeAndTransfer => vk::SharingMode::EXCLUSIVE,
        };

        // Create a Vulkan buffer.
        let mut vk_create_buffer_info = vk::BufferCreateInfo::default()
            .size(size as vk::DeviceSize)
            .usage(vk_usage_flags)
            .sharing_mode(vk_sharing_mode);
        if vk_sharing_mode == vk::SharingMode::CONCURRENT {
            vk_create_buffer_info =
                vk_create_buffer_info.queue_family_indices(&vk_queue_family_indices);
        }
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

        let device_address = match device_address {
            DeviceAddress::Disabled => None,
            DeviceAddress::Enabled => Some(unsafe {
                device.vk_device().get_buffer_device_address(
                    &vk::BufferDeviceAddressInfo::default().buffer(vk_buffer),
                )
            }),
        };

        Ok(Arc::new(Self {
            device,
            vk_buffer,
            buffer_type,
            size,
            queues,
            vk_sharing_mode,
            device_address,
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

    /// Which queues may use the buffer.
    pub fn queues(&self) -> BufferQueues {
        self.queues
    }

    pub(crate) fn vk_sharing_mode(&self) -> vk::SharingMode {
        self.vk_sharing_mode
    }

    /// The buffer's 64-bit device address, for shaders to access it with `GL_EXT_buffer_reference`.
    /// `None` if the buffer wasn't created with `DeviceAddress::Enabled`.
    /// A `Context` keeps alive only the buffers it binds or copies, not the ones a shader reaches
    /// through their addresses: hand those to `Context::keep_alive`, so the context holds them
    /// until its work has finished.
    pub fn device_address(&self) -> Option<u64> {
        self.device_address
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
