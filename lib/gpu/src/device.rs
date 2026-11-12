use std::ffi::{CStr, CString, c_char};
use std::sync::Arc;

use ash::vk;
use gpu_allocator::vulkan::{Allocation, AllocationCreateDesc, Allocator, AllocatorCreateDesc};
use parking_lot::{Mutex, MutexGuard};

use crate::device_features::{HalfPrecision, OptionalFeatures, has_extension};
use crate::memory_budget::{device_local_heap_index, query_memory_budget};
use crate::transfer_queue::dedicated_transfer_family;
use crate::*;

/// GPU device structure.
/// It's a wrapper around Vulkan device.
pub struct Device {
    /// Instance that owns the device.
    instance: Arc<Instance>,

    /// Physical device the device was created on, for queries made later (memory budget).
    vk_physical_device: vk::PhysicalDevice,

    /// Native Vulkan device handle.
    vk_device: ash::Device,

    /// Hardware device name.
    name: String,

    /// GPU memory allocator from `gpu-allocator` crate.
    /// It's an Option because of drop order. We need to drop it before the device.
    /// But `Allocator` is destroyed by it's own drop.
    gpu_allocator: Option<Mutex<Allocator>>,

    /// All found compute queues.
    compute_queues: Vec<Queue>,

    /// Queue for transfer contexts, see `transfer_queue`.
    transfer_queue: Queue,

    /// Whether `transfer_queue` is a dedicated queue or the compute queue.
    transfer_queue_kind: TransferQueueKind,

    /// Vulkan requires external synchronization of a queue during `vkQueueSubmit`.
    /// Contexts on the same queue may live on different threads, and the transfer queue can be
    /// the compute queue, so every submission on this device takes this lock.
    queue_submit_lock: Mutex<()>,

    /// Optional features that were enabled.
    optional_features: OptionalFeatures,

    /// Index of the memory heap `memory_budget` reports on.
    memory_budget_heap_index: usize,

    /// GPU subgroup (warp in CUDA terms) size.
    subgroup_size: usize,

    /// Is subgroup size (warp) dynamic.
    /// If true, we need to use additional subgroup size control in the pipeline.
    /// And use Vulkan extension that allows to set subgroup size.
    is_dynamic_subgroup_size: bool,

    /// Maximum work group size for compute shaders.
    /// It's used in bounds checking in Context.
    max_compute_work_group_count: [usize; 3],

    /// `maxStorageBufferRange` in bytes, see `max_buffer_size`.
    max_buffer_size: usize,

    /// `maxBufferSize` in bytes, see `max_buffer_create_size_bytes`.
    max_buffer_create_size_bytes: u64,

    /// `maxMemoryAllocationSize` in bytes, see `max_memory_allocation_size_bytes`.
    max_memory_allocation_size_bytes: u64,

    /// Selected queue index to use.
    queue_index: usize,

    /// Does the device support half precision floats.
    has_half_precision: bool,
}

// GPU execution queue.
#[derive(Clone)]
pub struct Queue {
    // Native Vulkan queue handler.
    pub vk_queue: vk::Queue,

    // Queue family index for the native Vulkan queue.
    pub vk_queue_family_index: usize,

    // Index in the family for the native Vulkan queue.
    pub vk_queue_index: usize,
}

impl Device {
    pub fn new(
        instance: Arc<Instance>,
        vk_physical_device: &PhysicalDevice,
    ) -> GpuResult<Arc<Device>> {
        Self::new_with_params(instance, vk_physical_device, 0, false)
    }

    pub fn new_with_params(
        instance: Arc<Instance>,
        vk_physical_device: &PhysicalDevice,
        queue_index: usize,
        skip_half_precision: bool,
    ) -> GpuResult<Arc<Device>> {
        #[allow(unused_mut)]
        let mut extensions_cstr: Vec<CString> = vec![CString::from(ash::khr::maintenance1::NAME)];
        #[cfg(target_os = "macos")]
        {
            extensions_cstr.push(CString::from(ash::khr::portability_subset::NAME));
        }

        let available_extensions = unsafe {
            instance
                .vk_instance()
                .enumerate_device_extension_properties(vk_physical_device.vk_physical_device)?
        };

        let vk_queue_families = unsafe {
            instance
                .vk_instance()
                .get_physical_device_queue_family_properties(vk_physical_device.vk_physical_device)
        };

        let max_queue_priorities_counts: Vec<Vec<f32>> = vk_queue_families
            .iter()
            .map(|vk_queue_family| vec![0.; vk_queue_family.queue_count as usize])
            .collect();

        let queue_create_infos: Vec<vk::DeviceQueueCreateInfo> = max_queue_priorities_counts
            .iter()
            .enumerate()
            .map(|(queue_family_index, queue_priorities)| {
                vk::DeviceQueueCreateInfo::default()
                    .flags(vk::DeviceQueueCreateFlags::empty())
                    .queue_family_index(queue_family_index as u32)
                    .queue_priorities(queue_priorities.as_slice())
            })
            .collect();

        // Define Vulkan features that we need.
        let mut enabled_physical_device_features_1_1 =
            vk::PhysicalDeviceVulkan11Features::default();
        let mut enabled_physical_device_features_1_2 =
            vk::PhysicalDeviceVulkan12Features::default();
        let mut enabled_physical_device_features_1_3 =
            vk::PhysicalDeviceVulkan13Features::default();
        let mut enabled_physical_devices_features = vk::PhysicalDeviceFeatures2::default()
            .push_next(&mut enabled_physical_device_features_1_1)
            .push_next(&mut enabled_physical_device_features_1_2)
            .push_next(&mut enabled_physical_device_features_1_3);
        unsafe {
            instance.vk_instance().get_physical_device_features2(
                vk_physical_device.vk_physical_device,
                &mut enabled_physical_devices_features,
            );
        };

        // From Vulkan 1.1 we need storage buffer 16 bit access.
        if !enabled_physical_device_features_1_1.storage_buffer16_bit_access == 0 {
            return Err(GpuError::NotSupported(
                "Storage buffer 16 bit access is not supported".to_string(),
            ));
        }
        let mut physical_device_features_1_1 =
            vk::PhysicalDeviceVulkan11Features::default().storage_buffer16_bit_access(true);

        // From Vulkan 1.2 we need int8/float16 support.
        if !enabled_physical_device_features_1_2.shader_int8 == 0 {
            return Err(GpuError::NotSupported("Int8 is not supported".to_string()));
        }
        let has_half_precision =
            !skip_half_precision && enabled_physical_device_features_1_2.shader_float16 == 1;
        if !has_half_precision {
            log::warn!("Half precision is not supported, falling back to full precision floats");
        }

        // Optional features, detected once half precision is decided: cooperative matrix
        // configurations with f16 components are usable only with it.
        let optional_features = OptionalFeatures::detect(
            &instance,
            vk_physical_device.vk_physical_device,
            &available_extensions,
            if has_half_precision {
                HalfPrecision::Enabled
            } else {
                HalfPrecision::Disabled
            },
        );
        let physical_device_features =
            vk::PhysicalDeviceFeatures::default().shader_int64(optional_features.shader_int64);

        if !enabled_physical_device_features_1_2.storage_buffer8_bit_access == 0 {
            return Err(GpuError::NotSupported(
                "Storage buffer 8 bit access is not supported".to_string(),
            ));
        }
        let mut physical_device_features_1_2 = vk::PhysicalDeviceVulkan12Features::default()
            .shader_int8(true)
            .shader_float16(has_half_precision)
            .storage_buffer8_bit_access(true)
            .buffer_device_address(optional_features.buffer_device_address)
            .vulkan_memory_model(optional_features.vulkan_memory_model)
            .vulkan_memory_model_device_scope(optional_features.vulkan_memory_model);

        // Optional from Vulkan 1.3: integer dot products.
        // Subgroup size control is added below if the subgroup size is dynamic.
        let mut physical_device_features_1_3 = vk::PhysicalDeviceVulkan13Features::default()
            .shader_integer_dot_product(optional_features.integer_dot_product.is_some());

        let mut cooperative_matrix_features =
            vk::PhysicalDeviceCooperativeMatrixFeaturesKHR::default()
                .cooperative_matrix(optional_features.cooperative_matrix.is_some());

        let max_compute_work_group_count;
        let max_buffer_size;
        let max_buffer_create_size_bytes;
        let max_memory_allocation_size_bytes;
        let mut is_dynamic_subgroup_size = false;
        let subgroup_size = {
            let props = unsafe {
                instance
                    .vk_instance()
                    .get_physical_device_properties(vk_physical_device.vk_physical_device)
            };
            max_compute_work_group_count = [
                props.limits.max_compute_work_group_count[0] as usize,
                props.limits.max_compute_work_group_count[1] as usize,
                props.limits.max_compute_work_group_count[2] as usize,
            ];
            max_buffer_size = props.limits.max_storage_buffer_range as usize;
            let mut subgroup_properties = vk::PhysicalDeviceSubgroupProperties::default();
            let mut maintenance_3_properties = vk::PhysicalDeviceMaintenance3Properties::default();
            let mut vulkan_1_3_properties = vk::PhysicalDeviceVulkan13Properties::default();
            let mut props2 = vk::PhysicalDeviceProperties2::default()
                .push_next(&mut subgroup_properties)
                .push_next(&mut maintenance_3_properties)
                .push_next(&mut vulkan_1_3_properties);
            unsafe {
                instance.vk_instance().get_physical_device_properties2(
                    vk_physical_device.vk_physical_device,
                    &mut props2,
                );
            }
            max_buffer_create_size_bytes = vulkan_1_3_properties.max_buffer_size;
            max_memory_allocation_size_bytes = maintenance_3_properties.max_memory_allocation_size;

            let subgroup_size = if vulkan_1_3_properties.min_subgroup_size
                != vulkan_1_3_properties.max_subgroup_size
            {
                if !enabled_physical_device_features_1_3.subgroup_size_control == 0 {
                    return Err(GpuError::NotSupported(
                        "Subgroup size control is not supported".to_string(),
                    ));
                }
                physical_device_features_1_3 =
                    physical_device_features_1_3.subgroup_size_control(true);

                if !vulkan_1_3_properties
                    .required_subgroup_size_stages
                    .contains(vk::ShaderStageFlags::COMPUTE)
                {
                    // A strange situation where subgroup size can be different but we cannot set it.
                    // We cannot handle this case (we have to know subgroup size), so skip device creation.
                    return Err(GpuError::NotSupported(
                        "Subgroup size is dynamic but not supported for compute stage".to_string(),
                    ));
                }
                is_dynamic_subgroup_size = true;
                // prefer max subgroup size
                vulkan_1_3_properties.max_subgroup_size as usize
            } else {
                subgroup_properties.subgroup_size as usize
            };

            log::info!("Create GPU device {}", vk_physical_device.name);
            log::debug!("GPU subgroup size: {subgroup_size}");
            subgroup_size
        };

        // convert extension names to raw pointers to provide to Vulkan
        Self::check_extensions_list(&available_extensions, &extensions_cstr)?;
        let extension_names_raw: Vec<*const c_char> = extensions_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .chain(optional_features.extensions().into_iter().map(CStr::as_ptr))
            .collect();

        let mut device_create_info = vk::DeviceCreateInfo::default()
            .flags(vk::DeviceCreateFlags::empty())
            .queue_create_infos(&queue_create_infos)
            .enabled_extension_names(&extension_names_raw)
            .enabled_features(&physical_device_features)
            .push_next(&mut physical_device_features_1_1)
            .push_next(&mut physical_device_features_1_2)
            .push_next(&mut physical_device_features_1_3);
        if optional_features.cooperative_matrix.is_some() {
            device_create_info = device_create_info.push_next(&mut cooperative_matrix_features);
        }

        let vk_device_result = unsafe {
            instance.vk_instance().create_device(
                vk_physical_device.vk_physical_device,
                &device_create_info,
                instance.cpu_allocation_callbacks(),
            )
        };

        let vk_device = match vk_device_result {
            Ok(vk_device) => vk_device,
            Err(e) => return Err(GpuError::from(e)),
        };

        let mut compute_queues = Vec::new();
        let mut dedicated_transfer_queues = Vec::new();
        let dedicated_transfer_family = dedicated_transfer_family(&vk_queue_families);

        for (vk_queue_family_index, vk_queue_family) in vk_queue_families.iter().enumerate() {
            for vk_queue_index in 0..vk_queue_family.queue_count as usize {
                let vk_queue = unsafe {
                    vk_device.get_device_queue(vk_queue_family_index as u32, vk_queue_index as u32)
                };
                let queue = Queue {
                    vk_queue,
                    vk_queue_family_index,
                    vk_queue_index,
                };

                let queue_flags = vk_queue_family.queue_flags;
                if vk_queue != vk::Queue::null() {
                    if dedicated_transfer_family == Some(vk_queue_family_index) {
                        dedicated_transfer_queues.push(queue.clone());
                    }
                    if queue_flags.contains(vk::QueueFlags::COMPUTE) {
                        compute_queues.push(queue);
                    }
                }
            }
        }

        if compute_queues.is_empty() {
            unsafe {
                vk_device.destroy_device(instance.cpu_allocation_callbacks());
            }
            return Err(GpuError::NotSupported(
                "Device has no compute queue".to_string(),
            ));
        }

        // A dedicated transfer queue where the device has one, else the compute queue.
        let (transfer_queue, transfer_queue_kind) = if dedicated_transfer_queues.is_empty() {
            (
                compute_queues[queue_index % compute_queues.len()].clone(),
                TransferQueueKind::ComputeQueue,
            )
        } else {
            (
                dedicated_transfer_queues[queue_index % dedicated_transfer_queues.len()].clone(),
                TransferQueueKind::Dedicated,
            )
        };

        let memory_budget_heap_index = {
            let memory_properties = unsafe {
                instance
                    .vk_instance()
                    .get_physical_device_memory_properties(vk_physical_device.vk_physical_device)
            };
            device_local_heap_index(&memory_properties)
        };

        let gpu_allocator_result = Allocator::new(&AllocatorCreateDesc {
            instance: instance.vk_instance().clone(),
            device: vk_device.clone(),
            physical_device: vk_physical_device.vk_physical_device,
            debug_settings: Default::default(),
            // With buffer device addresses enabled, all memory is allocated with
            // `VK_MEMORY_ALLOCATE_DEVICE_ADDRESS_BIT`, which buffers with
            // `SHADER_DEVICE_ADDRESS` usage need.
            buffer_device_address: optional_features.buffer_device_address,
            allocation_sizes: Default::default(),
        });

        let gpu_allocator = match gpu_allocator_result {
            Ok(gpu_allocator) => Some(Mutex::new(gpu_allocator)),
            Err(e) => {
                unsafe {
                    vk_device.destroy_device(instance.cpu_allocation_callbacks());
                }
                return Err(GpuError::from(e));
            }
        };

        log::debug!(
            "GPU optional features: integer dot product {}, cooperative matrix {}, \
             buffer device address {}, shader int64 {}, Vulkan memory model {}, memory budget {:?}, \
             transfer queue {transfer_queue_kind:?}",
            optional_features.integer_dot_product.is_some(),
            optional_features.cooperative_matrix.is_some(),
            optional_features.buffer_device_address,
            optional_features.shader_int64,
            optional_features.vulkan_memory_model,
            optional_features.memory_budget,
        );

        Ok(Arc::new(Device {
            instance: instance.clone(),
            vk_physical_device: vk_physical_device.vk_physical_device,
            vk_device,
            gpu_allocator,
            compute_queues,
            transfer_queue,
            transfer_queue_kind,
            queue_submit_lock: Mutex::new(()),
            optional_features,
            memory_budget_heap_index,
            subgroup_size,
            max_compute_work_group_count,
            max_buffer_size,
            max_buffer_create_size_bytes,
            max_memory_allocation_size_bytes,
            is_dynamic_subgroup_size,
            queue_index,
            name: vk_physical_device.name.clone(),
            has_half_precision,
        }))
    }

    /// Get CPU allocator.
    pub fn cpu_allocation_callbacks(&self) -> Option<&vk::AllocationCallbacks<'_>> {
        self.instance.cpu_allocation_callbacks()
    }

    /// Allocate GPU memory.
    pub fn allocate(&self, allocation_desc: &AllocationCreateDesc) -> GpuResult<Allocation> {
        if let Some(gpu_allocator) = &self.gpu_allocator {
            let mut gpu_allocator = gpu_allocator.lock();
            gpu_allocator
                .allocate(allocation_desc)
                .map_err(GpuError::from)
        } else {
            Err(GpuError::Other(
                "GPU allocator is not available".to_string(),
            ))
        }
    }

    /// Free GPU memory.
    pub fn free(&self, allocation: Allocation) {
        if let Some(gpu_allocator) = &self.gpu_allocator {
            let mut gpu_allocator = gpu_allocator.lock();
            if let Err(e) = gpu_allocator.free(allocation) {
                // Log error because free is called from Drop.
                log::error!("Failed to free GPU memory: {e:?}");
            }
        } else {
            log::error!("GPU allocator is not available");
        }
    }

    /// Get subgroup size (warp in terms of CUDA).
    pub fn subgroup_size(&self) -> usize {
        self.subgroup_size
    }

    pub fn instance(&self) -> Arc<Instance> {
        self.instance.clone()
    }

    pub fn vk_device(&self) -> &ash::Device {
        &self.vk_device
    }

    pub fn is_dynamic_subgroup_size(&self) -> bool {
        self.is_dynamic_subgroup_size
    }

    pub fn max_compute_work_group_count(&self) -> [usize; 3] {
        self.max_compute_work_group_count
    }

    /// Largest range of a storage buffer that a descriptor set can bind
    /// (`maxStorageBufferRange`), in bytes. Not the largest buffer that can be created, see
    /// `max_buffer_create_size_bytes`.
    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }

    /// Largest buffer that can be created (`maxBufferSize`, Vulkan 1.3), in bytes.
    /// The part of a buffer beyond `max_buffer_size` is reachable only through its device
    /// address. A buffer also needs one memory allocation, see `max_memory_allocation_size_bytes`.
    pub fn max_buffer_create_size_bytes(&self) -> u64 {
        self.max_buffer_create_size_bytes
    }

    /// Largest single memory allocation (`maxMemoryAllocationSize`, Vulkan 1.1), in bytes.
    /// The driver may still fail a smaller allocation, and a buffer above it can't be created.
    pub fn max_memory_allocation_size_bytes(&self) -> u64 {
        self.max_memory_allocation_size_bytes
    }

    pub fn has_half_precision(&self) -> bool {
        self.has_half_precision
    }

    pub fn compute_queue(&self) -> &Queue {
        &self.compute_queues[self.queue_index % self.compute_queues.len()]
    }

    /// Queue for transfer contexts (`Context::with_queue(device, ContextQueue::Transfer)`),
    /// for copies that overlap the compute queue's work.
    /// A queue of a transfer-only queue family where the device has one, else the compute queue
    /// itself. `transfer_queue_kind` says which.
    pub fn transfer_queue(&self) -> &Queue {
        &self.transfer_queue
    }

    /// Whether `transfer_queue` is a dedicated transfer queue or the compute queue.
    pub fn transfer_queue_kind(&self) -> TransferQueueKind {
        self.transfer_queue_kind
    }

    /// Lock to hold while submitting to any queue of this device.
    pub(crate) fn lock_queue_submit(&self) -> MutexGuard<'_, ()> {
        self.queue_submit_lock.lock()
    }

    /// Properties of the 8-bit integer dot products, if `shaderIntegerDotProduct`
    /// (`VK_KHR_shader_integer_dot_product`, core in Vulkan 1.3) is enabled.
    /// `None` if the device doesn't support it, and also on a Vulkan 1.2 device that has only
    /// the KHR extension: only the core Vulkan 1.3 feature is enabled.
    pub fn integer_dot_product(&self) -> Option<IntegerDotProductProperties> {
        self.optional_features.integer_dot_product
    }

    /// The cooperative matrix configurations this device, as created, can use in a shader, in the
    /// order the device reports them. The device's list is filtered by the rule on
    /// `CooperativeMatrixProperties`: subgroup scope, and component types whose features are
    /// enabled (f16 only with `has_half_precision`, 64-bit integers only with `has_shader_int64`).
    /// `None` if `VK_KHR_cooperative_matrix` isn't enabled: the device doesn't support cooperative
    /// matrices in compute shaders, doesn't support the Vulkan memory model (which shaders with
    /// cooperative matrices need), or reports no configuration that passes the filter. So a
    /// returned list is never empty.
    pub fn cooperative_matrix_properties(&self) -> Option<&[CooperativeMatrixProperties]> {
        self.optional_features.cooperative_matrix.as_deref()
    }

    /// Whether `bufferDeviceAddress` is enabled, so buffers can be created with
    /// `DeviceAddress::Enabled` and shaders can access them through 64-bit addresses.
    pub fn has_buffer_device_address(&self) -> bool {
        self.optional_features.buffer_device_address
    }

    /// Whether `shaderInt64` is enabled, for 64-bit integers (such as address arithmetic) in shaders.
    pub fn has_shader_int64(&self) -> bool {
        self.optional_features.shader_int64
    }

    /// Whether `vulkanMemoryModel` and `vulkanMemoryModelDeviceScope` are enabled. They're enabled
    /// only as a pair: false if the device lacks either, even with `vulkanMemoryModel` alone.
    pub fn has_vulkan_memory_model(&self) -> bool {
        self.optional_features.vulkan_memory_model
    }

    /// The device-local memory (VRAM) budget of this process, queried now.
    /// From `VK_EXT_memory_budget` where the driver has it, else the size of the heap.
    pub fn memory_budget(&self) -> MemoryBudget {
        query_memory_budget(
            &self.instance,
            self.vk_physical_device,
            self.memory_budget_heap_index,
            self.optional_features.memory_budget,
        )
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_extensions_list(
        available_extensions: &[vk::ExtensionProperties],
        required_extensions: &[CString],
    ) -> GpuResult<()> {
        for required_extension in required_extensions {
            let is_extension_available =
                has_extension(available_extensions, required_extension.as_c_str());

            if !is_extension_available {
                return Err(GpuError::NotSupported(format!(
                    "Extension {required_extension:?} is not supported"
                )));
            }
        }

        Ok(())
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        self.gpu_allocator = None;
        unsafe {
            // For now, we don't need to wait for device idle.
            // It doesn't have timeout, so it can hang the application.
            // Moreover, we control all execution by Context and catch timeout.
            // It we have infinity loops in shader we leak this device and let it running intil OS stops it.
            // self.vk_device.device_wait_idle().unwrap();
            self.vk_device
                .destroy_device(self.cpu_allocation_callbacks());
        }
    }
}
