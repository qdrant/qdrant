use std::ffi::CString;
use std::sync::Arc;

use ash::vk;
use gpu_allocator::vulkan::{Allocation, AllocationCreateDesc, Allocator, AllocatorCreateDesc};
use parking_lot::Mutex;

use crate::*;

/// GPU device structure.
/// It's a wrapper around Vulkan device.
pub struct Device {
    /// Instance that owns the device.
    instance: Arc<Instance>,

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

    /// All found transfer queues.
    _transfer_queues: Vec<Queue>,

    /// GPU subgroup (warp in CUDA terms) size.
    subgroup_size: usize,

    /// Is subgroup size (warp) dynamic.
    /// If true, we need to use additional subgroup size control in the pipeline.
    /// And use Vulkan extension that allows to set subgroup size.
    is_dynamic_subgroup_size: bool,

    /// Maximum work group size for compute shaders.
    /// It's used in bounds checking in Context.
    max_compute_work_group_count: [usize; 3],

    /// Maximum GPU buffer size.
    max_buffer_size: usize,

    /// Selected queue index to use.
    queue_index: usize,
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
        Self::new_with_queue_index(instance, vk_physical_device, 0)
    }

    pub fn new_with_queue_index(
        instance: Arc<Instance>,
        vk_physical_device: &PhysicalDevice,
        queue_index: usize,
    ) -> GpuResult<Arc<Device>> {
        #[allow(unused_mut)]
        let mut extensions_cstr: Vec<CString> = vec![CString::from(ash::khr::maintenance1::NAME)];
        #[cfg(target_os = "macos")]
        {
            extensions_cstr.push(CString::from(ash::khr::portability_subset::NAME));
        }

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

        let physical_device_features = vk::PhysicalDeviceFeatures::default();

        // TODO(gpu): check presence of features

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
        if !enabled_physical_device_features_1_2.shader_float16 == 0 {
            return Err(GpuError::NotSupported(
                "Float16 is not supported".to_string(),
            ));
        }
        if !enabled_physical_device_features_1_2.storage_buffer8_bit_access == 0 {
            return Err(GpuError::NotSupported(
                "Storage buffer 8 bit access is not supported".to_string(),
            ));
        }
        let mut physical_device_features_1_2 = vk::PhysicalDeviceVulkan12Features::default()
            .shader_int8(true)
            .shader_float16(true)
            .storage_buffer8_bit_access(true);

        // From Vulkan 1.3 we need subgroup size control if it's dynamic.
        let mut physical_device_features_1_3 = vk::PhysicalDeviceVulkan13Features::default();

        let max_compute_work_group_count;
        let max_buffer_size;
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
            let mut vulkan_1_3_properties = vk::PhysicalDeviceVulkan13Properties::default();
            let mut props2 = vk::PhysicalDeviceProperties2::default()
                .push_next(&mut subgroup_properties)
                .push_next(&mut vulkan_1_3_properties);
            unsafe {
                instance.vk_instance().get_physical_device_properties2(
                    vk_physical_device.vk_physical_device,
                    &mut props2,
                );
            }

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
        Self::check_extensions_list(
            &instance,
            vk_physical_device.vk_physical_device,
            &extensions_cstr,
        )?;
        let extension_names_raw: Vec<*const i8> = extensions_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        let device_create_info = vk::DeviceCreateInfo::default()
            .flags(vk::DeviceCreateFlags::empty())
            .queue_create_infos(&queue_create_infos)
            .enabled_extension_names(&extension_names_raw)
            .enabled_features(&physical_device_features)
            .push_next(&mut physical_device_features_1_1)
            .push_next(&mut physical_device_features_1_2)
            .push_next(&mut physical_device_features_1_3);

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
        let mut transfer_queues = Vec::new();

        for (vk_queue_family_index, vk_queue_family) in vk_queue_families.iter().enumerate() {
            for vk_queue_index in 0..vk_queue_family.queue_count as usize {
                let vk_queue = unsafe {
                    vk_device.get_device_queue(vk_queue_family_index as u32, vk_queue_index as u32)
                };
                let queue = Queue {
                    vk_queue,
                    vk_queue_index,
                    vk_queue_family_index,
                };

                let queue_flags = vk_queue_family.queue_flags;
                if vk_queue != vk::Queue::null() {
                    if queue_flags.contains(vk::QueueFlags::TRANSFER) {
                        transfer_queues.push(queue.clone());
                    }
                    if queue_flags.contains(vk::QueueFlags::COMPUTE) {
                        compute_queues.push(queue);
                    }
                }
            }
        }

        let gpu_allocator_result = Allocator::new(&AllocatorCreateDesc {
            instance: instance.vk_instance().clone(),
            device: vk_device.clone(),
            physical_device: vk_physical_device.vk_physical_device,
            debug_settings: Default::default(),
            buffer_device_address: false,
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

        Ok(Arc::new(Device {
            instance: instance.clone(),
            vk_device,
            gpu_allocator,
            compute_queues,
            _transfer_queues: transfer_queues,
            subgroup_size,
            max_compute_work_group_count,
            max_buffer_size,
            is_dynamic_subgroup_size,
            queue_index,
            name: vk_physical_device.name.clone(),
        }))
    }

    /// Get CPU allocator.
    pub fn cpu_allocation_callbacks(&self) -> Option<&vk::AllocationCallbacks> {
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
                log::error!("Failed to free GPU memory: {:?}", e);
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

    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }

    pub fn compute_queue(&self) -> &Queue {
        &self.compute_queues[self.queue_index % self.compute_queues.len()]
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn check_extensions_list(
        instance: &Instance,
        vk_physical_device: vk::PhysicalDevice,
        required_extensions: &[CString],
    ) -> GpuResult<()> {
        let available_extensions = unsafe {
            instance
                .vk_instance()
                .enumerate_device_extension_properties(vk_physical_device)?
        };

        for required_extension in required_extensions {
            let is_extension_available = available_extensions.iter().any(|extension| {
                let extension_name =
                    unsafe { std::ffi::CStr::from_ptr(extension.extension_name.as_ptr()) };
                extension_name == required_extension.as_c_str()
            });

            if !is_extension_available {
                return Err(GpuError::NotSupported(format!(
                    "Extension {:?} is not supported",
                    required_extension
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
