use std::ffi::CString;
use std::sync::Arc;

use ash::vk;
use gpu_allocator::vulkan::{Allocation, AllocationCreateDesc, Allocator, AllocatorCreateDesc};
use parking_lot::Mutex;

use crate::*;

/// GPU device structure.
/// It's a wrapper around Vulkan device.
pub struct Device {
    /// Name of the device provided by GPU driver.
    pub name: String,

    /// Instance that owns the device.
    pub instance: Arc<Instance>,

    /// Native Vulkan device handle.
    pub vk_device: ash::Device,

    /// Native Vulkan physical device handle.
    /// It's the source of the device creation.
    /// It's managed by the Vulkan instance and does not need to be destroyed.
    pub vk_physical_device: vk::PhysicalDevice,

    /// GPU memory allocator from `gpu-allocator` crate.
    /// It's an Option because of drop order. We need to drop it before the device.
    /// But `Allocator` is destroyed by it's own drop.
    pub gpu_allocator: Option<Mutex<Allocator>>,

    /// All found compute queues.
    pub compute_queues: Vec<Queue>,

    /// All found transfer queues.
    pub transfer_queues: Vec<Queue>,

    /// GPU subgroup (warp in CUDA terms) size.
    pub subgroup_size: usize,

    /// Is subgroup size (warp) dynamic.
    /// If true, we need to use additional subgroup size control in the pipeline.
    /// And use Vulkan extension that allows to set subgroup size.
    pub is_dynamic_subgroup_size: bool,

    /// Maximum work group size for compute shaders.
    /// It's used in bounds checking in Context.
    pub max_compute_work_group_size: [usize; 3],

    /// Selected queue index to use.
    pub queue_index: usize,
}

// GPU execution queue.
#[derive(Clone)]
pub struct Queue {
    // Native Vulkan queue hander.
    pub vk_queue: vk::Queue,

    // Queue family index for the native Vulkan queue.
    pub vk_queue_family_index: usize,

    // Index in the family for the native Vulkan queue.
    pub vk_queue_index: usize,
}

impl Device {
    pub fn new(
        instance: Arc<Instance>,
        vk_physical_device: PhysicalDevice,
    ) -> GpuResult<Arc<Device>> {
        Self::new_with_queue_index(instance, vk_physical_device, 0)
    }

    pub fn new_with_queue_index(
        instance: Arc<Instance>,
        vk_physical_device: PhysicalDevice,
        queue_index: usize,
    ) -> GpuResult<Arc<Device>> {
        #[allow(unused_mut)]
        let mut extensions_cstr: Vec<CString> =
            vec![CString::from(ash::vk::KhrMaintenance1Fn::name())];
        #[cfg(target_os = "macos")]
        {
            extensions_cstr.push(CString::from(ash::vk::KhrPortabilitySubsetFn::name()));
        }

        let vk_queue_families = unsafe {
            instance
                .vk_instance
                .get_physical_device_queue_family_properties(vk_physical_device.vk_physical_device)
        };

        let max_queue_priorities_count = vk_queue_families
            .iter()
            .map(|vk_queue_family| vk_queue_family.queue_count as usize)
            .max()
            .ok_or_else(|| GpuError::Other("No queue families found".to_string()))?;
        let queue_priorities = vec![0.; max_queue_priorities_count];

        let queue_create_infos: Vec<vk::DeviceQueueCreateInfo> = (0..vk_queue_families.len())
            .map(|queue_family_index| {
                vk::DeviceQueueCreateInfo::builder()
                    .flags(vk::DeviceQueueCreateFlags::empty())
                    .queue_family_index(queue_family_index as u32)
                    .queue_priorities(queue_priorities.as_slice())
                    .build()
            })
            .collect();

        let physical_device_features = vk::PhysicalDeviceFeatures {
            ..Default::default()
        };

        // TODO(gpu): check presence of features

        // Define Vulkan features that we need.
        // From Vulkan 1.1 we need storage buffer 16 bit access.
        let physical_device_features_1_1 =
            vk::PhysicalDeviceVulkan11Features::builder().storage_buffer16_bit_access(true);

        // From Vulkan 1.2 we need int8/float16 support.
        let physical_device_features_1_2 = vk::PhysicalDeviceVulkan12Features::builder()
            .shader_int8(true)
            .shader_float16(true)
            .storage_buffer8_bit_access(true);

        // From Vulkan 1.3 we need subgroup size control if it's dynamic.
        let mut physical_device_features_1_3 = vk::PhysicalDeviceVulkan13Features::builder();

        let max_compute_work_group_size;
        let mut is_dynamic_subgroup_size = false;
        let subgroup_size = unsafe {
            let props = instance
                .vk_instance
                .get_physical_device_properties(vk_physical_device.vk_physical_device);
            max_compute_work_group_size = [
                props.limits.max_compute_work_group_size[0] as usize,
                props.limits.max_compute_work_group_size[1] as usize,
                props.limits.max_compute_work_group_size[2] as usize,
            ];
            let mut subgroup_properties = vk::PhysicalDeviceSubgroupProperties::default();
            let mut vulkan_1_3_properties = vk::PhysicalDeviceVulkan13Properties::default();
            let mut props2 = vk::PhysicalDeviceProperties2::builder()
                .push_next(&mut subgroup_properties)
                .push_next(&mut vulkan_1_3_properties)
                .build();
            instance.vk_instance.get_physical_device_properties2(
                vk_physical_device.vk_physical_device,
                &mut props2,
            );
            log::info!(
                "Choosed GPU device: {:?}",
                ::std::ffi::CStr::from_ptr(props.device_name.as_ptr())
            );

            let subgroup_size = if vulkan_1_3_properties.min_subgroup_size
                != vulkan_1_3_properties.max_subgroup_size
            {
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
                physical_device_features_1_3 =
                    physical_device_features_1_3.subgroup_size_control(true);
                // prefer max subgroup size
                vulkan_1_3_properties.max_subgroup_size as usize
            } else {
                subgroup_properties.subgroup_size as usize
            };

            log::debug!("GPU subgroup size: {subgroup_size}");
            subgroup_size
        };

        let mut physical_device_features_1_1 = physical_device_features_1_1.build();
        let mut physical_device_features_1_2 = physical_device_features_1_2.build();
        let mut physical_device_features_1_3 = physical_device_features_1_3.build();

        // convert extension names to raw pointers to provide to Vulkan
        let extension_names_raw: Vec<*const i8> = extensions_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        let device_create_info = vk::DeviceCreateInfo::builder()
            .flags(vk::DeviceCreateFlags::empty())
            .queue_create_infos(&queue_create_infos)
            .enabled_extension_names(&extension_names_raw)
            .enabled_features(&physical_device_features)
            .push_next(&mut physical_device_features_1_1)
            .push_next(&mut physical_device_features_1_2)
            .push_next(&mut physical_device_features_1_3)
            .build();

        let vk_device_result = unsafe {
            instance.vk_instance.create_device(
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
            instance: instance.vk_instance.clone(),
            device: vk_device.clone(),
            physical_device: vk_physical_device.vk_physical_device,
            debug_settings: Default::default(),
            buffer_device_address: false,
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
            name: vk_physical_device.name.clone(),
            instance: instance.clone(),
            vk_device,
            vk_physical_device: vk_physical_device.vk_physical_device,
            gpu_allocator,
            compute_queues,
            transfer_queues,
            subgroup_size,
            max_compute_work_group_size,
            is_dynamic_subgroup_size,
            queue_index,
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
