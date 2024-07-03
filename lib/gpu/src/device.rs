use std::ffi::CString;
use std::ptr;
use std::sync::{Arc, Mutex};

use ash::vk;
use gpu_allocator::vulkan::{Allocation, AllocationCreateDesc, Allocator, AllocatorCreateDesc};

use crate::*;

#[derive(Clone)]
pub struct Queue {
    pub vk_queue: vk::Queue,
    pub vk_queue_family_index: usize,
    pub vk_queue_index: usize,
}

pub struct Device {
    pub instance: Arc<Instance>,
    pub vk_device: ash::Device,
    pub vk_physical_device: vk::PhysicalDevice,
    pub gpu_allocator: Option<Mutex<Allocator>>,
    pub compute_queues: Vec<Queue>,
    pub transfer_queues: Vec<Queue>,
    pub subgroup_size: usize,
    pub compiler: shaderc::Compiler,
}

impl Device {
    pub fn new(instance: Arc<Instance>, vk_physical_device: vk::PhysicalDevice) -> Option<Device> {
        let compiler = shaderc::Compiler::new().unwrap();

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
                .get_physical_device_queue_family_properties(vk_physical_device)
        };

        let max_queue_priorities_count = vk_queue_families
            .iter()
            .map(|vk_queue_family| vk_queue_family.queue_count as usize)
            .max()?;
        let queue_priorities = vec![0.; max_queue_priorities_count];

        let queue_create_infos: Vec<vk::DeviceQueueCreateInfo> = vk_queue_families
            .iter()
            .enumerate()
            .map(|(index, vk_queue_family)| vk::DeviceQueueCreateInfo {
                s_type: vk::StructureType::DEVICE_QUEUE_CREATE_INFO,
                p_next: ptr::null(),
                flags: vk::DeviceQueueCreateFlags::empty(),
                queue_family_index: index as u32,
                p_queue_priorities: queue_priorities.as_ptr(),
                queue_count: vk_queue_family.queue_count,
            })
            .collect();

        let physical_device_features = vk::PhysicalDeviceFeatures {
            ..Default::default()
        };

        // TODO(gpu): check presence of features
        let mut physical_device_features_1_2 = vk::PhysicalDeviceVulkan12Features::builder()
            .shader_int8(true)
            .shader_float16(true)
            .storage_buffer8_bit_access(true)
            .build();

        let mut physical_device_features_1_1 = vk::PhysicalDeviceVulkan11Features::builder()
            .storage_buffer16_bit_access(true)
            .build();

        let extension_names_raw: Vec<*const i8> = extensions_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        let device_create_info = vk::DeviceCreateInfo::builder()
            .flags(vk::DeviceCreateFlags::empty())
            .queue_create_infos(&queue_create_infos)
            .enabled_extension_names(&extension_names_raw)
            .enabled_features(&physical_device_features)
            .push_next(&mut physical_device_features_1_2)
            .push_next(&mut physical_device_features_1_1)
            .build();

        let vk_device = unsafe {
            instance.vk_instance.create_device(
                vk_physical_device,
                &device_create_info,
                instance.alloc.as_ref(),
            )
        };

        let subgroup_size = unsafe {
            let props = instance
                .vk_instance
                .get_physical_device_properties(vk_physical_device);
            let mut subgroup_properties = vk::PhysicalDeviceSubgroupProperties::default();
            let mut props2 = vk::PhysicalDeviceProperties2::builder()
                .push_next(&mut subgroup_properties)
                .build();
            instance
                .vk_instance
                .get_physical_device_properties2(vk_physical_device, &mut props2);
            log::info!(
                "Choosed GPU evice name: {:?}",
                ::std::ffi::CStr::from_ptr(props.device_name.as_ptr())
            );
            log::debug!("GPU subgroup size: {:?}", subgroup_properties.subgroup_size);
            subgroup_properties.subgroup_size as usize
        };

        let mut compute_queues = Vec::new();
        let mut transfer_queues = Vec::new();
        if let Ok(vk_device) = vk_device {
            for (vk_queue_family_index, vk_queue_family) in vk_queue_families.iter().enumerate() {
                for vk_queue_index in 0..vk_queue_family.queue_count as usize {
                    let vk_queue = unsafe {
                        vk_device
                            .get_device_queue(vk_queue_family_index as u32, vk_queue_index as u32)
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

            let gpu_allocator = Some(Mutex::new(
                Allocator::new(&AllocatorCreateDesc {
                    instance: instance.vk_instance.clone(),
                    device: vk_device.clone(),
                    physical_device: vk_physical_device,
                    debug_settings: Default::default(),
                    buffer_device_address: false,
                })
                .ok()?,
            ));

            Some(Device {
                instance: instance.clone(),
                vk_device,
                vk_physical_device,
                gpu_allocator,
                compute_queues,
                transfer_queues,
                subgroup_size,
                compiler,
            })
        } else {
            None
        }
    }

    pub fn alloc(&self) -> Option<&vk::AllocationCallbacks> {
        self.instance.alloc.as_ref()
    }

    pub fn gpu_alloc(&self, allocation_desc: &AllocationCreateDesc) -> GpuResult<Allocation> {
        let mut gpu_allocator = self.gpu_allocator.as_ref().unwrap().lock().unwrap();
        gpu_allocator
            .allocate(allocation_desc)
            .map_err(GpuError::AllocationError)
    }

    pub fn gpu_free(&self, allocation: Allocation) {
        let mut gpu_allocator = self.gpu_allocator.as_ref().unwrap().lock().unwrap();
        gpu_allocator.free(allocation).unwrap();
    }

    pub fn subgroup_size(&self) -> usize {
        self.subgroup_size
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        self.gpu_allocator = None;
        unsafe {
            self.vk_device.device_wait_idle().unwrap();
            self.vk_device.destroy_device(self.alloc());
        }
    }
}
