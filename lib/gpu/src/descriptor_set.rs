use std::sync::Arc;

use ash::vk;

use crate::*;

/// `DescriptorSet` is a collection of buffers that can be bound to a shader.
/// It depends on a DescriptorSetLayout which defines linkage to the shader.
/// This structure does not need shader directly, shader will be provided by `Pipeline`.
/// It can be reused between different pipelines and shaders with the same layout.
#[derive(Clone)]
pub struct DescriptorSet {
    /// Device that owns the descriptor set.
    device: Arc<Device>,

    /// Descriptor set layout that defines linkage to the shader.
    /// Keep it alive to prevent it from being dropped.
    _layout: Arc<DescriptorSetLayout>,

    /// Collection of uniform buffers.
    uniform_buffers: Vec<(usize, Arc<Buffer>)>,

    /// Collection of storage buffers.
    storage_buffers: Vec<(usize, Arc<Buffer>)>,

    /// Native Vulkan descriptor pool handle.
    vk_descriptor_pool: vk::DescriptorPool,

    /// Native Vulkan descriptor set handle.
    vk_descriptor_set: vk::DescriptorSet,
}

pub struct DescriptorSetBuilder {
    descriptor_set_layout: Arc<DescriptorSetLayout>,
    uniform_buffers: Vec<(usize, Arc<Buffer>)>,
    storage_buffers: Vec<(usize, Arc<Buffer>)>,
}

// Mark `DescriptorSet` as a GPU resource that should be kept alive while it's in use by the GPU context.
impl Resource for DescriptorSet {}

impl DescriptorSetBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize, uniform_buffer: Arc<Buffer>) -> Self {
        self.uniform_buffers.push((binding, uniform_buffer));
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize, storage_buffer: Arc<Buffer>) -> Self {
        self.storage_buffers.push((binding, storage_buffer));
        self
    }

    pub fn build(&self) -> GpuResult<Arc<DescriptorSet>> {
        DescriptorSet::new(
            self.descriptor_set_layout.device().clone(),
            self.descriptor_set_layout.clone(),
            self.uniform_buffers.clone(),
            self.storage_buffers.clone(),
        )
    }
}

impl DescriptorSet {
    pub fn builder(descriptor_set_layout: Arc<DescriptorSetLayout>) -> DescriptorSetBuilder {
        DescriptorSetBuilder {
            descriptor_set_layout,
            uniform_buffers: Vec::new(),
            storage_buffers: Vec::new(),
        }
    }

    fn new(
        device: Arc<Device>,
        layout: Arc<DescriptorSetLayout>,
        uniform_buffers: Vec<(usize, Arc<Buffer>)>,
        storage_buffers: Vec<(usize, Arc<Buffer>)>,
    ) -> GpuResult<Arc<Self>> {
        // Vulkan descriptor pool is required to allocate descriptor sets.
        let vk_descriptor_pool =
            Self::create_vk_descriptor_pool(&device, &uniform_buffers, &storage_buffers)?;

        // Create Vulkan descriptor set.
        let vk_descriptor_set_layouts = vec![layout.vk_descriptor_set_layout()];
        let vk_descriptor_set_result =
            Self::create_vk_descriptor_set(&device, &vk_descriptor_set_layouts, vk_descriptor_pool);

        let vk_descriptor_set = match vk_descriptor_set_result {
            Ok(vk_descriptor_set) => vk_descriptor_set,
            Err(error) => {
                unsafe {
                    // Destroy descriptor pool if descriptor set creation failed.
                    device.vk_device().destroy_descriptor_pool(
                        vk_descriptor_pool,
                        device.cpu_allocation_callbacks(),
                    );
                }
                return Err(error);
            }
        };

        let result = Arc::new(Self {
            device,
            _layout: layout,
            uniform_buffers,
            storage_buffers,
            vk_descriptor_pool,
            vk_descriptor_set,
        });
        result.update()?;
        Ok(result)
    }

    pub fn vk_descriptor_set(&self) -> vk::DescriptorSet {
        self.vk_descriptor_set
    }

    fn create_vk_descriptor_pool(
        device: &Arc<Device>,
        uniform_buffers: &[(usize, Arc<Buffer>)],
        storage_buffers: &[(usize, Arc<Buffer>)],
    ) -> GpuResult<vk::DescriptorPool> {
        let mut vk_descriptor_pool_sizes = Vec::new();
        if !uniform_buffers.is_empty() {
            vk_descriptor_pool_sizes.push(
                vk::DescriptorPoolSize::default()
                    .ty(vk::DescriptorType::UNIFORM_BUFFER)
                    .descriptor_count(uniform_buffers.len() as u32),
            );
        }
        if !storage_buffers.is_empty() {
            vk_descriptor_pool_sizes.push(
                vk::DescriptorPoolSize::default()
                    .ty(vk::DescriptorType::STORAGE_BUFFER)
                    .descriptor_count(storage_buffers.len() as u32),
            );
        }

        if !vk_descriptor_pool_sizes.is_empty() {
            let vk_descriptor_pool_create_info = vk::DescriptorPoolCreateInfo::default()
                .pool_sizes(&vk_descriptor_pool_sizes)
                .max_sets(1);
            unsafe {
                device
                    .vk_device()
                    .create_descriptor_pool(
                        &vk_descriptor_pool_create_info,
                        device.cpu_allocation_callbacks(),
                    )
                    .map_err(GpuError::from)
            }
        } else {
            Err(GpuError::Other(
                "DescriptorSet must have at least one uniform or storage buffer".to_string(),
            ))
        }
    }

    fn create_vk_descriptor_set(
        device: &Arc<Device>,
        vk_descriptor_set_layout: &[vk::DescriptorSetLayout],
        vk_descriptor_pool: vk::DescriptorPool,
    ) -> GpuResult<vk::DescriptorSet> {
        let vk_descriptor_set_allocate_info = vk::DescriptorSetAllocateInfo::default()
            .descriptor_pool(vk_descriptor_pool)
            .set_layouts(vk_descriptor_set_layout);
        unsafe {
            Ok(device
                .vk_device()
                .allocate_descriptor_sets(&vk_descriptor_set_allocate_info)?[0])
        }
    }

    fn update(&self) -> GpuResult<()> {
        // Collected parameters for vk::update_descriptor_sets
        let mut vk_write_descriptor_sets: Vec<vk::WriteDescriptorSet> = vec![];

        // `vk_descriptor_uniform_buffer_infos` is a collection of uniform buffers.
        // It should be alive because `vk_write_descriptor_sets` references pointer to it.
        let mut vk_descriptor_uniform_buffer_infos = Vec::new();
        for (_binding, uniform_buffer) in &self.uniform_buffers {
            if uniform_buffer.buffer_type() != BufferType::Uniform {
                return Err(GpuError::Other(
                    "Uniform buffer type must be `BufferType::Uniform`".to_string(),
                ));
            }
            let vk_descriptor_buffer_info = vk::DescriptorBufferInfo::default()
                .buffer(uniform_buffer.vk_buffer())
                .offset(0)
                .range(uniform_buffer.size() as u64);
            vk_descriptor_uniform_buffer_infos.push(vec![vk_descriptor_buffer_info]);
        }

        for ((binding, _uniform_buffer), buffer_infos) in self
            .uniform_buffers
            .iter()
            .zip(vk_descriptor_uniform_buffer_infos.iter())
        {
            let vk_write_descriptor_set = vk::WriteDescriptorSet::default()
                .dst_set(self.vk_descriptor_set)
                .dst_binding(*binding as u32)
                .dst_array_element(0)
                .descriptor_type(vk::DescriptorType::UNIFORM_BUFFER)
                .buffer_info(buffer_infos);
            vk_write_descriptor_sets.push(vk_write_descriptor_set);
        }

        // `vk_descriptor_storage_buffer_infos` is a collection of storage buffers.
        // It should be alive because `vk_write_descriptor_sets` references pointer to it.
        let mut vk_descriptor_storage_buffer_infos = Vec::new();
        for (_binding, storage_buffer) in &self.storage_buffers {
            if storage_buffer.buffer_type() != BufferType::Storage {
                return Err(GpuError::Other(
                    "Storage buffer type must be `BufferType::Storage`".to_string(),
                ));
            }

            let vk_descriptor_buffer_info = vk::DescriptorBufferInfo::default()
                .buffer(storage_buffer.vk_buffer())
                .offset(0)
                .range(storage_buffer.size() as u64);
            vk_descriptor_storage_buffer_infos.push(vec![vk_descriptor_buffer_info]);
        }

        for ((binding, _storage_buffer), buffer_info) in self
            .storage_buffers
            .iter()
            .zip(vk_descriptor_storage_buffer_infos.iter())
        {
            let vk_write_descriptor_set = vk::WriteDescriptorSet::default()
                .dst_set(self.vk_descriptor_set)
                .dst_binding(*binding as u32)
                .dst_array_element(0)
                .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                .buffer_info(buffer_info);
            vk_write_descriptor_sets.push(vk_write_descriptor_set);
        }

        unsafe {
            self.device
                .vk_device()
                .update_descriptor_sets(&vk_write_descriptor_sets, &[]);
        }

        Ok(())
    }
}

impl Drop for DescriptorSet {
    fn drop(&mut self) {
        if self.vk_descriptor_pool != vk::DescriptorPool::null() {
            unsafe {
                self.device.vk_device().destroy_descriptor_pool(
                    self.vk_descriptor_pool,
                    self.device.cpu_allocation_callbacks(),
                );
            }
            self.vk_descriptor_pool = vk::DescriptorPool::null()
        }
        self.uniform_buffers.clear();
        self.storage_buffers.clear();
    }
}
