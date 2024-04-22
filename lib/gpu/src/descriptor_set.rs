use std::sync::Arc;

use ash::vk;

use crate::*;

pub struct DescriptorBuilder {
    pub descriptor_set_layout: Arc<DescriptorSetLayout>,
    pub uniform_buffers: Vec<(usize, Arc<Buffer>)>,
    pub storage_buffers: Vec<(usize, Arc<Buffer>)>,
}

#[derive(Clone)]
pub struct DescriptorSet {
    pub device: Arc<Device>,
    pub layout: Arc<DescriptorSetLayout>,
    pub uniform_buffers: Vec<(usize, Arc<Buffer>)>,
    pub storage_buffers: Vec<(usize, Arc<Buffer>)>,
    pub vk_descriptor_pool: vk::DescriptorPool,
    pub vk_descriptor_set: vk::DescriptorSet,
    pub vk_descriptor_set_layouts: Vec<vk::DescriptorSetLayout>,
}

impl Resource for DescriptorSet {}

impl DescriptorBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize, uniform_buffer: Arc<Buffer>) -> Self {
        self.uniform_buffers.push((binding, uniform_buffer));
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize, storage_buffer: Arc<Buffer>) -> Self {
        self.storage_buffers.push((binding, storage_buffer));
        self
    }

    pub fn build(&self) -> Arc<DescriptorSet> {
        Arc::new(DescriptorSet::new(
            self.descriptor_set_layout.device.clone(),
            self.descriptor_set_layout.clone(),
            self.uniform_buffers.clone(),
            self.storage_buffers.clone(),
        ))
    }
}

impl Drop for DescriptorSet {
    fn drop(&mut self) {
        unsafe {
            if self.vk_descriptor_pool != vk::DescriptorPool::null() {
                self.device
                    .vk_device
                    .destroy_descriptor_pool(self.vk_descriptor_pool, self.device.alloc());
                self.vk_descriptor_pool = vk::DescriptorPool::null()
            }
        }
        self.uniform_buffers.clear();
        self.storage_buffers.clear();
    }
}

impl DescriptorSet {
    pub fn builder(descriptor_set_layout: Arc<DescriptorSetLayout>) -> DescriptorBuilder {
        DescriptorBuilder {
            descriptor_set_layout,
            uniform_buffers: Vec::new(),
            storage_buffers: Vec::new(),
        }
    }

    pub fn new(
        device: Arc<Device>,
        layout: Arc<DescriptorSetLayout>,
        uniform_buffers: Vec<(usize, Arc<Buffer>)>,
        storage_buffers: Vec<(usize, Arc<Buffer>)>,
    ) -> Self {
        let vk_descriptor_pool =
            Self::create_vk_descriptor_pool(&device, &uniform_buffers, &storage_buffers);
        let vk_descriptor_set_layouts = vec![layout.vk_descriptor_set_layout];
        let vk_descriptor_set =
            Self::create_vk_descriptor_set(&device, &vk_descriptor_set_layouts, vk_descriptor_pool);
        let result = Self {
            device,
            layout,
            uniform_buffers,
            storage_buffers,
            vk_descriptor_pool,
            vk_descriptor_set,
            vk_descriptor_set_layouts,
        };
        result.update();
        result
    }

    fn create_vk_descriptor_pool(
        device: &Arc<Device>,
        uniform_buffers: &[(usize, Arc<Buffer>)],
        storage_buffers: &[(usize, Arc<Buffer>)],
    ) -> vk::DescriptorPool {
        let mut vk_descriptor_pool_sizes = Vec::new();
        if !uniform_buffers.is_empty() {
            vk_descriptor_pool_sizes.push(
                vk::DescriptorPoolSize::builder()
                    .ty(vk::DescriptorType::UNIFORM_BUFFER)
                    .descriptor_count(uniform_buffers.len() as u32)
                    .build(),
            );
        }
        if !storage_buffers.is_empty() {
            vk_descriptor_pool_sizes.push(
                vk::DescriptorPoolSize::builder()
                    .ty(vk::DescriptorType::STORAGE_BUFFER)
                    .descriptor_count(storage_buffers.len() as u32)
                    .build(),
            );
        }

        if !vk_descriptor_pool_sizes.is_empty() {
            let vk_descriptor_pool_create_info = vk::DescriptorPoolCreateInfo::builder()
                .pool_sizes(&vk_descriptor_pool_sizes)
                .max_sets(1)
                .build();
            unsafe {
                device
                    .vk_device
                    .create_descriptor_pool(&vk_descriptor_pool_create_info, device.alloc())
                    .unwrap()
            }
        } else {
            panic!("empty DescriptorSet")
        }
    }

    fn create_vk_descriptor_set(
        device: &Arc<Device>,
        vk_descriptor_set_layout: &[vk::DescriptorSetLayout],
        vk_descriptor_pool: vk::DescriptorPool,
    ) -> vk::DescriptorSet {
        let vk_descriptor_set_allocate_info = vk::DescriptorSetAllocateInfo::builder()
            .descriptor_pool(vk_descriptor_pool)
            .set_layouts(vk_descriptor_set_layout)
            .build();
        unsafe {
            *device
                .vk_device
                .allocate_descriptor_sets(&vk_descriptor_set_allocate_info)
                .unwrap()
                .first()
                .unwrap()
        }
    }

    fn update(&self) {
        let mut vk_write_descriptor_sets = Vec::new();
        let mut vk_descriptor_uniform_buffer_infos = Vec::new();
        for (binding, uniform_buffer) in &self.uniform_buffers {
            let vk_descriptor_buffer_info = vk::DescriptorBufferInfo::builder()
                .buffer(uniform_buffer.vk_buffer)
                .offset(0)
                .range(uniform_buffer.size as u64)
                .build();
            vk_descriptor_uniform_buffer_infos.push(vec![vk_descriptor_buffer_info]);
            let vk_write_descriptor_set = vk::WriteDescriptorSet::builder()
                .dst_set(self.vk_descriptor_set)
                .dst_binding(*binding as u32)
                .dst_array_element(0)
                .descriptor_type(vk::DescriptorType::UNIFORM_BUFFER)
                .buffer_info(vk_descriptor_uniform_buffer_infos.last().unwrap())
                .build();
            vk_write_descriptor_sets.push(vk_write_descriptor_set);
        }

        let mut vk_descriptor_storage_buffer_infos = Vec::new();
        for (binding, storage_buffer) in &self.storage_buffers {
            let vk_descriptor_buffer_info = vk::DescriptorBufferInfo::builder()
                .buffer(storage_buffer.vk_buffer)
                .offset(0)
                .range(storage_buffer.size as u64)
                .build();
            vk_descriptor_storage_buffer_infos.push(vec![vk_descriptor_buffer_info]);
            let vk_write_descriptor_set = vk::WriteDescriptorSet::builder()
                .dst_set(self.vk_descriptor_set)
                .dst_binding(*binding as u32)
                .dst_array_element(0)
                .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                .buffer_info(vk_descriptor_storage_buffer_infos.last().unwrap())
                .build();
            vk_write_descriptor_sets.push(vk_write_descriptor_set);
        }

        unsafe {
            self.device
                .vk_device
                .update_descriptor_sets(&vk_write_descriptor_sets, &[]);
        }
    }
}
