use std::sync::Arc;

use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};

#[repr(C)]
struct GpuLinksParamsBuffer {
    m: u32,
    links_capacity: u32,
}

pub struct GpuLinks {
    pub m: usize,
    pub links_capacity: usize,
    pub points_count: usize,
    pub max_patched_points: usize,
    pub links: Vec<PointOffsetType>,
    pub device: Arc<gpu::Device>,
    pub links_buffer: Arc<gpu::Buffer>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub patch_buffer: Arc<gpu::Buffer>,
    pub patched_points: Vec<(PointOffsetType, usize)>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuLinks {
    pub fn new(
        device: Arc<gpu::Device>,
        m: usize,
        links_capacity: usize,
        points_count: usize,
        max_patched_points: usize,
    ) -> OperationResult<Self> {
        let links_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            points_count * (links_capacity + 1) * std::mem::size_of::<PointOffsetType>(),
        ));
        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        ));
        let links_patch_capacity =
            max_patched_points * (links_capacity + 1) * std::mem::size_of::<PointOffsetType>();
        let patch_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            links_patch_capacity + std::mem::size_of::<GpuLinksParamsBuffer>(),
        ));

        let params = GpuLinksParamsBuffer {
            m: m as u32,
            links_capacity: links_capacity as u32,
        };
        patch_buffer.upload(&params, 0);

        let mut upload_context = gpu::Context::new(device.clone());
        upload_context.copy_gpu_buffer(
            patch_buffer.clone(),
            params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        );
        upload_context.clear_buffer(links_buffer.clone());
        upload_context.run();
        upload_context.wait_finish();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone())
            .add_storage_buffer(1, links_buffer.clone())
            .build();

        Ok(Self {
            m,
            links_capacity,
            points_count,
            max_patched_points,
            links: vec![0; points_count * (links_capacity + 1)],
            device,
            links_buffer,
            params_buffer,
            patch_buffer,
            patched_points: vec![],
            descriptor_set_layout,
            descriptor_set,
        })
    }

    pub fn upload(&self, gpu_context: &mut gpu::Context, count: usize) {
        let upload_size =
            count * (self.links_capacity + 1) * std::mem::size_of::<PointOffsetType>();
        let staging_buffer = Arc::new(gpu::Buffer::new(
            self.device.clone(),
            gpu::BufferType::CpuToGpu,
            upload_size,
        ));
        staging_buffer.upload_slice(&self.links[0..count * (self.links_capacity + 1)], 0);

        gpu_context.copy_gpu_buffer(
            staging_buffer.clone(),
            self.links_buffer.clone(),
            0,
            0,
            upload_size,
        );
        gpu_context.run();
        gpu_context.wait_finish();
    }

    pub fn update_params(&mut self, context: &mut gpu::Context, m: usize) {
        self.m = m;

        let params = GpuLinksParamsBuffer {
            m: m as u32,
            links_capacity: self.links_capacity as u32,
        };
        let links_patch_capacity = self.max_patched_points
            * (self.links_capacity + 1)
            * std::mem::size_of::<PointOffsetType>();
        self.patch_buffer.upload(&params, links_patch_capacity);

        context.copy_gpu_buffer(
            self.patch_buffer.clone(),
            self.params_buffer.clone(),
            links_patch_capacity,
            0,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        );
    }

    pub fn clear(&mut self, gpu_context: &mut gpu::Context) -> OperationResult<()> {
        if !self.patched_points.is_empty() {
            return Err(OperationError::service_error(
                "Gpu links patches are not empty",
            ));
        }
        gpu_context.clear_buffer(self.links_buffer.clone());
        self.links = vec![0; self.links.len()];
        Ok(())
    }

    pub fn apply_gpu_patches(&mut self, gpu_context: &mut gpu::Context) {
        for (i, &(patched_point_id, patched_links_count)) in self.patched_points.iter().enumerate()
        {
            let patch_start_index =
                i * (self.links_capacity + 1) * std::mem::size_of::<PointOffsetType>();
            let patch_size = (patched_links_count + 1) * std::mem::size_of::<PointOffsetType>();
            let links_start_index = patched_point_id as usize
                * (self.links_capacity + 1)
                * std::mem::size_of::<PointOffsetType>();
            gpu_context.copy_gpu_buffer(
                self.patch_buffer.clone(),
                self.links_buffer.clone(),
                patch_start_index,
                links_start_index,
                patch_size,
            );
        }
        self.patched_points.clear();
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        let start_index = point_id as usize * (self.links_capacity + 1);
        let len = self.links[start_index] as usize;
        &self.links[start_index + 1..start_index + 1 + len]
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        links: &[PointOffsetType],
    ) -> OperationResult<()> {
        if self.patched_points.len() >= self.max_patched_points {
            return Err(OperationError::service_error("Gpu links patches are full"));
        }
        let start_index = point_id as usize * (self.links_capacity + 1);
        self.links[start_index] = links.len() as PointOffsetType;
        self.links[start_index + 1..start_index + 1 + links.len()].copy_from_slice(links);

        self.patched_points.push((point_id, links.len()));
        let mut patch_start_index = self.patched_points.len()
            * (self.links_capacity + 1)
            * std::mem::size_of::<PointOffsetType>();
        self.patch_buffer
            .upload(&(links.len() as u32), patch_start_index);
        patch_start_index += std::mem::size_of::<PointOffsetType>();
        self.patch_buffer.upload_slice(links, patch_start_index);

        Ok(())
    }
}
