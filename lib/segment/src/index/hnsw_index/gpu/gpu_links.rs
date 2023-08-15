use std::collections::HashMap;
use std::sync::Arc;

use common::types::PointOffsetType;

use super::shader_builder::ShaderBuilderParameters;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::gpu::GPU_TIMEOUT;
use crate::index::hnsw_index::graph_layers::GraphLayersBase;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;

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
    pub device: Arc<gpu::Device>,
    pub links_buffer: Arc<gpu::Buffer>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub patch_buffer: Arc<gpu::Buffer>,
    pub patched_points: Vec<(PointOffsetType, usize)>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl ShaderBuilderParameters for GpuLinks {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "iterators.comp".to_string(),
                include_str!("shaders/iterators.comp").to_string(),
            ),
            (
                "links.comp".to_string(),
                include_str!("shaders/links.comp").to_string(),
            ),
        ])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        defines.insert(
            "LINKS_CAPACITY".to_owned(),
            Some(self.links_capacity.to_string()),
        );
        defines
    }
}

impl GpuLinks {
    pub fn new(
        device: Arc<gpu::Device>,
        m: usize,
        links_capacity: usize,
        points_count: usize,
        max_patched_points: usize,
    ) -> gpu::GpuResult<Self> {
        let links_buffer = gpu::Buffer::new(
            device.clone(),
            "Links buffer",
            gpu::BufferType::Storage,
            points_count * (links_capacity + 1) * std::mem::size_of::<PointOffsetType>(),
        )?;
        let params_buffer = gpu::Buffer::new(
            device.clone(),
            "Links params buffer",
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        )?;
        let links_patch_capacity =
            max_patched_points * (links_capacity + 1) * std::mem::size_of::<PointOffsetType>();
        let patch_buffer = gpu::Buffer::new(
            device.clone(),
            "Links patch buffer",
            gpu::BufferType::CpuToGpu,
            links_patch_capacity + std::mem::size_of::<GpuLinksParamsBuffer>(),
        )?;

        let params = GpuLinksParamsBuffer {
            m: m as u32,
            links_capacity: links_capacity as u32,
        };
        patch_buffer.upload(&params, 0)?;

        let mut upload_context = gpu::Context::new(device.clone())?;
        upload_context.copy_gpu_buffer(
            patch_buffer.clone(),
            params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        )?;
        upload_context.clear_buffer(links_buffer.clone())?;
        upload_context.run()?;
        upload_context.wait_finish(GPU_TIMEOUT)?;

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone())?;

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone())
            .add_storage_buffer(1, links_buffer.clone())
            .build()?;

        Ok(Self {
            m,
            links_capacity,
            points_count,
            max_patched_points,
            device,
            links_buffer,
            params_buffer,
            patch_buffer,
            patched_points: vec![],
            descriptor_set_layout,
            descriptor_set,
        })
    }

    pub fn update_params(&mut self, context: &mut gpu::Context, m: usize) -> OperationResult<()> {
        self.m = m;

        let params = GpuLinksParamsBuffer {
            m: m as u32,
            links_capacity: self.links_capacity as u32,
        };
        let links_patch_capacity = self.max_patched_points
            * (self.links_capacity + 1)
            * std::mem::size_of::<PointOffsetType>();
        self.patch_buffer.upload(&params, links_patch_capacity)?;

        context.copy_gpu_buffer(
            self.patch_buffer.clone(),
            self.params_buffer.clone(),
            links_patch_capacity,
            0,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        )?;
        Ok(())
    }

    pub fn clear(&mut self, gpu_context: &mut gpu::Context) -> OperationResult<()> {
        if !self.patched_points.is_empty() {
            self.patched_points.clear();
        }
        gpu_context.clear_buffer(self.links_buffer.clone())?;
        Ok(())
    }

    pub fn apply_gpu_patches(&mut self, gpu_context: &mut gpu::Context) -> OperationResult<()> {
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
            )?;
        }
        self.patched_points.clear();
        Ok(())
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        links: &[PointOffsetType],
    ) -> OperationResult<()> {
        if self.patched_points.len() >= self.max_patched_points {
            return Err(OperationError::service_error("Gpu links patches are full"));
        }

        let mut patch_start_index = self.patched_points.len()
            * (self.links_capacity + 1)
            * std::mem::size_of::<PointOffsetType>();
        self.patch_buffer
            .upload(&(links.len() as u32), patch_start_index)?;
        patch_start_index += std::mem::size_of::<PointOffsetType>();
        self.patch_buffer.upload_slice(links, patch_start_index)?;
        self.patched_points.push((point_id, links.len()));

        Ok(())
    }

    pub fn upload_links(
        &mut self,
        level: usize,
        graph_layers_builder: &GraphLayersBuilder,
        context: &mut gpu::Context,
    ) -> OperationResult<()> {
        self.update_params(context, graph_layers_builder.get_m(level))?;
        self.clear(context)?;
        context.run()?;
        context.wait_finish(GPU_TIMEOUT)?;

        let timer = std::time::Instant::now();
        let points: Vec<_> = (0..graph_layers_builder.links_layers.len())
            .filter(|&point_id| {
                graph_layers_builder.get_point_level(point_id as PointOffsetType) >= level
            })
            .filter(|&point_id| {
                !graph_layers_builder.links_layers[point_id][level]
                    .read()
                    .is_empty()
            })
            .collect();

        for points_slice in points.chunks(self.max_patched_points) {
            for &point_id in points_slice {
                let links = graph_layers_builder.links_layers[point_id][level].read();
                self.set_links(point_id as PointOffsetType, &links)?;
            }
            self.apply_gpu_patches(context)?;
            context.run()?;
            context.wait_finish(GPU_TIMEOUT)?;
        }

        log::trace!("Upload links on level {level} time: {:?}", timer.elapsed());
        Ok(())
    }

    pub fn download_links(
        &mut self,
        level: usize,
        graph_layers_builder: &GraphLayersBuilder,
        context: &mut gpu::Context,
    ) -> OperationResult<()> {
        let timer = std::time::Instant::now();

        let links_patch_capacity = self.max_patched_points
            * (self.links_capacity + 1)
            * std::mem::size_of::<PointOffsetType>();
        let download_buffer = gpu::Buffer::new(
            self.device.clone(),
            "Download links staging buffer",
            gpu::BufferType::GpuToCpu,
            links_patch_capacity,
        )
        .unwrap();

        let points = (0..graph_layers_builder.links_layers.len() as PointOffsetType)
            .filter(|&point_id| graph_layers_builder.get_point_level(point_id) >= level)
            .collect::<Vec<_>>();

        for chunk_index in 0..points.len().div_ceil(self.max_patched_points) {
            let start = chunk_index * self.max_patched_points;
            let end = (start + self.max_patched_points).min(points.len());
            let chunk_size = end - start;
            for (i, &point_id) in points[start..end].iter().enumerate() {
                let links_size = (self.links_capacity + 1) * std::mem::size_of::<PointOffsetType>();
                context.copy_gpu_buffer(
                    self.links_buffer.clone(),
                    download_buffer.clone(),
                    point_id as usize * links_size,
                    i * links_size,
                    links_size,
                )?;
            }
            context.run()?;
            context.wait_finish(GPU_TIMEOUT)?;

            let mut links =
                vec![PointOffsetType::default(); chunk_size * (self.links_capacity + 1)];
            download_buffer.download_slice(&mut links, 0)?;

            for (index, chunk) in links.chunks(self.links_capacity + 1).enumerate() {
                let point_id = points[start + index] as usize;
                let links_count = chunk[0] as usize;
                let links = &chunk[1..=links_count];
                let mut dst = graph_layers_builder.links_layers[point_id][level].write();
                dst.clear();
                dst.extend_from_slice(links);
            }
        }

        log::trace!(
            "Download links for level {} in time {:?}",
            level,
            timer.elapsed()
        );
        Ok(())
    }
}
