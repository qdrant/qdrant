use std::sync::Arc;

use crate::entry::entry_point::OperationResult;
use crate::types::PointOffsetType;

#[repr(C)]
struct GpuLinksParamsBuffer {
    m: u32,
    ef: u32,
}

pub struct GpuLinks {
    pub m: usize,
    pub ef: usize,
    pub points_count: usize,
    pub links: Vec<PointOffsetType>,
    pub device: Arc<gpu::Device>,
    pub links_buffer: Arc<gpu::Buffer>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuLinks {
    pub fn new(
        device: Arc<gpu::Device>,
        m: usize,
        ef: usize,
        points_count: usize,
    ) -> OperationResult<Self> {
        let links_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            points_count * (m + 1) * std::mem::size_of::<PointOffsetType>(),
        ));
        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        ));

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            std::mem::size_of::<GpuLinksParamsBuffer>(),
        ));

        let params = GpuLinksParamsBuffer {
            m: m as u32,
            ef: ef as u32,
        };
        staging_buffer.upload(&params, 0);

        let mut upload_context = gpu::Context::new(device.clone());
        upload_context.copy_gpu_buffer(
            staging_buffer.clone(),
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
            ef,
            points_count,
            links: vec![0; points_count * (m + 1)],
            device,
            links_buffer,
            params_buffer,
            descriptor_set_layout,
            descriptor_set,
        })
    }

    pub fn upload(&mut self, count: usize) {
        let upload_size = count * (self.m + 1) * std::mem::size_of::<PointOffsetType>();
        let staging_buffer = Arc::new(gpu::Buffer::new(
            self.device.clone(),
            gpu::BufferType::CpuToGpu,
            upload_size,
        ));
        staging_buffer.upload_slice(&self.links[0..count * (self.m + 1)], 0);

        let mut upload_context = gpu::Context::new(self.device.clone());
        upload_context.copy_gpu_buffer(
            staging_buffer.clone(),
            self.links_buffer.clone(),
            0,
            0,
            upload_size,
        );
        upload_context.run();
        upload_context.wait_finish();
    }

    pub fn download(&mut self) {
        let timer = std::time::Instant::now();

        let chunk_size = 10_000_000;
        let staging_buffer = Arc::new(gpu::Buffer::new(
            self.device.clone(),
            gpu::BufferType::GpuToCpu,
            chunk_size * std::mem::size_of::<PointOffsetType>(),
        ));

        let mut context = gpu::Context::new(self.device.clone());
        let mut chunk_begin = 0;
        while chunk_begin < self.links.len() {
            let chunk_end = std::cmp::min(chunk_begin + chunk_size, self.links.len());

            context.copy_gpu_buffer(
                self.links_buffer.clone(),
                staging_buffer.clone(),
                chunk_begin * std::mem::size_of::<PointOffsetType>(),
                0,
                chunk_end - chunk_begin,
            );
            context.run();
            context.wait_finish();

            let slice = &mut self.links[chunk_begin..chunk_end];
            staging_buffer.download_slice(slice, 0);

            chunk_begin += chunk_size;
        }

        log::debug!(
            "Download links from GPU time = {:?}, links data size {} MB",
            timer.elapsed(),
            self.links.len() * std::mem::size_of::<PointOffsetType>() / 1024 / 1024
        );
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        let start_index = point_id as usize * (self.m + 1);
        let len = self.links[start_index] as usize;
        &self.links[start_index + 1..start_index + 1 + len]
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        let start_index = point_id as usize * (self.m + 1);
        self.links[start_index] = links.len() as PointOffsetType;
        self.links[start_index + 1..start_index + 1 + links.len()].copy_from_slice(links);
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::types::PointOffsetType;

    fn generate_random_links(m: usize, points_count: usize) -> Vec<Vec<PointOffsetType>> {
        let mut rnd = StdRng::seed_from_u64(42);
        let mut result = vec![];
        for _ in 0..points_count {
            let links_count = rnd.gen_range(1..m);
            let rnd_links = (0..links_count)
                .map(|_| rnd.gen_range(0..points_count as PointOffsetType))
                .collect::<Vec<_>>();
            result.push(rnd_links);
        }
        result
    }

    #[test]
    fn test_gpu_links_sorting() {
        let m = 8;
        let ef = 8;
        let points_count = 1_000_000;
        let fill_count = 100_000;

        let generated_links = generate_random_links(m, fill_count);

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let mut gpu_links = GpuLinks::new(device.clone(), m, ef, points_count).unwrap();

        for (i, links) in generated_links.iter().enumerate() {
            gpu_links.set_links(i as PointOffsetType, &links);
        }

        gpu_links.upload(fill_count);

        // test 1: download and check that links are same
        gpu_links.download();
        for (i, links) in generated_links.iter().enumerate() {
            let gpu_links = gpu_links.get_links(i as PointOffsetType);
            assert_eq!(gpu_links, links);
        }

        // test 2: run shader that sorts links and check that links are sorted
        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/test_links.spv"),
        ));

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_links.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(pipeline, &[gpu_links.descriptor_set.clone()]);
        context.dispatch(points_count, 1, 1);
        context.run();
        context.wait_finish();

        gpu_links.download();
        for (i, links) in generated_links.iter().enumerate() {
            let mut links = links.to_owned();
            links.sort();
            links.reverse();
            let gpu_links = gpu_links.get_links(i as PointOffsetType);
            assert_eq!(gpu_links, links);
        }
    }
}
