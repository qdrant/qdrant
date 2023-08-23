use std::sync::Arc;

use super::gpu_builder_context::GpuBuilderContext;
use super::gpu_links::GpuLinks;
use super::gpu_search_context::GpuSearchContext;
use super::gpu_vector_storage::GpuVectorStorage;
use crate::types::PointOffsetType;
use crate::vector_storage::VectorStorageEnum;

pub const CANDIDATES_CAPACITY_DIV: usize = 8;

pub struct GpuGraphBuilder {
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub point_levels: Vec<usize>,
    pub gpu_instance: Arc<gpu::Instance>,
    pub gpu_device: Arc<gpu::Device>,
    pub gpu_context: gpu::Context,
    pub gpu_vector_storage: GpuVectorStorage,
    pub gpu_links: GpuLinks,
    pub gpu_search_context: GpuSearchContext,
    pub gpu_builder_context: GpuBuilderContext,
    pub update_entry_pipeline: Arc<gpu::Pipeline>,
    pub link_pipeline: Arc<gpu::Pipeline>,
    pub gpu_threads: usize,
    pub timer: Option<std::time::Instant>,
}

unsafe impl Send for GpuGraphBuilder {}
unsafe impl Sync for GpuGraphBuilder {}

impl GpuGraphBuilder {
    pub fn new(
        num_vectors: usize,
        m: usize,
        m0: usize,
        ef_construct: usize,
        vector_storage: &VectorStorageEnum,
        point_levels: Vec<usize>,
        gpu_threads: usize,
    ) -> Self {
        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let gpu_instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let gpu_device = Arc::new(
            gpu::Device::new(gpu_instance.clone(), gpu_instance.vk_physical_devices[0]).unwrap(),
        );
        let gpu_context = gpu::Context::new(gpu_device.clone());
        let gpu_vector_storage = GpuVectorStorage::new(gpu_device.clone(), vector_storage).unwrap();
        let gpu_links =
            GpuLinks::new(gpu_device.clone(), m, ef_construct, m0, num_vectors).unwrap();

        let candidates_capacity = num_vectors / CANDIDATES_CAPACITY_DIV;
        let gpu_search_context = GpuSearchContext::new(
            gpu_threads,
            num_vectors,
            m0,
            ef_construct,
            candidates_capacity,
            gpu_device.clone(),
        );
        let gpu_builder_context =
            GpuBuilderContext::new(gpu_device.clone(), num_vectors, gpu_threads);

        // init gpu pilelines
        let update_entry_shader = Arc::new(gpu::Shader::new(
            gpu_device.clone(),
            include_bytes!("./shaders/update_entries.spv"),
        ));
        let update_entry_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_builder_context.descriptor_set_layout.clone())
            .add_shader(update_entry_shader.clone())
            .build(gpu_device.clone());

        let link_shader = Arc::new(gpu::Shader::new(
            gpu_device.clone(),
            include_bytes!("./shaders/run_requests.spv"),
        ));
        let link_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_builder_context.descriptor_set_layout.clone())
            .add_shader(link_shader.clone())
            .build(gpu_device.clone());

        Self {
            m,
            m0,
            ef_construct,
            point_levels,
            gpu_instance,
            gpu_device,
            gpu_context,
            gpu_vector_storage,
            gpu_links,
            gpu_search_context,
            gpu_builder_context,
            update_entry_pipeline,
            link_pipeline,
            gpu_threads,
            timer: None,
        }
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        self.gpu_links.get_links(point_id)
    }

    pub fn set_links(&mut self, point_id: PointOffsetType, links: &[PointOffsetType]) {
        self.gpu_links.set_links(point_id, links)
    }

    pub fn clear_links(&mut self) {
        self.gpu_links.clear(&mut self.gpu_context);
    }

    pub fn build_level(
        &mut self,
        entries: Vec<Option<PointOffsetType>>,
        level: usize,
        start_idx: PointOffsetType,
    ) {
        if start_idx == self.num_vectors() as PointOffsetType {
            return;
        }

        let upload_entries_count = if self.gpu_builder_context.has_been_runned() {
            start_idx as usize
        } else {
            self.num_vectors()
        };
        let gpu_entries = entries
            .iter()
            .take(upload_entries_count)
            .cloned()
            .map(|entry| entry.unwrap_or(PointOffsetType::MAX))
            .collect::<Vec<_>>();
        self.gpu_builder_context
            .upload_entries(&mut self.gpu_context, &gpu_entries);

        let level_m = self.get_m(level);
        if level == 0 {
            self.gpu_links
                .update_params(&mut self.gpu_context, level_m, self.ef_construct);
        }
        self.gpu_links
            .upload(&mut self.gpu_context, start_idx as usize);
        let mut link_count = 0;
        let mut update_entry_count = 0;
        let mut update_entry_buffer = vec![];
        let mut links_buffer = vec![];
        for idx in start_idx..self.num_vectors() as PointOffsetType {
            if let Some(entry_point) = entries[idx as usize].clone() {
                let entry_level = self.get_point_level(entry_point);
                let point_level = self.get_point_level(idx as PointOffsetType);
                if level > entry_level && level > point_level {
                    continue;
                }

                let link_level = std::cmp::min(entry_level, point_level);
                if level > link_level && entry_level >= point_level {
                    update_entry_count += 1;
                    update_entry_buffer.push(idx as PointOffsetType);
                    if update_entry_buffer.len() == self.gpu_threads {
                        self.run(&update_entry_buffer, &[]);
                        update_entry_buffer.clear();
                    }
                } else if link_level >= level {
                    link_count += 1;
                    links_buffer.push(idx as PointOffsetType);
                    if links_buffer.len() == self.gpu_threads {
                        self.run(&update_entry_buffer, &links_buffer);
                        update_entry_buffer.clear();
                        links_buffer.clear();
                    }
                }
            }
        }
        self.run(&update_entry_buffer, &links_buffer);
        self.gpu_context.wait_finish();

        self.gpu_links.download(&mut self.gpu_context);

        println!(
            "GPU level {}, links {}, updates {}, start_idx {}",
            level, link_count, update_entry_count, start_idx,
        );
    }

    fn run(&mut self, update_entry_points: &[PointOffsetType], link_points: &[PointOffsetType]) {
        self.gpu_context.wait_finish();
        let visited = self
            .gpu_search_context
            .get_visited_count(&mut self.gpu_context);
        println!(
            "Visited {:?}, point id = {:?}, time = {:?}",
            visited,
            link_points.get(0).cloned().unwrap_or_default(),
            self.timer.map(|t| t.elapsed()).unwrap_or_default()
        );
        self.timer = Some(std::time::Instant::now());

        self.gpu_builder_context.upload_process_points(
            &mut self.gpu_context,
            update_entry_points,
            link_points,
        );

        debug_assert!(update_entry_points.len() <= self.gpu_threads);
        debug_assert!(link_points.len() <= self.gpu_threads);
        if update_entry_points.len() == 0 && link_points.len() == 0 {
            return;
        }

        if link_points.len() > 0 {
            self.gpu_search_context.clear(&mut self.gpu_context);
        }

        if update_entry_points.len() > 0 {
            self.gpu_context.bind_pipeline(
                self.update_entry_pipeline.clone(),
                &[
                    self.gpu_vector_storage.descriptor_set.clone(),
                    self.gpu_links.descriptor_set.clone(),
                    self.gpu_search_context.descriptor_set.clone(),
                    self.gpu_builder_context.descriptor_set.clone(),
                ],
            );
            self.gpu_context.dispatch(update_entry_points.len(), 1, 1);
        }

        if link_points.len() > 0 {
            self.gpu_context.bind_pipeline(
                self.link_pipeline.clone(),
                &[
                    self.gpu_vector_storage.descriptor_set.clone(),
                    self.gpu_links.descriptor_set.clone(),
                    self.gpu_search_context.descriptor_set.clone(),
                    self.gpu_builder_context.descriptor_set.clone(),
                ],
            );
            self.gpu_context.dispatch(link_points.len(), 1, 1);
        }

        self.gpu_context.run();
    }

    fn num_vectors(&self) -> usize {
        self.gpu_vector_storage.count
    }

    fn get_point_level(&self, point_id: PointOffsetType) -> usize {
        self.point_levels[point_id as usize]
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }
}
