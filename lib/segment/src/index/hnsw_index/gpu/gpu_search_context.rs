use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::types::{PointOffsetType, ScoredPointOffset};
use itertools::Itertools;

use super::gpu_candidates_heap::GpuCandidatesHeap;
use super::gpu_links::GpuLinks;
use super::gpu_nearest_heap::GpuNearestHeap;
use super::gpu_vector_storage::GpuVectorStorage;
use super::gpu_visited_flags::GpuVisitedFlags;
use super::shader_builder::ShaderBuilderParameters;
use super::GPU_TIMEOUT;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct GpuRequest {
    pub id: PointOffsetType,
    pub entry: PointOffsetType,
}

#[derive(Clone, Debug)]
pub struct GpuGraphLinksPatch {
    pub id: PointOffsetType,
    pub links: Vec<PointOffsetType>,
}

pub struct GpuSearchContext {
    pub device: Arc<gpu::Device>,
    pub context: gpu::Context,
    pub groups_count: usize,
    pub gpu_vector_storage: GpuVectorStorage,
    pub gpu_links: GpuLinks,
    pub gpu_nearest_heap: GpuNearestHeap,
    pub gpu_candidates_heap: GpuCandidatesHeap,
    pub gpu_visited_flags: GpuVisitedFlags,
    pub is_dirty_links: bool,

    pub upload_staging_buffer: Arc<gpu::Buffer>,
    pub download_staging_buffer: Arc<gpu::Buffer>,

    pub requests_buffer: Arc<gpu::Buffer>,
    pub responses_buffer: Arc<gpu::Buffer>,

    pub greedy_descriptor_set: Arc<gpu::DescriptorSet>,
    pub greedy_pipeline: Arc<gpu::Pipeline>,

    pub search_responses_buffer: Arc<gpu::Buffer>,
    pub search_descriptor_set: Arc<gpu::DescriptorSet>,
    pub search_pipeline: Arc<gpu::Pipeline>,

    pub patches_responses_buffer: Arc<gpu::Buffer>,
    pub patches_descriptor_set: Arc<gpu::DescriptorSet>,
    pub patches_pipeline: Arc<gpu::Pipeline>,

    pub insert_atomics_buffer: Arc<gpu::Buffer>,
    pub insert_descriptor_set: Arc<gpu::DescriptorSet>,
    pub insert_pipeline: Arc<gpu::Pipeline>,

    pub updates_timer: std::time::Duration,
    pub updates_count: usize,
    pub patches_timer: std::time::Duration,
    pub patches_count: usize,
    pub exact: bool,
}

struct GpuSearchContextParams {
    exact: bool,
}

impl ShaderBuilderParameters for GpuSearchContextParams {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([(
            "search_context.comp".to_string(),
            include_str!("shaders/search_context.comp").to_string(),
        )])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        if self.exact {
            defines.insert("EXACT".to_owned(), None);
        }
        defines
    }
}

struct GpuSearchContextGroupAllocation {
    groups_count: usize,
    gpu_nearest_heap: GpuNearestHeap,
    gpu_candidates_heap: GpuCandidatesHeap,
    gpu_visited_flags: GpuVisitedFlags,
    upload_staging_buffer: Arc<gpu::Buffer>,
    download_staging_buffer: Arc<gpu::Buffer>,
    requests_buffer: Arc<gpu::Buffer>,
    responses_buffer: Arc<gpu::Buffer>,
    search_responses_buffer: Arc<gpu::Buffer>,
    patches_responses_buffer: Arc<gpu::Buffer>,
    insert_atomics_buffer: Arc<gpu::Buffer>,
}

impl GpuSearchContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        device: Arc<gpu::Device>,
        max_groups_count: usize,
        vector_storage: &VectorStorageEnum,
        quantized_storage: Option<&QuantizedVectors>,
        m: usize,
        m0: usize,
        ef: usize,
        max_patched_points: usize,
        force_half_precision: bool,
        exact: bool,
        stopped: &AtomicBool,
    ) -> OperationResult<Self> {
        let points_count = vector_storage.total_vector_count();
        let search_context_params = GpuSearchContextParams { exact };

        let gpu_vector_storage = GpuVectorStorage::new(
            device.clone(),
            vector_storage,
            quantized_storage,
            force_half_precision,
            stopped,
        )?;
        let gpu_links = GpuLinks::new(device.clone(), m, m0, points_count, max_patched_points)?;

        let allocation_timer = std::time::Instant::now();
        let GpuSearchContextGroupAllocation {
            groups_count,
            gpu_nearest_heap,
            gpu_candidates_heap,
            gpu_visited_flags,
            upload_staging_buffer,
            download_staging_buffer,
            requests_buffer,
            responses_buffer,
            search_responses_buffer,
            patches_responses_buffer,
            insert_atomics_buffer,
        } = Self::allocate_grouped_data(device.clone(), max_groups_count, points_count, ef, m0)
            .map_err(|_| OperationError::service_error("Failed to allocate gpu data"))?;
        log::debug!(
            "GPU groups count = {groups_count} (max = {max_groups_count}), allocation time: {:?}",
            allocation_timer.elapsed()
        );

        let greedy_search_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/run_greedy_search.comp"))
            .with_parameters(&gpu_vector_storage)
            .with_parameters(&gpu_nearest_heap)
            .with_parameters(&gpu_candidates_heap)
            .with_parameters(&gpu_links)
            .with_parameters(&gpu_visited_flags)
            .with_parameters(&search_context_params)
            .build()?;

        let insert_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/run_insert_vector.comp"))
            .with_parameters(&gpu_vector_storage)
            .with_parameters(&gpu_nearest_heap)
            .with_parameters(&gpu_candidates_heap)
            .with_parameters(&gpu_links)
            .with_parameters(&gpu_visited_flags)
            .with_parameters(&search_context_params)
            .build()?;

        let search_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_hnsw_search.comp"))
            .with_parameters(&gpu_vector_storage)
            .with_parameters(&gpu_nearest_heap)
            .with_parameters(&gpu_candidates_heap)
            .with_parameters(&gpu_links)
            .with_parameters(&gpu_visited_flags)
            .with_parameters(&search_context_params)
            .build()?;

        let patches_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/run_get_patch.comp"))
            .with_parameters(&gpu_vector_storage)
            .with_parameters(&gpu_nearest_heap)
            .with_parameters(&gpu_candidates_heap)
            .with_parameters(&gpu_links)
            .with_parameters(&gpu_visited_flags)
            .with_parameters(&search_context_params)
            .build()?;

        let greedy_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone())?;

        let greedy_descriptor_set =
            gpu::DescriptorSet::builder(greedy_descriptor_set_layout.clone())
                .add_storage_buffer(0, requests_buffer.clone())
                .add_storage_buffer(1, responses_buffer.clone())
                .build()?;

        let greedy_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, greedy_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_visited_flags.descriptor_set_layout.clone())
            .add_shader(greedy_search_shader.clone())
            .build(device.clone())?;

        let search_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone())?;

        let search_descriptor_set =
            gpu::DescriptorSet::builder(search_descriptor_set_layout.clone())
                .add_storage_buffer(0, requests_buffer.clone())
                .add_storage_buffer(1, search_responses_buffer.clone())
                .build()?;

        let search_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, search_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_visited_flags.descriptor_set_layout.clone())
            .add_shader(search_shader.clone())
            .build(device.clone())?;

        let patches_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone())?;

        let patches_descriptor_set =
            gpu::DescriptorSet::builder(patches_descriptor_set_layout.clone())
                .add_storage_buffer(0, requests_buffer.clone())
                .add_storage_buffer(1, patches_responses_buffer.clone())
                .add_storage_buffer(2, responses_buffer.clone())
                .build()?;

        let patches_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, patches_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_visited_flags.descriptor_set_layout.clone())
            .add_shader(patches_shader.clone())
            .build(device.clone())?;

        let insert_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone())?;

        let insert_descriptor_set =
            gpu::DescriptorSet::builder(insert_descriptor_set_layout.clone())
                .add_storage_buffer(0, requests_buffer.clone())
                .add_storage_buffer(1, responses_buffer.clone())
                .add_storage_buffer(2, insert_atomics_buffer.clone())
                .build()?;

        let insert_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, insert_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_visited_flags.descriptor_set_layout.clone())
            .add_shader(insert_shader.clone())
            .build(device.clone())?;

        let context = gpu::Context::new(device.clone())?;
        Ok(Self {
            gpu_vector_storage,
            gpu_links,
            gpu_nearest_heap,
            gpu_candidates_heap,
            gpu_visited_flags,
            device,
            context,
            groups_count,
            is_dirty_links: false,
            upload_staging_buffer,
            download_staging_buffer,
            requests_buffer,
            responses_buffer,
            greedy_descriptor_set,
            greedy_pipeline,
            search_responses_buffer,
            search_descriptor_set,
            search_pipeline,
            patches_responses_buffer,
            patches_descriptor_set,
            patches_pipeline,
            insert_atomics_buffer,
            insert_descriptor_set,
            insert_pipeline,
            updates_timer: Default::default(),
            updates_count: 0,
            patches_timer: Default::default(),
            patches_count: 0,
            exact,
        })
    }

    fn allocate_grouped_data(
        device: Arc<gpu::Device>,
        max_groups_count: usize,
        points_count: usize,
        ef: usize,
        m0: usize,
    ) -> gpu::GpuResult<GpuSearchContextGroupAllocation> {
        let mut visited_flags_factor = 1.0;
        // TODO(gpu): move 32 to config
        while visited_flags_factor < 32.0 {
            // TODO(gpu): decrease only gpu visited flags
            // TODO(gpu): restart only if OOM, for other errors return an error
            if let Ok(alloc) = Self::try_allocate_grouped_data(
                device.clone(),
                max_groups_count,
                points_count,
                visited_flags_factor,
                ef,
                m0,
            ) {
                return Ok(alloc);
            }
            visited_flags_factor *= 1.5;
        }
        panic!("Failed to allocate gpu data")
    }

    fn try_allocate_grouped_data(
        device: Arc<gpu::Device>,
        groups_count: usize,
        points_count: usize,
        visited_flags_factor: f32,
        ef: usize,
        m0: usize,
    ) -> OperationResult<GpuSearchContextGroupAllocation> {
        let requests_buffer = gpu::Buffer::new(
            device.clone(),
            "Search requests buffer",
            gpu::BufferType::Storage,
            groups_count * std::mem::size_of::<GpuRequest>(),
        )?;
        let responses_buffer = gpu::Buffer::new(
            device.clone(),
            "Search responses buffer",
            gpu::BufferType::Storage,
            groups_count * std::mem::size_of::<PointOffsetType>(),
        )?;

        let gpu_nearest_heap = GpuNearestHeap::new(device.clone(), ef, std::cmp::max(ef, m0 + 1))?;
        let gpu_candidates_heap =
            GpuCandidatesHeap::new(device.clone(), std::cmp::max(ef, m0 + 1))?;
        let gpu_visited_flags = GpuVisitedFlags::new(
            device.clone(),
            groups_count,
            (points_count as f32 / visited_flags_factor) as usize,
        )?;

        // todo(gpu): remove this buffer
        let patches_responses_buffer = gpu::Buffer::new(
            device.clone(),
            "Patch responses buffer",
            gpu::BufferType::Storage,
            groups_count * ((m0 + 1) * (m0 + 2)) * std::mem::size_of::<PointOffsetType>(),
        )?;

        let upload_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search context upload staging buffer",
            gpu::BufferType::CpuToGpu,
            requests_buffer.size(),
        )?;
        let download_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search context download staging buffer",
            gpu::BufferType::GpuToCpu,
            patches_responses_buffer.size() + responses_buffer.size(),
        )?;

        let search_responses_buffer = gpu::Buffer::new(
            device.clone(),
            "Search responses buffer",
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        )?;
        let insert_atomics_buffer = gpu::Buffer::new(
            device.clone(),
            "Insert atomics buffer",
            gpu::BufferType::Storage,
            points_count * std::mem::size_of::<PointOffsetType>(),
        )?;

        Ok(GpuSearchContextGroupAllocation {
            groups_count,
            gpu_nearest_heap,
            gpu_candidates_heap,
            gpu_visited_flags,
            upload_staging_buffer,
            download_staging_buffer,
            requests_buffer,
            responses_buffer,
            search_responses_buffer,
            patches_responses_buffer,
            insert_atomics_buffer,
        })
    }

    pub fn download_responses(&mut self, count: usize) -> OperationResult<Vec<PointOffsetType>> {
        self.context.copy_gpu_buffer(
            self.responses_buffer.clone(),
            self.download_staging_buffer.clone(),
            0,
            0,
            count * std::mem::size_of::<PointOffsetType>(),
        )?;
        self.run_context()?;
        let mut gpu_responses = vec![PointOffsetType::default(); count];
        self.download_staging_buffer
            .download_slice(&mut gpu_responses, 0)?;

        // TODO(gpu) if response if uint::MAX, we need to reallocate
        for i in 0..count {
            if gpu_responses[i] == PointOffsetType::MAX {
                return Err(OperationError::service_error("Candidates heap overflow"));
            }
        }

        Ok(gpu_responses)
    }

    pub fn search(
        &mut self,
        requests: &[GpuRequest],
    ) -> OperationResult<Vec<Vec<ScoredPointOffset>>> {
        if requests.len() > self.groups_count {
            return Err(OperationError::service_error(
                "Too many gpu greedy search requests",
            ));
        }

        if self.is_dirty() {
            self.apply_links_patch().unwrap();
        }
        self.gpu_visited_flags.clear(&mut self.context)?;

        self.upload_staging_buffer.upload_slice(requests, 0)?;
        self.context.copy_gpu_buffer(
            self.upload_staging_buffer.clone(),
            self.requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        )?;
        self.run_context()?;

        self.context.bind_pipeline(
            self.search_pipeline.clone(),
            &[
                self.search_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set(),
                self.gpu_links.descriptor_set.clone(),
                self.gpu_visited_flags.descriptor_set.clone(),
            ],
        )?;
        self.context.dispatch(requests.len(), 1, 1)?;
        self.run_context()?;

        // Download response
        self.context.copy_gpu_buffer(
            self.search_responses_buffer.clone(),
            self.download_staging_buffer.clone(),
            0,
            0,
            requests.len() * self.gpu_nearest_heap.ef * std::mem::size_of::<ScoredPointOffset>(),
        )?;
        self.run_context()?;
        let mut gpu_responses =
            vec![ScoredPointOffset::default(); requests.len() * self.gpu_nearest_heap.ef];
        self.download_staging_buffer
            .download_slice(&mut gpu_responses, 0)?;
        Ok(gpu_responses
            .chunks(self.gpu_nearest_heap.ef)
            .map(|r| {
                r.iter()
                    .take_while(|s| s.idx != PointOffsetType::MAX)
                    .cloned()
                    .collect_vec()
            })
            .collect_vec())
    }

    pub fn greedy_search(
        &mut self,
        requests: &[GpuRequest],
        prev_results_count: usize,
    ) -> OperationResult<Vec<PointOffsetType>> {
        if requests.len() > self.groups_count {
            return Err(OperationError::service_error(
                "Too many gpu greedy search requests",
            ));
        }

        let timer = std::time::Instant::now();

        if self.is_dirty() {
            self.apply_links_patch().unwrap();
        }

        // upload requests
        self.upload_staging_buffer.upload_slice(requests, 0)?;
        self.context.copy_gpu_buffer(
            self.upload_staging_buffer.clone(),
            self.requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        )?;

        // download previous results
        if prev_results_count > 0 {
            self.context.copy_gpu_buffer(
                self.responses_buffer.clone(),
                self.download_staging_buffer.clone(),
                0,
                0,
                prev_results_count * std::mem::size_of::<PointOffsetType>(),
            )?;
        }
        self.run_context()?;

        self.context.bind_pipeline(
            self.greedy_pipeline.clone(),
            &[
                self.greedy_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set(),
                self.gpu_links.descriptor_set.clone(),
                self.gpu_visited_flags.descriptor_set.clone(),
            ],
        )?;
        self.context.dispatch(requests.len(), 1, 1)?;
        self.run_context()?;

        self.updates_timer += timer.elapsed();
        self.updates_count += 1;

        if prev_results_count > 0 {
            let mut gpu_responses = vec![PointOffsetType::default(); prev_results_count];
            self.download_staging_buffer
                .download_slice(&mut gpu_responses, 0)?;
            Ok(gpu_responses)
        } else {
            Ok(vec![])
        }
    }

    pub fn run_insert_vector(
        &mut self,
        requests: &[GpuRequest],
        prev_results_count: usize,
    ) -> OperationResult<Vec<PointOffsetType>> {
        if requests.len() > self.groups_count {
            return Err(OperationError::service_error("Too many gpu patch requests"));
        }

        let timer = std::time::Instant::now();

        if self.is_dirty() {
            self.apply_links_patch().unwrap();
        }
        self.gpu_visited_flags.clear(&mut self.context)?;

        // clear atomics
        if self.gpu_visited_flags.params.generation == 1 {
            self.context
                .clear_buffer(self.insert_atomics_buffer.clone())?;
        }

        // upload requests
        self.upload_staging_buffer.upload_slice(requests, 0)?;
        self.context.copy_gpu_buffer(
            self.upload_staging_buffer.clone(),
            self.requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        )?;

        // download previous results
        if prev_results_count > 0 {
            self.context.copy_gpu_buffer(
                self.responses_buffer.clone(),
                self.download_staging_buffer.clone(),
                0,
                0,
                prev_results_count * std::mem::size_of::<PointOffsetType>(),
            )?;
        }
        self.run_context()?;

        self.context.bind_pipeline(
            self.insert_pipeline.clone(),
            &[
                self.insert_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set(),
                self.gpu_links.descriptor_set.clone(),
                self.gpu_visited_flags.descriptor_set.clone(),
            ],
        )?;
        self.context.dispatch(requests.len(), 1, 1)?;
        self.run_context()?;

        self.patches_timer += timer.elapsed();
        self.patches_count += 1;

        if prev_results_count > 0 {
            let mut gpu_responses = vec![PointOffsetType::default(); prev_results_count];
            self.download_staging_buffer
                .download_slice(&mut gpu_responses, 0)?;
            Ok(gpu_responses)
        } else {
            Ok(vec![])
        }
    }

    pub fn run_get_patch(
        &mut self,
        requests: &[GpuRequest],
    ) -> OperationResult<(Vec<Vec<GpuGraphLinksPatch>>, Vec<PointOffsetType>)> {
        if requests.len() > self.groups_count {
            return Err(OperationError::service_error("Too many gpu patch requests"));
        }

        if self.is_dirty() {
            self.apply_links_patch().unwrap();
        }
        self.gpu_visited_flags.clear(&mut self.context)?;

        self.upload_staging_buffer.upload_slice(requests, 0)?;
        self.context.copy_gpu_buffer(
            self.upload_staging_buffer.clone(),
            self.requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        )?;
        self.run_context()?;

        self.context.bind_pipeline(
            self.patches_pipeline.clone(),
            &[
                self.patches_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set(),
                self.gpu_links.descriptor_set.clone(),
                self.gpu_visited_flags.descriptor_set.clone(),
            ],
        )?;
        self.context.dispatch(requests.len(), 1, 1)?;

        let timer = std::time::Instant::now();
        self.run_context()?;
        self.patches_timer += timer.elapsed();

        // Download response
        self.context.copy_gpu_buffer(
            self.responses_buffer.clone(),
            self.download_staging_buffer.clone(),
            0,
            0,
            requests.len() * std::mem::size_of::<PointOffsetType>(),
        )?;
        self.context.copy_gpu_buffer(
            self.patches_responses_buffer.clone(),
            self.download_staging_buffer.clone(),
            0,
            self.responses_buffer.size(),
            self.patches_responses_buffer.size(),
        )?;
        self.run_context()?;
        let mut new_entries = vec![PointOffsetType::default(); requests.len()];
        self.download_staging_buffer
            .download_slice(&mut new_entries, 0)?;

        let mut patches_data = vec![
            PointOffsetType::default();
            self.patches_responses_buffer.size()
                / std::mem::size_of::<PointOffsetType>()
        ];
        self.download_staging_buffer
            .download_slice(&mut patches_data, self.responses_buffer.size())?;

        let m = self.gpu_links.m;
        let mut all_patches = vec![];
        for i in 0..requests.len() {
            let patch_size = m + 2;
            let all_patches_size = (m + 1) * patch_size;
            let mut patches_offset = i * all_patches_size;

            let mut patches = vec![];
            for _ in 0..m + 1 {
                let point_id = patches_data[patches_offset];
                if point_id == PointOffsetType::MAX {
                    break;
                }
                let links_count = patches_data[patches_offset + 1] as usize;
                let links = &patches_data[patches_offset + 2..patches_offset + 2 + links_count];
                patches.push(GpuGraphLinksPatch {
                    id: point_id,
                    links: links.to_vec(),
                });
                patches_offset += patch_size;
            }
            all_patches.push(patches);
        }

        Ok((all_patches, new_entries))
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        links: &[PointOffsetType],
    ) -> OperationResult<()> {
        self.is_dirty_links = true;
        self.gpu_links.set_links(point_id, links)
    }

    pub fn upload_links(
        &mut self,
        level: usize,
        graph_layers_builder: &GraphLayersBuilder,
    ) -> OperationResult<()> {
        self.gpu_links
            .upload_links(level, graph_layers_builder, &mut self.context)
    }

    pub fn download_links(
        &mut self,
        level: usize,
        graph_layers_builder: &GraphLayersBuilder,
    ) -> OperationResult<()> {
        self.gpu_links
            .download_links(level, graph_layers_builder, &mut self.context)
    }

    pub fn clear(&mut self, new_m: usize) -> OperationResult<()> {
        self.gpu_links.update_params(&mut self.context, new_m)?;
        self.gpu_links.clear(&mut self.context)?;
        self.run_context()?;
        Ok(())
    }

    pub fn apply_links_patch(&mut self) -> OperationResult<()> {
        self.gpu_links.apply_gpu_patches(&mut self.context)?;
        self.is_dirty_links = false;
        Ok(())
    }

    pub fn run_context(&mut self) -> OperationResult<()> {
        self.context.run()?;
        self.context.wait_finish(GPU_TIMEOUT)?;
        Ok(())
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty_links
    }
}

#[cfg(test)]
mod tests {
    use common::types::ScoredPointOffset;
    use itertools::Itertools;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
    use crate::index::hnsw_index::graph_layers::GraphLayersBase;
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::DotProductMetric;
    use crate::types::Distance;
    use crate::vector_storage::chunked_vector_storage::VectorOffsetType;
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;

    #[repr(C)]
    struct TestSearchRequest {
        id: PointOffsetType,
        entry: PointOffsetType,
    }

    struct TestData {
        gpu_search_context: GpuSearchContext,
        gpu_search_context_params: GpuSearchContextParams,
        vector_holder: TestRawScorerProducer<DotProductMetric>,
        graph_layers_builder: GraphLayersBuilder,
    }

    fn create_test_data(
        num_vectors: usize,
        groups_count: usize,
        dim: usize,
        m: usize,
        ef: usize,
    ) -> TestData {
        // Generate random vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder = TestRawScorerProducer::<DotProductMetric>::new(
            dim,
            num_vectors + groups_count,
            &mut rng,
        );

        // upload vectors to storage
        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();
        for idx in 0..(num_vectors + groups_count) {
            let v = vector_holder.get_vector(idx as PointOffsetType);
            storage
                .insert_vector(idx as PointOffsetType, v.as_vec_ref())
                .unwrap();
        }

        // Build HNSW index
        let mut graph_layers_builder = GraphLayersBuilder::new(num_vectors, m, m, ef, 1, true);
        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx, level);
        }
        for idx in 0..(num_vectors as PointOffsetType) {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx as VectorOffsetType).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone()).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_builder.link_new_point(idx, scorer);
            raw_scorer.take_hardware_counter().discard_results();
        }

        // Create GPU search context
        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance = gpu::Instance::new(Some(&debug_messenger), None, false).unwrap();
        let device = gpu::Device::new(instance.clone(), &instance.physical_devices()[0]).unwrap();

        let mut gpu_search_context = GpuSearchContext::new(
            device,
            groups_count,
            &storage,
            None,
            m,
            m,
            ef,
            num_vectors,
            false,
            true,
            &false.into(),
        )
        .unwrap();

        // Upload HNSW links to GPU
        for idx in 0..(num_vectors as PointOffsetType) {
            let mut links = vec![];
            graph_layers_builder.links_map(idx, 0, |link| links.push(link));
            gpu_search_context.set_links(idx, &links).unwrap();
        }
        gpu_search_context.apply_links_patch().unwrap();
        gpu_search_context.run_context().unwrap();

        TestData {
            gpu_search_context,
            vector_holder,
            graph_layers_builder,
            gpu_search_context_params: GpuSearchContextParams { exact: true },
        }
    }

    #[test]
    fn test_gpu_hnsw_search_on_level() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 1024;
        let groups_count = 8;
        let dim = 64;
        let m = 16;
        let ef = 32;

        let mut test = create_test_data(num_vectors, groups_count, dim, m, ef);

        // create request data
        let mut search_requests = vec![];
        for i in 0..groups_count {
            search_requests.push(GpuRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        let gpu_responses_1 = test.gpu_search_context.search(&search_requests).unwrap();

        // restart search to check reset
        let gpu_responses_2 = test.gpu_search_context.search(&search_requests).unwrap();

        // Check response
        for i in 0..groups_count {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = test.vector_holder.vectors.get(num_vectors + i).to_vec();
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(added_vector.clone())
                .unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            let entry = ScoredPointOffset {
                idx: 0,
                score: scorer.score_point(0),
            };
            let search_result = test
                .graph_layers_builder
                .search_on_level(entry, 0, ef, &mut scorer)
                .into_vec();
            for (cpu, (gpu_1, gpu_2)) in search_result
                .iter()
                .zip(gpu_responses_1[i].iter().zip(gpu_responses_2[i].iter()))
            {
                assert_eq!(cpu.idx, gpu_1.idx);
                assert_eq!(cpu.idx, gpu_2.idx);
                assert!((cpu.score - gpu_1.score).abs() < 1e-5);
                assert!((cpu.score - gpu_2.score).abs() < 1e-5);
            }
            raw_scorer.take_hardware_counter().discard_results();
        }
    }

    #[test]
    fn test_gpu_greedy_search() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 1024;
        let groups_count = 8;
        let dim = 64;
        let m = 16;
        let ef = 32;

        let mut test = create_test_data(num_vectors, groups_count, dim, m, ef);

        // create request data
        let mut search_requests = vec![];
        for i in 0..groups_count {
            search_requests.push(GpuRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        test.gpu_search_context
            .greedy_search(&search_requests, 0)
            .unwrap();
        let gpu_responses = test
            .gpu_search_context
            .download_responses(groups_count)
            .unwrap();

        // Check response
        for i in 0..groups_count {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = test.vector_holder.vectors.get(num_vectors + i).to_vec();
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(added_vector.clone())
                .unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            let search_result = test
                .graph_layers_builder
                .search_entry_on_level(0, 0, &mut scorer);
            assert_eq!(search_result.idx, gpu_responses[i]);
            raw_scorer.take_hardware_counter().discard_results();
        }
    }

    #[test]
    fn test_gpu_hnsw_patch() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 1024;
        let groups_count = 8;
        let dim = 64;
        let m = 16;
        let ef = 32;

        let mut test = create_test_data(num_vectors, groups_count, dim, m, ef);

        // create request data
        let mut requests = vec![];
        for i in 0..groups_count {
            requests.push(GpuRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        let (patches, new_entries) = test.gpu_search_context.run_get_patch(&requests).unwrap();

        for (i, gpu_patches) in patches.iter().enumerate() {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = test.vector_holder.vectors.get(num_vectors + i).to_vec();
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(added_vector.clone())
                .unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let (cpu_patches, cpu_new_entry) =
                test.graph_layers_builder
                    .get_patch(requests[i], 0, &mut scorer);
            raw_scorer.take_hardware_counter().discard_results();

            assert_eq!(new_entries[i], *cpu_new_entry.first().unwrap());
            assert_eq!(gpu_patches.len(), cpu_patches.len());
            for (gpu_patch, cpu_patch) in gpu_patches.iter().zip(cpu_patches.iter()) {
                assert_eq!(gpu_patch.id, cpu_patch.id);
                assert_eq!(gpu_patch.links, cpu_patch.links);
            }
        }
    }

    #[test]
    fn test_gpu_heuristic() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let num_vectors = 1024;
        let groups_count = 8;
        let dim = 64;
        let m = 16;
        let ef = 32;

        let mut test = create_test_data(num_vectors, groups_count, dim, m, ef);

        // create request data
        let mut search_requests = vec![];
        for i in 0..groups_count {
            search_requests.push(TestSearchRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        // upload search requests to GPU
        let search_requests_buffer = gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            "Search requests buffer",
            gpu::BufferType::Storage,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        )
        .unwrap();
        let upload_staging_buffer = gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            "Search context upload staging buffer",
            gpu::BufferType::CpuToGpu,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        )
        .unwrap();
        upload_staging_buffer
            .upload_slice(&search_requests, 0)
            .unwrap();
        test.gpu_search_context
            .context
            .copy_gpu_buffer(
                upload_staging_buffer.clone(),
                search_requests_buffer.clone(),
                0,
                0,
                search_requests_buffer.size(),
            )
            .unwrap();
        test.gpu_search_context.run_context().unwrap();

        // create response and response staging buffers
        let responses_buffer = gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            "Search responses buffer",
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        )
        .unwrap();
        let responses_staging_buffer = gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            "Search responses staging buffer",
            gpu::BufferType::GpuToCpu,
            responses_buffer.size(),
        )
        .unwrap();

        // Create test pipeline
        let shader = ShaderBuilder::new(test.gpu_search_context.device.clone())
            .with_shader_code(include_str!("shaders/tests/test_heuristic.comp"))
            .with_parameters(&test.gpu_search_context.gpu_vector_storage)
            .with_parameters(&test.gpu_search_context.gpu_nearest_heap)
            .with_parameters(&test.gpu_search_context.gpu_candidates_heap)
            .with_parameters(&test.gpu_search_context.gpu_links)
            .with_parameters(&test.gpu_search_context.gpu_visited_flags)
            .with_parameters(&test.gpu_search_context_params)
            .build()
            .unwrap();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(test.gpu_search_context.device.clone())
            .unwrap();

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, search_requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build()
            .unwrap();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(
                1,
                test.gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set_layout(),
            )
            .add_descriptor_set_layout(
                2,
                test.gpu_search_context
                    .gpu_links
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                3,
                test.gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set_layout
                    .clone(),
            )
            .add_shader(shader.clone())
            .build(test.gpu_search_context.device.clone())
            .unwrap();

        test.gpu_search_context
            .context
            .bind_pipeline(
                pipeline.clone(),
                &[
                    descriptor_set.clone(),
                    test.gpu_search_context.gpu_vector_storage.descriptor_set(),
                    test.gpu_search_context.gpu_links.descriptor_set.clone(),
                    test.gpu_search_context
                        .gpu_visited_flags
                        .descriptor_set
                        .clone(),
                ],
            )
            .unwrap();
        test.gpu_search_context
            .context
            .dispatch(groups_count, 1, 1)
            .unwrap();
        test.gpu_search_context.run_context().unwrap();

        // Download response
        test.gpu_search_context
            .context
            .copy_gpu_buffer(
                responses_buffer.clone(),
                responses_staging_buffer.clone(),
                0,
                0,
                responses_buffer.size(),
            )
            .unwrap();
        test.gpu_search_context.run_context().unwrap();
        let mut gpu_responses = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer
            .download_slice(&mut gpu_responses, 0)
            .unwrap();
        let gpu_responses = gpu_responses
            .chunks_exact(ef)
            .map(|r| r.to_owned())
            .collect_vec();

        // Check response
        for i in 0..groups_count {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = test.vector_holder.vectors.get(num_vectors + i).to_vec();
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(added_vector.clone())
                .unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            let entry = ScoredPointOffset {
                idx: 0,
                score: scorer.score_point(0),
            };
            let search_result =
                test.graph_layers_builder
                    .search_on_level(entry, 0, ef, &mut scorer);

            let scorer_fn = |a, b| scorer.score_internal(a, b);

            let heuristic =
                GraphLayersBuilder::select_candidates_with_heuristic(search_result, m, scorer_fn);

            for (&cpu, gpu) in heuristic.iter().zip(gpu_responses[i].iter()) {
                assert_eq!(cpu, gpu.idx);
            }
            raw_scorer.take_hardware_counter().discard_results();
        }
    }
}
