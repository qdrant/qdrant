use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common::types::PointOffsetType;

use super::gpu_links::GpuLinks;
use super::gpu_vector_storage::GpuVectorStorage;
use super::gpu_visited_flags::GpuVisitedFlags;
use super::shader_builder::ShaderBuilderParameters;
use super::GPU_TIMEOUT;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;

/// If EF is less than this value, we use linear search instead of binary heap.
const MIN_POINTS_FOR_BINARY_HEAP: usize = 512;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct GpuRequest {
    pub id: PointOffsetType,
    pub entry: PointOffsetType,
}

/// Structure to perform insert and update entries operations on GPU.
/// It handles all GPU resources and shaders instead of gpu vector storage,
/// which may be shared between multiple hnsw constructions.
pub struct GpuInsertContext<'a> {
    context: gpu::Context,
    groups_count: usize,

    gpu_vector_storage: &'a GpuVectorStorage,
    gpu_links: GpuLinks,
    gpu_visited_flags: GpuVisitedFlags,
    insert_resources: GpuInsertResources,

    greedy_pipeline: Arc<gpu::Pipeline>,
    insert_pipeline: Arc<gpu::Pipeline>,

    updates_timer: std::time::Duration,
    updates_count: usize,
    patches_timer: std::time::Duration,
    patches_count: usize,
}

struct GpuInsertResources {
    requests_buffer: Arc<gpu::Buffer>,
    requests_staging_buffer: Arc<gpu::Buffer>,

    responses_buffer: Arc<gpu::Buffer>,
    responses_staging_buffer: Arc<gpu::Buffer>,
    insert_atomics_buffer: Arc<gpu::Buffer>,

    greedy_descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    greedy_descriptor_set: Arc<gpu::DescriptorSet>,

    insert_descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    insert_descriptor_set: Arc<gpu::DescriptorSet>,

    exact: bool,
    ef: usize,
}

impl ShaderBuilderParameters for GpuInsertResources {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "shared_buffer.comp".to_string(),
                include_str!("shaders/shared_buffer.comp").to_string(),
            ),
            (
                "bheap.comp".to_string(),
                include_str!("shaders/bheap.comp").to_string(),
            ),
            (
                "search_context.comp".to_string(),
                include_str!("shaders/search_context.comp").to_string(),
            ),
        ])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        defines.insert("EF".to_owned(), Some(self.ef.to_string()));
        if self.exact {
            defines.insert("EXACT".to_owned(), None);
        }
        if self.ef < MIN_POINTS_FOR_BINARY_HEAP {
            defines.insert("BHEAP_LINEAR".to_owned(), None);
        }
        defines
    }
}

impl GpuInsertResources {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gpu_vector_storage: &GpuVectorStorage,
        groups_count: usize,
        points_remap: &[PointOffsetType],
        ef: usize,
        exact: bool,
    ) -> OperationResult<Self> {
        let device = gpu_vector_storage.device();

        let requests_buffer = gpu::Buffer::new(
            device.clone(),
            "Search requests buffer",
            gpu::BufferType::Storage,
            groups_count * std::mem::size_of::<GpuRequest>(),
        )?;
        let requests_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search context upload staging buffer",
            gpu::BufferType::CpuToGpu,
            requests_buffer.size(),
        )?;

        let responses_buffer = gpu::Buffer::new(
            device.clone(),
            "Search responses buffer",
            gpu::BufferType::Storage,
            groups_count * std::mem::size_of::<PointOffsetType>(),
        )?;
        let responses_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search context download staging buffer",
            gpu::BufferType::GpuToCpu,
            responses_buffer.size(),
        )?;

        let insert_atomics_buffer = gpu::Buffer::new(
            device.clone(),
            "Insert atomics buffer",
            gpu::BufferType::Storage,
            std::mem::size_of_val(points_remap),
        )?;

        let greedy_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone())?;

        let greedy_descriptor_set =
            gpu::DescriptorSet::builder(greedy_descriptor_set_layout.clone())
                .add_storage_buffer(0, requests_buffer.clone())
                .add_storage_buffer(1, responses_buffer.clone())
                .build()?;

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

        Ok(Self {
            requests_buffer,
            requests_staging_buffer,
            responses_buffer,
            responses_staging_buffer,
            insert_atomics_buffer,
            greedy_descriptor_set_layout,
            greedy_descriptor_set,
            insert_descriptor_set_layout,
            insert_descriptor_set,
            exact,
            ef,
        })
    }
}

impl<'a> GpuInsertContext<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gpu_vector_storage: &'a GpuVectorStorage,
        groups_count: usize,
        points_remap: &[PointOffsetType],
        m: usize,
        m0: usize,
        ef: usize,
        exact: bool,
        visited_flags_factor_range: std::ops::Range<usize>,
    ) -> OperationResult<Self> {
        let device = gpu_vector_storage.device();
        let points_count = gpu_vector_storage.num_vectors();
        let insert_resources =
            GpuInsertResources::new(gpu_vector_storage, groups_count, points_remap, ef, exact)?;

        let gpu_links = GpuLinks::new(device.clone(), m, m0, points_count)?;

        let gpu_visited_flags = GpuVisitedFlags::new(
            device.clone(),
            groups_count,
            points_remap,
            visited_flags_factor_range,
        )?;

        let greedy_search_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/run_greedy_search.comp"))
            .with_parameters(gpu_vector_storage)
            .with_parameters(&gpu_links)
            .with_parameters(&gpu_visited_flags)
            .with_parameters(&insert_resources)
            .build("run_greedy_search.comp")?;

        let insert_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/run_insert_vector.comp"))
            .with_parameters(gpu_vector_storage)
            .with_parameters(&gpu_links)
            .with_parameters(&gpu_visited_flags)
            .with_parameters(&insert_resources)
            .build("run_insert_vector.comp")?;

        let greedy_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, insert_resources.greedy_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout())
            .add_descriptor_set_layout(3, gpu_visited_flags.descriptor_set_layout())
            .add_shader(greedy_search_shader.clone())
            .build(device.clone())?;

        let insert_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, insert_resources.insert_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout())
            .add_descriptor_set_layout(3, gpu_visited_flags.descriptor_set_layout())
            .add_shader(insert_shader.clone())
            .build(device.clone())?;

        let mut context = gpu::Context::new(device)?;
        context.clear_buffer(insert_resources.insert_atomics_buffer.clone())?;
        context.run()?;
        context.wait_finish(GPU_TIMEOUT)?;

        Ok(Self {
            insert_resources,
            gpu_vector_storage,
            gpu_links,
            gpu_visited_flags,
            context,
            groups_count,
            greedy_pipeline,
            insert_pipeline,
            updates_timer: Default::default(),
            updates_count: 0,
            patches_timer: Default::default(),
            patches_count: 0,
        })
    }

    pub fn download_responses(&mut self, count: usize) -> OperationResult<Vec<PointOffsetType>> {
        self.context.copy_gpu_buffer(
            self.insert_resources.responses_buffer.clone(),
            self.insert_resources.responses_staging_buffer.clone(),
            0,
            0,
            count * std::mem::size_of::<PointOffsetType>(),
        )?;
        self.context.run()?;
        self.context.wait_finish(GPU_TIMEOUT)?;

        let mut gpu_responses = vec![PointOffsetType::default(); count];
        self.insert_resources
            .responses_staging_buffer
            .download_slice(&mut gpu_responses, 0)?;

        Ok(gpu_responses)
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

        // upload requests
        self.insert_resources
            .requests_staging_buffer
            .upload_slice(requests, 0)?;
        self.context.copy_gpu_buffer(
            self.insert_resources.requests_staging_buffer.clone(),
            self.insert_resources.requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        )?;

        // download previous results
        if prev_results_count > 0 {
            self.context.copy_gpu_buffer(
                self.insert_resources.responses_buffer.clone(),
                self.insert_resources.responses_staging_buffer.clone(),
                0,
                0,
                prev_results_count * std::mem::size_of::<PointOffsetType>(),
            )?;
        }
        self.context.run()?;
        self.context.wait_finish(GPU_TIMEOUT)?;

        self.context.bind_pipeline(
            self.greedy_pipeline.clone(),
            &[
                self.insert_resources.greedy_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set(),
                self.gpu_links.descriptor_set(),
                self.gpu_visited_flags.descriptor_set(),
            ],
        )?;
        self.context.dispatch(requests.len(), 1, 1)?;
        self.context.run()?;
        self.context.wait_finish(GPU_TIMEOUT)?;

        self.updates_timer += timer.elapsed();
        self.updates_count += 1;

        if prev_results_count > 0 {
            let mut gpu_responses = vec![PointOffsetType::default(); prev_results_count];
            self.insert_resources
                .responses_staging_buffer
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

        self.gpu_visited_flags.clear(&mut self.context)?;

        // upload requests
        self.insert_resources
            .requests_staging_buffer
            .upload_slice(requests, 0)?;
        self.context.copy_gpu_buffer(
            self.insert_resources.requests_staging_buffer.clone(),
            self.insert_resources.requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        )?;

        // download previous results
        if prev_results_count > 0 {
            self.context.copy_gpu_buffer(
                self.insert_resources.responses_buffer.clone(),
                self.insert_resources.responses_staging_buffer.clone(),
                0,
                0,
                prev_results_count * std::mem::size_of::<PointOffsetType>(),
            )?;
        }
        self.context.run()?;
        self.context.wait_finish(GPU_TIMEOUT)?;

        self.context.bind_pipeline(
            self.insert_pipeline.clone(),
            &[
                self.insert_resources.insert_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set(),
                self.gpu_links.descriptor_set(),
                self.gpu_visited_flags.descriptor_set(),
            ],
        )?;
        self.context.dispatch(requests.len(), 1, 1)?;
        self.context.run()?;
        self.context.wait_finish(GPU_TIMEOUT)?;

        self.patches_timer += timer.elapsed();
        self.patches_count += 1;

        if prev_results_count > 0 {
            let mut gpu_responses = vec![PointOffsetType::default(); prev_results_count];
            self.insert_resources
                .responses_staging_buffer
                .download_slice(&mut gpu_responses, 0)?;
            Ok(gpu_responses)
        } else {
            Ok(vec![])
        }
    }

    pub fn upload_links(
        &mut self,
        level: usize,
        graph_layers_builder: &GraphLayersBuilder,
        stopped: &AtomicBool,
    ) -> OperationResult<()> {
        self.gpu_links
            .upload_links(level, graph_layers_builder, &mut self.context, stopped)
    }

    pub fn download_links(
        &mut self,
        level: usize,
        graph_layers_builder: &GraphLayersBuilder,
        stopped: &AtomicBool,
    ) -> OperationResult<()> {
        self.gpu_links
            .download_links(level, graph_layers_builder, &mut self.context, stopped)
    }

    pub fn clear(&mut self, new_m: usize) -> OperationResult<()> {
        self.gpu_links.update_params(&mut self.context, new_m)?;
        self.gpu_links.clear(&mut self.context)?;
        Ok(())
    }

    pub fn log_measurements(&self) {
        log::debug!(
            "Gpu graph patches time: {:?}, count {:?}, avg {:?}",
            &self.patches_timer,
            self.patches_count,
            self.patches_timer
                .checked_div(self.patches_count as u32)
                .unwrap_or_default(),
        );
        log::debug!(
            "Gpu graph update entries time: {:?}, count {:?}, avg {:?}",
            &self.updates_timer,
            self.updates_count,
            self.updates_timer
                .checked_div(self.updates_count as u32)
                .unwrap_or_default(),
        );
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
    use crate::vector_storage::VectorStorage;

    #[repr(C)]
    struct TestSearchRequest {
        id: PointOffsetType,
        entry: PointOffsetType,
    }

    struct TestData {
        groups_count: usize,
        m: usize,
        ef: usize,
        gpu_vector_storage: GpuVectorStorage,
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

        let gpu_vector_storage =
            GpuVectorStorage::new(device.clone(), &storage, None, false, &false.into()).unwrap();

        TestData {
            groups_count,
            m,
            ef,
            gpu_vector_storage,
            vector_holder,
            graph_layers_builder,
        }
    }

    fn create_insert_context(test_data: &TestData) -> GpuInsertContext<'_> {
        let total_num_vectors = test_data.gpu_vector_storage.num_vectors() + test_data.groups_count;
        let point_ids = (0..total_num_vectors as PointOffsetType).collect_vec();

        let mut gpu_insert_context = GpuInsertContext::new(
            &test_data.gpu_vector_storage,
            test_data.groups_count,
            &point_ids,
            test_data.m,
            test_data.m,
            test_data.ef,
            true,
            1..32,
        )
        .unwrap();

        gpu_insert_context
            .upload_links(0, &test_data.graph_layers_builder, &false.into())
            .unwrap();
        gpu_insert_context
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

        let test = create_test_data(num_vectors, groups_count, dim, m, ef);
        let device = test.gpu_vector_storage.device();
        let mut gpu_insert_context = create_insert_context(&test);

        let search_responses_buffer = gpu::Buffer::new(
            device.clone(),
            "Search responses buffer",
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        )
        .unwrap();

        let download_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search context download staging buffer",
            gpu::BufferType::GpuToCpu,
            search_responses_buffer.size(),
        )
        .unwrap();

        let search_shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_hnsw_search.comp"))
            .with_parameters(gpu_insert_context.gpu_vector_storage)
            .with_parameters(&gpu_insert_context.gpu_links)
            .with_parameters(&gpu_insert_context.gpu_visited_flags)
            .with_parameters(&gpu_insert_context.insert_resources)
            .build("tests/test_hnsw_search.comp")
            .unwrap();

        let search_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone())
            .unwrap();

        let search_descriptor_set =
            gpu::DescriptorSet::builder(search_descriptor_set_layout.clone())
                .add_storage_buffer(
                    0,
                    gpu_insert_context.insert_resources.requests_buffer.clone(),
                )
                .add_storage_buffer(1, search_responses_buffer.clone())
                .build()
                .unwrap();

        let search_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, search_descriptor_set_layout.clone())
            .add_descriptor_set_layout(
                1,
                gpu_insert_context
                    .gpu_vector_storage
                    .descriptor_set_layout(),
            )
            .add_descriptor_set_layout(2, gpu_insert_context.gpu_links.descriptor_set_layout())
            .add_descriptor_set_layout(
                3,
                gpu_insert_context.gpu_visited_flags.descriptor_set_layout(),
            )
            .add_shader(search_shader.clone())
            .build(device.clone())
            .unwrap();

        // create request data
        let mut search_requests = vec![];
        for i in 0..groups_count {
            search_requests.push(GpuRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        let mut search = |requests: &[GpuRequest]| {
            gpu_insert_context
                .gpu_visited_flags
                .clear(&mut gpu_insert_context.context)
                .unwrap();
            gpu_insert_context
                .insert_resources
                .requests_staging_buffer
                .upload_slice(requests, 0)
                .unwrap();
            gpu_insert_context
                .context
                .copy_gpu_buffer(
                    gpu_insert_context
                        .insert_resources
                        .requests_staging_buffer
                        .clone(),
                    gpu_insert_context.insert_resources.requests_buffer.clone(),
                    0,
                    0,
                    std::mem::size_of_val(requests),
                )
                .unwrap();
            gpu_insert_context.context.run().unwrap();
            gpu_insert_context.context.wait_finish(GPU_TIMEOUT).unwrap();

            gpu_insert_context
                .context
                .bind_pipeline(
                    search_pipeline.clone(),
                    &[
                        search_descriptor_set.clone(),
                        gpu_insert_context.gpu_vector_storage.descriptor_set(),
                        gpu_insert_context.gpu_links.descriptor_set(),
                        gpu_insert_context.gpu_visited_flags.descriptor_set(),
                    ],
                )
                .unwrap();
            gpu_insert_context
                .context
                .dispatch(requests.len(), 1, 1)
                .unwrap();
            gpu_insert_context.context.run().unwrap();
            gpu_insert_context.context.wait_finish(GPU_TIMEOUT).unwrap();

            // Download response
            gpu_insert_context
                .context
                .copy_gpu_buffer(
                    search_responses_buffer.clone(),
                    download_staging_buffer.clone(),
                    0,
                    0,
                    requests.len() * ef * std::mem::size_of::<ScoredPointOffset>(),
                )
                .unwrap();
            gpu_insert_context.context.run().unwrap();
            gpu_insert_context.context.wait_finish(GPU_TIMEOUT).unwrap();

            let mut gpu_responses = vec![ScoredPointOffset::default(); requests.len() * ef];
            download_staging_buffer
                .download_slice(&mut gpu_responses, 0)
                .unwrap();
            gpu_responses
                .chunks(ef)
                .map(|r| {
                    r.iter()
                        .take_while(|s| s.idx != PointOffsetType::MAX)
                        .cloned()
                        .collect_vec()
                })
                .collect_vec()
        };

        let gpu_responses_1 = search(&search_requests);

        // restart search to check reset
        let gpu_responses_2 = search(&search_requests);

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

        let test = create_test_data(num_vectors, groups_count, dim, m, ef);
        let mut gpu_insert_context = create_insert_context(&test);

        // create request data
        let mut search_requests = vec![];
        for i in 0..groups_count {
            search_requests.push(GpuRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        gpu_insert_context
            .greedy_search(&search_requests, 0)
            .unwrap();
        let gpu_responses = gpu_insert_context.download_responses(groups_count).unwrap();

        // Check response
        for (i, &gpu_search_result) in gpu_responses.iter().enumerate() {
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
            assert_eq!(search_result.idx, gpu_search_result);
            raw_scorer.take_hardware_counter().discard_results();
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

        let test = create_test_data(num_vectors, groups_count, dim, m, ef);
        let device = test.gpu_vector_storage.device();
        let mut gpu_insert_context = create_insert_context(&test);

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
            device.clone(),
            "Search requests buffer",
            gpu::BufferType::Storage,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        )
        .unwrap();
        let upload_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search context upload staging buffer",
            gpu::BufferType::CpuToGpu,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        )
        .unwrap();
        upload_staging_buffer
            .upload_slice(&search_requests, 0)
            .unwrap();
        gpu_insert_context
            .context
            .copy_gpu_buffer(
                upload_staging_buffer.clone(),
                search_requests_buffer.clone(),
                0,
                0,
                search_requests_buffer.size(),
            )
            .unwrap();
        gpu_insert_context.context.run().unwrap();
        gpu_insert_context.context.wait_finish(GPU_TIMEOUT).unwrap();

        // create response and response staging buffers
        let responses_buffer = gpu::Buffer::new(
            device.clone(),
            "Search responses buffer",
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        )
        .unwrap();
        let responses_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Search responses staging buffer",
            gpu::BufferType::GpuToCpu,
            responses_buffer.size(),
        )
        .unwrap();

        // Create test pipeline
        let shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_heuristic.comp"))
            .with_parameters(gpu_insert_context.gpu_vector_storage)
            .with_parameters(&gpu_insert_context.gpu_links)
            .with_parameters(&gpu_insert_context.gpu_visited_flags)
            .with_parameters(&gpu_insert_context.insert_resources)
            .build("tests/test_heuristic.comp")
            .unwrap();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone())
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
                gpu_insert_context
                    .gpu_vector_storage
                    .descriptor_set_layout(),
            )
            .add_descriptor_set_layout(2, gpu_insert_context.gpu_links.descriptor_set_layout())
            .add_descriptor_set_layout(
                3,
                gpu_insert_context.gpu_visited_flags.descriptor_set_layout(),
            )
            .add_shader(shader.clone())
            .build(device.clone())
            .unwrap();

        gpu_insert_context
            .context
            .bind_pipeline(
                pipeline.clone(),
                &[
                    descriptor_set.clone(),
                    gpu_insert_context.gpu_vector_storage.descriptor_set(),
                    gpu_insert_context.gpu_links.descriptor_set(),
                    gpu_insert_context.gpu_visited_flags.descriptor_set(),
                ],
            )
            .unwrap();
        gpu_insert_context
            .context
            .dispatch(groups_count, 1, 1)
            .unwrap();
        gpu_insert_context.context.run().unwrap();
        gpu_insert_context.context.wait_finish(GPU_TIMEOUT).unwrap();

        // Download response
        gpu_insert_context
            .context
            .copy_gpu_buffer(
                responses_buffer.clone(),
                responses_staging_buffer.clone(),
                0,
                0,
                responses_buffer.size(),
            )
            .unwrap();
        gpu_insert_context.context.run().unwrap();
        gpu_insert_context.context.wait_finish(GPU_TIMEOUT).unwrap();

        let mut gpu_responses = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer
            .download_slice(&mut gpu_responses, 0)
            .unwrap();
        let gpu_responses = gpu_responses
            .chunks_exact(ef)
            .map(|r| r.to_owned())
            .collect_vec();

        // Check response
        for (i, gpu_group_result) in gpu_responses.iter().enumerate() {
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

            for (&cpu, gpu) in heuristic.iter().zip(gpu_group_result.iter()) {
                assert_eq!(cpu, gpu.idx);
            }
            raw_scorer.take_hardware_counter().discard_results();
        }
    }
}
