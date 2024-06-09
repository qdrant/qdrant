use std::sync::Arc;

use common::types::{PointOffsetType, ScoredPointOffset};

use super::gpu_candidates_heap::GpuCandidatesHeap;
use super::gpu_links::GpuLinks;
use super::gpu_nearest_heap::GpuNearestHeap;
use super::gpu_vector_storage::GpuVectorStorage;
use super::gpu_visited_flags::GpuVisitedFlags;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

#[repr(C)]
pub struct GreedySearchRequest {
    id: PointOffsetType,
    entry: PointOffsetType,
}

pub struct GraphLinksPatch {
    id: PointOffsetType,
    links: Vec<PointOffsetType>,
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

    pub greedy_upload_staging_buffer: Arc<gpu::Buffer>,
    pub greedy_download_staging_buffer: Arc<gpu::Buffer>,
    pub greedy_requests_buffer: Arc<gpu::Buffer>,
    pub greedy_responses_buffer: Arc<gpu::Buffer>,
    pub greedy_descriptor_set: Arc<gpu::DescriptorSet>,
    pub greedy_pipeline: Arc<gpu::Pipeline>,
    //pub greedy_upload_staging_buffer: Arc<gpu::Buffer>,
    //pub greedy_download_staging_buffer: Arc<gpu::Buffer>,
    //pub greedy_requests_buffer: Arc<gpu::Buffer>,
    //pub greedy_responses_buffer: Arc<gpu::Buffer>,
    //pub greedy_descriptor_set: Arc<gpu::DescriptorSet>,
    //pub greedy_pipeline: Arc<gpu::Pipeline>,
}

impl GpuSearchContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        debug_messenger: Option<&dyn gpu::DebugMessenger>,
        groups_count: usize,
        vector_storage: &VectorStorageEnum,
        dim: usize,
        m: usize,
        m0: usize,
        ef: usize,
        max_patched_points: usize,
    ) -> OperationResult<Self> {
        let instance = Arc::new(gpu::Instance::new("qdrant", debug_messenger, false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());
        let context = gpu::Context::new(device.clone());
        let points_count = vector_storage.total_vector_count();
        let candidates_capacity = points_count;

        let gpu_vector_storage = GpuVectorStorage::new(device.clone(), vector_storage, dim)?;
        let gpu_links = GpuLinks::new(device.clone(), m, m0, points_count, max_patched_points)?;
        let gpu_nearest_heap = GpuNearestHeap::new(device.clone(), groups_count, ef)?;
        let gpu_candidates_heap =
            GpuCandidatesHeap::new(device.clone(), groups_count, candidates_capacity)?;
        let gpu_visited_flags = GpuVisitedFlags::new(device.clone(), groups_count, points_count)?;

        let greedy_requests_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            groups_count * std::mem::size_of::<GreedySearchRequest>(),
        ));
        let greedy_upload_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            greedy_requests_buffer.size,
        ));
        let greedy_responses_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            groups_count * std::mem::size_of::<ScoredPointOffset>(),
        ));
        let greedy_download_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            greedy_responses_buffer.size,
        ));

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/compiled/run_greedy_search.spv"),
        ));
        let greedy_descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let greedy_descriptor_set =
            gpu::DescriptorSet::builder(greedy_descriptor_set_layout.clone())
                .add_storage_buffer(0, greedy_requests_buffer.clone())
                .add_storage_buffer(1, greedy_responses_buffer.clone())
                .build();

        let greedy_pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, greedy_descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, gpu_nearest_heap.descriptor_set_layout.clone())
            .add_descriptor_set_layout(4, gpu_candidates_heap.descriptor_set_layout.clone())
            .add_descriptor_set_layout(5, gpu_visited_flags.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

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
            greedy_upload_staging_buffer,
            greedy_download_staging_buffer,
            greedy_requests_buffer,
            greedy_responses_buffer,
            greedy_descriptor_set,
            greedy_pipeline,
        })
    }

    pub fn greedy_search(
        &mut self,
        requests: &[GreedySearchRequest],
    ) -> OperationResult<Vec<ScoredPointOffset>> {
        if requests.len() > self.groups_count {
            return Err(OperationError::service_error(
                "Too many gpu greedy search requests",
            ));
        }

        if self.is_dirty() {
            self.apply_links_patch().unwrap();
        }

        self.greedy_upload_staging_buffer.upload_slice(requests, 0);
        self.context.copy_gpu_buffer(
            self.greedy_upload_staging_buffer.clone(),
            self.greedy_requests_buffer.clone(),
            0,
            0,
            std::mem::size_of_val(requests),
        );
        self.run_context();

        self.context.bind_pipeline(
            self.greedy_pipeline.clone(),
            &[
                self.greedy_descriptor_set.clone(),
                self.gpu_vector_storage.descriptor_set.clone(),
                self.gpu_links.descriptor_set.clone(),
                self.gpu_nearest_heap.descriptor_set.clone(),
                self.gpu_candidates_heap.descriptor_set.clone(),
                self.gpu_visited_flags.descriptor_set.clone(),
            ],
        );
        self.context.dispatch(requests.len(), 1, 1);
        self.run_context();

        // Download response
        self.context.copy_gpu_buffer(
            self.greedy_responses_buffer.clone(),
            self.greedy_download_staging_buffer.clone(),
            0,
            0,
            requests.len() * std::mem::size_of::<ScoredPointOffset>(),
        );
        self.run_context();
        let mut gpu_responses = vec![ScoredPointOffset::default(); requests.len()];
        self.greedy_download_staging_buffer
            .download_slice(&mut gpu_responses, 0);
        Ok(gpu_responses)
    }

    pub fn run_insert_vector(
        &mut self,
        requests: &[GreedySearchRequest],
    ) -> Vec<Vec<GraphLinksPatch>> {
        todo!()
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        self.gpu_links.get_links(point_id)
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        links: &[PointOffsetType],
    ) -> OperationResult<()> {
        self.is_dirty_links = true;
        self.gpu_links.set_links(point_id, links)
    }

    pub fn update_layer_params(&mut self, m: usize) {
        self.gpu_links.update_params(&mut self.context, m);
    }

    fn apply_links_patch(&mut self) -> OperationResult<()> {
        self.gpu_links.apply_gpu_patches(&mut self.context);
        self.is_dirty_links = false;
        Ok(())
    }

    fn reset_context(&mut self) {
        self.gpu_visited_flags.clear(&mut self.context);
    }

    fn run_context(&mut self) {
        self.context.run();
        self.context.wait_finish();
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
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;

    #[repr(C)]
    struct TestSearchRequest {
        id: PointOffsetType,
        entry: PointOffsetType,
    }

    struct TestData {
        gpu_search_context: GpuSearchContext,
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
        let storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();
        {
            let mut borrowed_storage = storage.borrow_mut();
            for idx in 0..(num_vectors + groups_count) {
                let v = vector_holder.get_vector(idx as PointOffsetType);
                borrowed_storage
                    .insert_vector(idx as PointOffsetType, v.as_vec_ref())
                    .unwrap();
            }
        }

        // Build HNSW index
        let mut graph_layers_builder = GraphLayersBuilder::new(num_vectors, m, m, ef, 1, true);
        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx, level);
        }
        for idx in 0..(num_vectors as PointOffsetType) {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone()).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_builder.link_new_point(idx, scorer);
        }

        // Create GPU search context
        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let mut gpu_search_context = GpuSearchContext::new(
            Some(&debug_messenger),
            groups_count,
            &storage.borrow(),
            dim,
            m,
            m,
            ef,
            num_vectors,
        )
        .unwrap();

        // Upload HNSW links to GPU
        for idx in 0..(num_vectors as PointOffsetType) {
            let mut links = vec![];
            graph_layers_builder.links_map(idx, 0, |link| links.push(link));
            gpu_search_context.set_links(idx, &links).unwrap();
        }
        gpu_search_context.apply_links_patch().unwrap();
        gpu_search_context.run_context();

        TestData {
            gpu_search_context,
            vector_holder,
            graph_layers_builder,
        }
    }

    #[test]
    fn test_gpu_hnsw_search_on_level() {
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
        let search_requests_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::Storage,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        ));
        let upload_staging_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::CpuToGpu,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        ));
        upload_staging_buffer.upload_slice(&search_requests, 0);
        test.gpu_search_context.context.copy_gpu_buffer(
            upload_staging_buffer.clone(),
            search_requests_buffer.clone(),
            0,
            0,
            search_requests_buffer.size,
        );
        test.gpu_search_context.run_context();

        // create response and response staging buffers
        let responses_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        ));
        let responses_staging_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::GpuToCpu,
            responses_buffer.size,
        ));

        // Create test pipeline
        let shader = Arc::new(gpu::Shader::new(
            test.gpu_search_context.device.clone(),
            include_bytes!("./shaders/compiled/test_hnsw_search.spv"),
        ));
        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(test.gpu_search_context.device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, search_requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(
                1,
                test.gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set_layout
                    .clone(),
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
                    .gpu_nearest_heap
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                4,
                test.gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                5,
                test.gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set_layout
                    .clone(),
            )
            .add_shader(shader.clone())
            .build(test.gpu_search_context.device.clone());

        test.gpu_search_context.context.bind_pipeline(
            pipeline.clone(),
            &[
                descriptor_set.clone(),
                test.gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set
                    .clone(),
                test.gpu_search_context.gpu_links.descriptor_set.clone(),
                test.gpu_search_context
                    .gpu_nearest_heap
                    .descriptor_set
                    .clone(),
                test.gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set
                    .clone(),
                test.gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set
                    .clone(),
            ],
        );
        test.gpu_search_context.context.dispatch(groups_count, 1, 1);
        test.gpu_search_context.run_context();

        // Download response
        test.gpu_search_context.context.copy_gpu_buffer(
            responses_buffer.clone(),
            responses_staging_buffer.clone(),
            0,
            0,
            responses_buffer.size,
        );
        test.gpu_search_context.run_context();
        let mut gpu_responses_1 = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer.download_slice(&mut gpu_responses_1, 0);
        let gpu_responses_1 = gpu_responses_1
            .chunks_exact(ef)
            .map(|r| r.to_owned())
            .collect_vec();

        // restart search to check reset
        test.gpu_search_context.reset_context();
        test.gpu_search_context.run_context();
        test.gpu_search_context.context.bind_pipeline(
            pipeline,
            &[
                descriptor_set.clone(),
                test.gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set
                    .clone(),
                test.gpu_search_context.gpu_links.descriptor_set.clone(),
                test.gpu_search_context
                    .gpu_nearest_heap
                    .descriptor_set
                    .clone(),
                test.gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set
                    .clone(),
                test.gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set
                    .clone(),
            ],
        );
        test.gpu_search_context.context.dispatch(groups_count, 1, 1);
        test.gpu_search_context.run_context();

        // Download response second time
        test.gpu_search_context.context.copy_gpu_buffer(
            responses_buffer.clone(),
            responses_staging_buffer.clone(),
            0,
            0,
            responses_buffer.size,
        );
        test.gpu_search_context.run_context();
        let mut gpu_responses_2 = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer.download_slice(&mut gpu_responses_2, 0);
        let gpu_responses_2 = gpu_responses_2
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
        }
    }

    #[test]
    fn test_gpu_greedy_search() {
        let num_vectors = 1024;
        let groups_count = 8;
        let dim = 64;
        let m = 16;
        let ef = 32;

        let mut test = create_test_data(num_vectors, groups_count, dim, m, ef);

        // create request data
        let mut search_requests = vec![];
        for i in 0..groups_count {
            search_requests.push(GreedySearchRequest {
                id: (num_vectors + i) as PointOffsetType,
                entry: 0,
            });
        }

        let gpu_responses = test
            .gpu_search_context
            .greedy_search(&search_requests)
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
            assert_eq!(search_result.idx, gpu_responses[i].idx);
            assert!((search_result.score - gpu_responses[i].score).abs() < 1e-5);
        }
    }

    #[test]
    fn test_gpu_heuristic() {
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
        let search_requests_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::Storage,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        ));
        let upload_staging_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::CpuToGpu,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        ));
        upload_staging_buffer.upload_slice(&search_requests, 0);
        test.gpu_search_context.context.copy_gpu_buffer(
            upload_staging_buffer.clone(),
            search_requests_buffer.clone(),
            0,
            0,
            search_requests_buffer.size,
        );
        test.gpu_search_context.run_context();

        // create response and response staging buffers
        let responses_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        ));
        let responses_staging_buffer = Arc::new(gpu::Buffer::new(
            test.gpu_search_context.device.clone(),
            gpu::BufferType::GpuToCpu,
            responses_buffer.size,
        ));

        // Create test pipeline
        let shader = Arc::new(gpu::Shader::new(
            test.gpu_search_context.device.clone(),
            include_bytes!("./shaders/compiled/test_heuristic.spv"),
        ));
        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(test.gpu_search_context.device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, search_requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(
                1,
                test.gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set_layout
                    .clone(),
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
                    .gpu_nearest_heap
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                4,
                test.gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                5,
                test.gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set_layout
                    .clone(),
            )
            .add_shader(shader.clone())
            .build(test.gpu_search_context.device.clone());

        test.gpu_search_context.context.bind_pipeline(
            pipeline.clone(),
            &[
                descriptor_set.clone(),
                test.gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set
                    .clone(),
                test.gpu_search_context.gpu_links.descriptor_set.clone(),
                test.gpu_search_context
                    .gpu_nearest_heap
                    .descriptor_set
                    .clone(),
                test.gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set
                    .clone(),
                test.gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set
                    .clone(),
            ],
        );
        test.gpu_search_context.context.dispatch(groups_count, 1, 1);
        test.gpu_search_context.run_context();

        // Download response
        test.gpu_search_context.context.copy_gpu_buffer(
            responses_buffer.clone(),
            responses_staging_buffer.clone(),
            0,
            0,
            responses_buffer.size,
        );
        test.gpu_search_context.run_context();
        let mut gpu_responses = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer.download_slice(&mut gpu_responses, 0);
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
        }
    }
}
