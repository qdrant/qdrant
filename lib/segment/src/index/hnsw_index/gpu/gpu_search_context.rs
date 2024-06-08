use std::sync::Arc;

use common::types::PointOffsetType;

use super::gpu_candidates_heap::GpuCandidatesHeap;
use super::gpu_links::GpuLinks;
use super::gpu_nearest_heap::GpuNearestHeap;
use super::gpu_vector_storage::GpuVectorStorage;
use super::gpu_visited_flags::GpuVisitedFlags;
use crate::common::operation_error::OperationResult;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub struct GpuSearchContext {
    pub device: Arc<gpu::Device>,
    pub groups_count: usize,
    pub gpu_vector_storage: GpuVectorStorage,
    pub gpu_links: GpuLinks,
    pub gpu_nearest_heap: GpuNearestHeap,
    pub gpu_candidates_heap: GpuCandidatesHeap,
    pub gpu_visited_flags: GpuVisitedFlags,
}

impl GpuSearchContext {
    pub fn new(
        device: Arc<gpu::Device>,
        groups_count: usize,
        vector_storage: &VectorStorageEnum,
        dim: usize,
        m: usize,
        m0: usize,
        ef: usize,
        max_patched_points: usize,
    ) -> OperationResult<Self> {
        let points_count = vector_storage.total_vector_count();
        let candidates_capacity = points_count;
        Ok(Self {
            gpu_vector_storage: GpuVectorStorage::new(device.clone(), vector_storage, dim)?,
            gpu_links: GpuLinks::new(device.clone(), m, m0, points_count, max_patched_points)?,
            gpu_nearest_heap: GpuNearestHeap::new(device.clone(), groups_count, ef)?,
            gpu_candidates_heap: GpuCandidatesHeap::new(
                device.clone(),
                groups_count,
                candidates_capacity,
            )?,
            gpu_visited_flags: GpuVisitedFlags::new(device.clone(), groups_count, points_count)?,
            device,
            groups_count,
        })
    }

    pub fn reset_context(&mut self, gpu_context: &mut gpu::Context) {
        self.gpu_visited_flags.clear(gpu_context);
    }

    pub fn clear_links(&mut self, gpu_context: &mut gpu::Context) -> OperationResult<()> {
        self.gpu_links.clear(gpu_context)
    }

    pub fn get_links(&self, point_id: PointOffsetType) -> &[PointOffsetType] {
        self.gpu_links.get_links(point_id)
    }

    pub fn set_links(
        &mut self,
        point_id: PointOffsetType,
        links: &[PointOffsetType],
    ) -> OperationResult<()> {
        self.gpu_links.set_links(point_id, links)
    }

    pub fn apply_links_patch(&mut self, gpu_context: &mut gpu::Context) -> OperationResult<()> {
        Ok(self.gpu_links.apply_gpu_patches(gpu_context))
    }

    pub fn update_layer_params(&mut self, m: usize, gpu_context: &mut gpu::Context) {
        self.gpu_links.update_params(gpu_context, m);
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

    #[test]
    fn test_gpu_hnsw_search_on_level() {
        let num_vectors = 1024;
        let groups_count = 8;
        let dim = 64;
        let m = 16;
        let ef = 32;

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
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let mut context = gpu::Context::new(device.clone());
        let mut gpu_search_context = GpuSearchContext::new(
            device.clone(),
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
        gpu_search_context.apply_links_patch(&mut context).unwrap();
        context.run();
        context.wait_finish();

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
            device.clone(),
            gpu::BufferType::Storage,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        ));
        let upload_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            search_requests.len() * std::mem::size_of::<TestSearchRequest>(),
        ));
        upload_staging_buffer.upload_slice(&search_requests, 0);
        context.copy_gpu_buffer(
            upload_staging_buffer.clone(),
            search_requests_buffer.clone(),
            0,
            0,
            search_requests_buffer.size,
        );
        context.run();
        context.wait_finish();

        // create response and response staging buffers
        let responses_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            groups_count * ef * std::mem::size_of::<ScoredPointOffset>(),
        ));
        let responses_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            responses_buffer.size,
        ));

        // Create test pipeline
        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/compiled/test_hnsw_search.spv"),
        ));
        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, search_requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(
                1,
                gpu_search_context
                    .gpu_vector_storage
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                2,
                gpu_search_context.gpu_links.descriptor_set_layout.clone(),
            )
            .add_descriptor_set_layout(
                3,
                gpu_search_context
                    .gpu_nearest_heap
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                4,
                gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set_layout
                    .clone(),
            )
            .add_descriptor_set_layout(
                5,
                gpu_search_context
                    .gpu_visited_flags
                    .descriptor_set_layout
                    .clone(),
            )
            .add_shader(shader.clone())
            .build(device.clone());

        context.bind_pipeline(
            pipeline.clone(),
            &[
                descriptor_set.clone(),
                gpu_search_context.gpu_vector_storage.descriptor_set.clone(),
                gpu_search_context.gpu_links.descriptor_set.clone(),
                gpu_search_context.gpu_nearest_heap.descriptor_set.clone(),
                gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set
                    .clone(),
                gpu_search_context.gpu_visited_flags.descriptor_set.clone(),
            ],
        );
        context.dispatch(groups_count, 1, 1);
        context.run();
        context.wait_finish();

        // Download response
        context.copy_gpu_buffer(
            responses_buffer.clone(),
            responses_staging_buffer.clone(),
            0,
            0,
            responses_buffer.size,
        );
        context.run();
        context.wait_finish();
        let mut gpu_responses_1 = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer.download_slice(&mut gpu_responses_1, 0);
        let gpu_responses_1 = gpu_responses_1
            .chunks_exact(ef)
            .map(|r| r.to_owned())
            .collect_vec();

        // restart search to check reset
        gpu_search_context.reset_context(&mut context);
        context.run();
        context.wait_finish();
        context.bind_pipeline(
            pipeline,
            &[
                descriptor_set.clone(),
                gpu_search_context.gpu_vector_storage.descriptor_set.clone(),
                gpu_search_context.gpu_links.descriptor_set.clone(),
                gpu_search_context.gpu_nearest_heap.descriptor_set.clone(),
                gpu_search_context
                    .gpu_candidates_heap
                    .descriptor_set
                    .clone(),
                gpu_search_context.gpu_visited_flags.descriptor_set.clone(),
            ],
        );
        context.dispatch(groups_count, 1, 1);
        context.run();
        context.wait_finish();

        // Download response second time
        context.copy_gpu_buffer(
            responses_buffer.clone(),
            responses_staging_buffer.clone(),
            0,
            0,
            responses_buffer.size,
        );
        context.run();
        context.wait_finish();
        let mut gpu_responses_2 = vec![ScoredPointOffset::default(); groups_count * ef];
        responses_staging_buffer.download_slice(&mut gpu_responses_2, 0);
        let gpu_responses_2 = gpu_responses_2
            .chunks_exact(ef)
            .map(|r| r.to_owned())
            .collect_vec();

        // Check response
        for i in 0..groups_count {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(num_vectors + i).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone()).unwrap();
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            let entry = ScoredPointOffset {
                idx: 0,
                score: scorer.score_point(0),
            };
            let search_result = graph_layers_builder
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
}
