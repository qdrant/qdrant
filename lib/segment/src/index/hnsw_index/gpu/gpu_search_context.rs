use std::sync::Arc;

#[repr(C)]
pub struct GpuSearchContextParamsBuffer {
    nearest_capacity: u32,
    candidates_capacity: u32,
    visited_flags_capacity: u32,
    generation: u32,
}

pub struct GpuSearchContext {
    pub device: Arc<gpu::Device>,
    pub search_context_params_buffer: Arc<gpu::Buffer>,
    pub nearest_buffer: Arc<gpu::Buffer>,
    pub candidates_buffer: Arc<gpu::Buffer>,
    pub visited_flags_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
    pub params: GpuSearchContextParamsBuffer,
    pub params_staging_buffer: Arc<gpu::Buffer>,
}

impl GpuSearchContext {
    pub fn new(
        threads_count: usize,
        points_count: usize,
        m: usize,
        ef: usize,
        candidates_capacity: usize,
        device: Arc<gpu::Device>,
    ) -> Self {
        let nearest_capacity = std::cmp::max(m + 1, ef);

        let visited_flags_capacity = if points_count % 4 == 0 {
            points_count / 4
        } else {
            points_count / 4 + 1
        };

        let search_context_params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuSearchContextParamsBuffer>(),
        ));

        let params_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            std::mem::size_of::<GpuSearchContextParamsBuffer>(),
        ));

        let params = GpuSearchContextParamsBuffer {
            nearest_capacity: nearest_capacity as u32,
            candidates_capacity: candidates_capacity as u32,
            visited_flags_capacity: visited_flags_capacity as u32,
            generation: 0,
        };
        params_staging_buffer.upload(&params, 0);

        let mut upload_context = gpu::Context::new(device.clone());
        upload_context.copy_gpu_buffer(
            params_staging_buffer.clone(),
            search_context_params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuSearchContextParamsBuffer>(),
        );
        upload_context.run();
        upload_context.wait_finish();

        let nearest_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            threads_count
                * nearest_capacity
                * (std::mem::size_of::<f32>() + std::mem::size_of::<u32>()),
        ));

        let candidates_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            threads_count
                * candidates_capacity
                * (std::mem::size_of::<f32>() + std::mem::size_of::<u32>()),
        ));

        let visited_flags_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            threads_count * visited_flags_capacity * std::mem::size_of::<u32>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .add_storage_buffer(3)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, search_context_params_buffer.clone())
            .add_storage_buffer(1, nearest_buffer.clone())
            .add_storage_buffer(2, candidates_buffer.clone())
            .add_storage_buffer(3, visited_flags_buffer.clone())
            .build();

        println!(
            "Visited buffer size {} MB",
            visited_flags_buffer.size / 1024 / 1024
        );
        println!(
            "Candidates buffer size {} MB",
            candidates_buffer.size / 1024 / 1024
        );
        println!(
            "Nearest buffer size {} MB",
            nearest_buffer.size / 1024 / 1024
        );

        Self {
            device,
            search_context_params_buffer,
            nearest_buffer,
            candidates_buffer,
            visited_flags_buffer,
            descriptor_set_layout,
            descriptor_set,
            params,
            params_staging_buffer,
        }
    }

    pub fn clear(&mut self, gpu_context: &mut gpu::Context) {
        if self.params.generation == 255 {
            self.params.generation = 1;
            gpu_context.clear_buffer(self.visited_flags_buffer.clone());
        } else {
            self.params.generation += 1;
        }

        self.params_staging_buffer.upload(&self.params, 0);
        gpu_context.copy_gpu_buffer(
            self.params_staging_buffer.clone(),
            self.search_context_params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuSearchContextParamsBuffer>(),
        );

        gpu_context.clear_buffer(self.candidates_buffer.clone());
        gpu_context.clear_buffer(self.nearest_buffer.clone());
        gpu_context.run();
        gpu_context.wait_finish();
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;

    use atomic_refcell::AtomicRefCell;
    use bitvec::vec::BitVec;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::TempDir;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::random_vector;
    use crate::index::hnsw_index::gpu::gpu_links::GpuLinks;
    use crate::index::hnsw_index::gpu::gpu_vector_storage::GpuVectorStorage;
    use crate::index::hnsw_index::graph_layers::GraphLayersBase;
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::CosineMetric;
    use crate::types::{Distance, PointOffsetType};
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;
    use crate::vector_storage::{
        new_raw_scorer, ScoredPointOffset, VectorStorage, VectorStorageEnum,
    };

    struct TestData {
        _points: Vec<Vec<f32>>,
        _dir: TempDir,
        storage: Arc<AtomicRefCell<VectorStorageEnum>>,
        deleted_vec: BitVec,
        graph_layers: GraphLayersBuilder,
        _instance: Arc<gpu::Instance>,
        device: Arc<gpu::Device>,
        gpu_links: GpuLinks,
        gpu_search_context: GpuSearchContext,
        gpu_vector_storage: GpuVectorStorage,
    }

    fn new_test_data<R: Rng + ?Sized>(
        rnd: &mut R,
        m: usize,
        ef: usize,
        dim: usize,
        points_count: usize,
        search_count: usize,
        entry_points_num: usize,
        candidates_capacity: usize,
    ) -> TestData {
        let points = (0..points_count)
            .map(|_| CosineMetric::preprocess(&random_vector(rnd, dim)).unwrap())
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = open_simple_vector_storage(db, DB_VECTOR_CF, dim, Distance::Cosine).unwrap();
        {
            let mut borrowed_storage = storage.as_ref().borrow_mut();
            points.iter().enumerate().for_each(|(i, vec)| {
                borrowed_storage
                    .insert_vector(i as PointOffsetType, vec.as_slice())
                    .unwrap();
            });
        }
        let deleted_vec = BitVec::repeat(false, points_count);

        let mut graph_layers = GraphLayersBuilder::new_with_params(
            points_count,
            m,
            m,
            ef,
            entry_points_num,
            true,
            true,
        );

        for idx in 0..points_count {
            let level = graph_layers.get_random_layer(rnd);
            graph_layers.set_levels(idx as PointOffsetType, level);
        }

        for idx in 0..points_count - search_count {
            let borrowed_storage = storage.borrow();
            let vector = borrowed_storage.get_vector(idx as PointOffsetType).to_vec();
            let raw_scorer = new_raw_scorer(vector, &borrowed_storage, &deleted_vec);
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), None);
            graph_layers.link_new_point(idx as PointOffsetType, scorer);
        }

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());
        let mut context = gpu::Context::new(device.clone());

        let mut gpu_links = GpuLinks::new(device.clone(), m, ef, m, points_count).unwrap();
        for idx in 0..points_count as PointOffsetType {
            let mut links = vec![];
            graph_layers.links_map(idx, 0, |link| {
                links.push(link);
            });
            gpu_links.set_links(idx, &links);
        }
        gpu_links.upload(&mut context, points_count);

        let gpu_search_context = GpuSearchContext::new(
            search_count,
            points_count,
            m,
            ef,
            candidates_capacity,
            device.clone(),
        );
        let gpu_vector_storage = GpuVectorStorage::new(device.clone(), &storage.borrow()).unwrap();

        TestData {
            _points: points,
            _dir: dir,
            storage,
            deleted_vec,
            graph_layers,
            _instance: instance,
            device,
            gpu_links,
            gpu_search_context,
            gpu_vector_storage,
        }
    }

    #[test]
    fn test_gpu_hnsw_seach_context() {
        let m = 8;
        let ef = 16;
        let dim = 32;
        let points_count = 1024;
        let search_count = 64;
        let entry_points_num = 1;
        let candidates_capacity = 500;
        let mut rnd = StdRng::seed_from_u64(42);

        let TestData {
            _points,
            _dir,
            storage,
            deleted_vec,
            graph_layers,
            _instance,
            device,
            gpu_links,
            mut gpu_search_context,
            gpu_vector_storage,
        } = new_test_data(
            &mut rnd,
            m,
            ef,
            dim,
            points_count,
            search_count,
            entry_points_num,
            candidates_capacity,
        );

        let mut search_requests: Vec<(PointOffsetType, PointOffsetType)> = vec![];
        let mut search_results: Vec<Vec<ScoredPointOffset>> = vec![];
        for idx in points_count - search_count..points_count {
            let borrowed_storage = storage.borrow();
            let vector = borrowed_storage.get_vector(idx as PointOffsetType).to_vec();
            let raw_scorer = new_raw_scorer(vector, &borrowed_storage, &deleted_vec);
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

            let entry_point = match graph_layers
                .get_entry_points()
                .borrow_mut()
                .get_entry_point(|_| scorer.check_vector(idx as PointOffsetType))
            {
                None => continue,
                Some(ep) => ep,
            };

            search_requests.push((idx as PointOffsetType, entry_point.point_id));
            let scored_entry = ScoredPointOffset {
                idx: entry_point.point_id,
                score: scorer.score_point(entry_point.point_id),
            };
            let search_result = graph_layers
                .search_on_level(scored_entry, 0, ef, &mut scorer)
                .into_vec();
            search_results.push(search_result);
        }

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/test_searcher.spv"),
        ));

        let requests_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            2 * search_count * std::mem::size_of::<u32>(),
        ));

        let responses_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            search_count * ef * (std::mem::size_of::<f32>() + std::mem::size_of::<u32>()),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            search_requests.len() * 2 * std::mem::size_of::<PointOffsetType>(),
        ));
        staging_buffer.upload_slice(&search_requests, 0);

        let mut context = gpu::Context::new(device.clone());
        context.copy_gpu_buffer(
            staging_buffer.clone(),
            requests_buffer.clone(),
            0,
            0,
            search_requests.len() * 2 * std::mem::size_of::<PointOffsetType>(),
        );
        context.run();
        context.wait_finish();

        gpu_search_context.clear(&mut context);

        context.bind_pipeline(
            pipeline.clone(),
            &[
                gpu_vector_storage.descriptor_set.clone(),
                gpu_links.descriptor_set.clone(),
                gpu_search_context.descriptor_set.clone(),
                descriptor_set.clone(),
            ],
        );
        context.dispatch(search_requests.len(), 1, 1);
        context.run();
        context.wait_finish();

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            search_requests.len() * ef * std::mem::size_of::<PointOffsetType>(),
        ));
        context.copy_gpu_buffer(
            responses_buffer.clone(),
            staging_buffer.clone(),
            0,
            0,
            search_requests.len() * ef * std::mem::size_of::<PointOffsetType>(),
        );
        context.run();
        context.wait_finish();

        let mut gpu_results: Vec<PointOffsetType> = vec![0; search_requests.len() * ef];
        staging_buffer.download_slice(&mut gpu_results, 0);

        for (i, search_result) in search_results.iter().enumerate() {
            let mut gpu_result = gpu_results[i * ef..(i + 1) * ef].to_vec();
            gpu_result.reverse();
            let cpu_result = search_result.iter().map(|r| r.idx).collect::<Vec<_>>();
            assert_eq!(gpu_result, cpu_result);
        }
    }

    /*
    #[test]
    fn test_gpu_hnsw_greedy_seach_context() {
        let m = 8;
        let ef = 16;
        let dim = 32;
        let points_count = 1024;
        let search_count = 64;
        let entry_points_num = 1;
        let candidates_capacity = 500;
        let mut rnd = StdRng::seed_from_u64(42);

        let TestData {
            _points,
            _dir,
            storage,
            deleted_vec,
            graph_layers,
            _instance,
            device,
            gpu_links,
            gpu_search_context,
            gpu_vector_storage,
        } = new_test_data(
            &mut rnd,
            m,
            ef,
            dim,
            points_count,
            search_count,
            entry_points_num,
            candidates_capacity,
        );

        let mut search_requests: Vec<(PointOffsetType, PointOffsetType)> = vec![];
        let mut search_results: Vec<PointOffsetType> = vec![];
        for idx in points_count - search_count..points_count {
            let borrowed_storage = storage.borrow();
            let vector = borrowed_storage.get_vector(idx as PointOffsetType).to_vec();
            let raw_scorer = new_raw_scorer(vector, &borrowed_storage, &deleted_vec);
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

            let entry_point = match graph_layers
                .get_entry_points()
                .borrow_mut()
                .get_entry_point(|_| scorer.check_vector(idx as PointOffsetType))
            {
                None => continue,
                Some(ep) => ep,
            };

            search_requests.push((idx as PointOffsetType, entry_point.point_id));
            let search_result = graph_layers
                .search_entry(
                    entry_point.point_id,
                    0,
                    0,
                    &mut scorer,
                ).idx;
            search_results.push(search_result);
        }

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/test_greedy_searcher.spv"),
        ));

        let requests_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            2 * search_count * std::mem::size_of::<u32>(),
        ));

        let responses_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            search_count * std::mem::size_of::<u32>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            search_requests.len() * 2 * std::mem::size_of::<PointOffsetType>(),
        ));
        staging_buffer.upload_slice(&search_requests, 0);

        let mut context = gpu::Context::new(device.clone());
        context.copy_gpu_buffer(
            staging_buffer.clone(),
            requests_buffer.clone(),
            0,
            0,
            search_requests.len() * 2 * std::mem::size_of::<PointOffsetType>(),
        );
        context.run();
        context.wait_finish();

        context.bind_pipeline(
            pipeline.clone(),
            &[
                gpu_vector_storage.descriptor_set.clone(),
                gpu_links.descriptor_set.clone(),
                gpu_search_context.descriptor_set.clone(),
                descriptor_set.clone(),
            ],
        );
        context.dispatch(search_requests.len(), 1, 1);
        context.run();
        context.wait_finish();

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            search_requests.len() * std::mem::size_of::<PointOffsetType>(),
        ));
        context.copy_gpu_buffer(
            responses_buffer.clone(),
            staging_buffer.clone(),
            0,
            0,
            search_requests.len() * std::mem::size_of::<PointOffsetType>(),
        );
        context.run();
        context.wait_finish();

        let mut gpu_results: Vec<PointOffsetType> = vec![0; search_requests.len()];
        staging_buffer.download_slice(&mut gpu_results, 0);

        for (i, &cpu_result) in search_results.iter().enumerate() {
            let gpu_result = gpu_results[i];
            assert_eq!(gpu_result, cpu_result);
        }
    }
    */

    #[test]
    fn test_gpu_hnsw_heuristic() {
        let m = 8;
        let ef = 16;
        let dim = 32;
        let points_count = 1024;
        let search_count = 64;
        let entry_points_num = 1;
        let candidates_capacity = 500;
        let mut rnd = StdRng::seed_from_u64(42);

        let TestData {
            _points,
            _dir,
            storage,
            deleted_vec,
            graph_layers,
            _instance,
            device,
            gpu_links,
            mut gpu_search_context,
            gpu_vector_storage,
        } = new_test_data(
            &mut rnd,
            m,
            ef,
            dim,
            points_count,
            search_count,
            entry_points_num,
            candidates_capacity,
        );

        let mut search_requests: Vec<(PointOffsetType, PointOffsetType)> = vec![];
        let mut heuristic_results: Vec<Vec<PointOffsetType>> = vec![];
        for idx in points_count - search_count..points_count {
            let borrowed_storage = storage.borrow();
            let vector = borrowed_storage.get_vector(idx as PointOffsetType).to_vec();
            let raw_scorer = new_raw_scorer(vector, &borrowed_storage, &deleted_vec);
            let mut scorer = FilteredScorer::new(raw_scorer.as_ref(), None);

            let entry_point = match graph_layers
                .get_entry_points()
                .borrow_mut()
                .get_entry_point(|_| scorer.check_vector(idx as PointOffsetType))
            {
                None => continue,
                Some(ep) => ep,
            };

            search_requests.push((idx as PointOffsetType, entry_point.point_id));
            let scored_entry = ScoredPointOffset {
                idx: entry_point.point_id,
                score: scorer.score_point(entry_point.point_id),
            };
            let search_result = graph_layers.search_on_level(scored_entry, 0, ef, &mut scorer);
            let selected_result =
                GraphLayersBuilder::select_candidates_with_heuristic(search_result, m, |i, j| {
                    scorer.score_internal(i, j)
                });
            heuristic_results.push(selected_result);
        }

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/test_heuristic.spv"),
        ));

        let requests_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            2 * search_count * std::mem::size_of::<u32>(),
        ));

        let responses_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            search_count * (m + 1) * std::mem::size_of::<u32>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, requests_buffer.clone())
            .add_storage_buffer(1, responses_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_search_context.descriptor_set_layout.clone())
            .add_descriptor_set_layout(3, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            search_requests.len() * 2 * std::mem::size_of::<PointOffsetType>(),
        ));
        staging_buffer.upload_slice(&search_requests, 0);

        let mut context = gpu::Context::new(device.clone());
        context.copy_gpu_buffer(
            staging_buffer.clone(),
            requests_buffer.clone(),
            0,
            0,
            search_requests.len() * 2 * std::mem::size_of::<PointOffsetType>(),
        );
        context.run();
        context.wait_finish();

        gpu_search_context.clear(&mut context);

        context.bind_pipeline(
            pipeline.clone(),
            &[
                gpu_vector_storage.descriptor_set.clone(),
                gpu_links.descriptor_set.clone(),
                gpu_search_context.descriptor_set.clone(),
                descriptor_set.clone(),
            ],
        );
        context.dispatch(search_requests.len(), 1, 1);
        context.run();
        context.wait_finish();

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            search_count * (m + 1) * std::mem::size_of::<u32>(),
        ));
        context.copy_gpu_buffer(
            responses_buffer.clone(),
            staging_buffer.clone(),
            0,
            0,
            search_count * (m + 1) * std::mem::size_of::<u32>(),
        );
        context.run();
        context.wait_finish();

        let mut gpu_results: Vec<PointOffsetType> = vec![0; search_count * (m + 1)];
        staging_buffer.download_slice(&mut gpu_results, 0);

        for (i, search_result) in heuristic_results.iter().enumerate() {
            let gpu_full_result = &gpu_results[i * (m + 1)..(i + 1) * (m + 1)];
            let gpu_result = &gpu_full_result[1..1 + gpu_full_result[0] as usize];
            assert_eq!(gpu_result, search_result);
        }
    }
}
