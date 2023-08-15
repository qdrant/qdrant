pub mod batched_points;
pub mod cpu_level_builder;
pub mod devices_manager;
pub mod gpu_candidates_heap;
pub mod gpu_graph_builder;
pub mod gpu_level_builder;
pub mod gpu_links;
pub mod gpu_nearest_heap;
pub mod gpu_search_context;
pub mod gpu_vector_storage;
pub mod gpu_visited_flags;
pub mod shader_builder;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use batched_points::BatchedPoints;
use bitvec::vec::BitVec;
use devices_manager::DevicesMaganer;
use lazy_static::lazy_static;
use parking_lot::{Mutex, RwLock};

use super::graph_layers_builder::GraphLayersBuilder;
use crate::common::operation_error::{OperationError, OperationResult};

lazy_static! {
    static ref GPU_INSTANCE: OperationResult<Arc<gpu::Instance>> = create_gpu_instance();
    static ref GPU_DEVICE: OperationResult<Arc<gpu::Device>> = create_gpu_device();
    static ref GPU_DEVICE_FILER: Mutex<String> = Mutex::new("".to_string());
    pub static ref GPU_DEVICES_MANAGER: OperationResult<DevicesMaganer> = init_devices_manager();
}

/// Each GPU operation has a timeout by Vulkan API specification.
/// Choose large enough timeout.
/// We cannot use too small timeout and check stopper in the loop because
/// GPU resources should be alive while GPU operation is in progress.
static GPU_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

static GPU_INDEXING: AtomicBool = AtomicBool::new(false);
static GPU_WAIT_FREE: AtomicBool = AtomicBool::new(true);
static GPU_FORCE_HALF_PRECISION: AtomicBool = AtomicBool::new(false);
static GPU_MAX_GROUPS: AtomicUsize = AtomicUsize::new(GPU_MAX_GROUPS_COUNT_DEFAULT);
pub const GPU_MAX_GROUPS_COUNT_DEFAULT: usize = 512;
static GPU_MIN_POINTS_COUNT: AtomicUsize = AtomicUsize::new(10_000);
static GPU_DEVICE_START_INDEX: AtomicUsize = AtomicUsize::new(0);
static GPU_DEVICES_COUNT: AtomicUsize = AtomicUsize::new(usize::MAX);
static GPU_PARALLEL_INDEXES: AtomicUsize = AtomicUsize::new(0);

fn create_gpu_instance() -> OperationResult<Arc<gpu::Instance>> {
    Ok(gpu::Instance::new(None, None, false)?)
}

fn init_devices_manager() -> OperationResult<DevicesMaganer> {
    let instance = GPU_INSTANCE.clone()?;
    let filter = GPU_DEVICE_FILER.lock().clone();
    // TODO(gpu): add start index and count
    let devices_manager = DevicesMaganer::new(
        instance,
        &filter,
        GPU_DEVICE_START_INDEX.load(Ordering::Relaxed),
        GPU_DEVICES_COUNT.load(Ordering::Relaxed),
        GPU_WAIT_FREE.load(Ordering::Relaxed),
        GPU_PARALLEL_INDEXES.load(Ordering::Relaxed),
    )?;
    Ok(devices_manager)
}

fn create_gpu_device() -> OperationResult<Arc<gpu::Device>> {
    let instance = GPU_INSTANCE.clone()?;
    let filter = GPU_DEVICE_FILER.lock().clone();

    for physical_device in instance.physical_devices() {
        let device_name = physical_device.name.to_owned();
        if filter.is_empty() || device_name.contains(&filter) {
            log::info!("Found GPU device: {}", device_name);
            return Ok(gpu::Device::new(instance.clone(), physical_device)?);
        }
    }

    Err(OperationError::service_error("No GPU device found"))
}

pub fn set_gpu_indexing(gpu_indexing: bool) {
    GPU_INDEXING.store(gpu_indexing, Ordering::Relaxed);
}

pub fn get_gpu_indexing() -> bool {
    GPU_INDEXING.load(Ordering::Relaxed)
}

pub fn set_wait_free(wait_free: bool) {
    GPU_WAIT_FREE.store(wait_free, Ordering::Relaxed);
}

pub fn set_gpu_force_half_precision(force_half_precision: bool) {
    GPU_FORCE_HALF_PRECISION.store(force_half_precision, Ordering::Relaxed);
}

pub fn get_gpu_force_half_precision() -> bool {
    GPU_FORCE_HALF_PRECISION.load(Ordering::Relaxed)
}

pub fn set_gpu_max_groups(max_groups: Option<usize>) {
    if let Some(max_groups) = max_groups {
        GPU_MAX_GROUPS.store(max_groups, Ordering::Relaxed);
    }
}

pub fn get_gpu_max_groups() -> usize {
    GPU_MAX_GROUPS.load(Ordering::Relaxed)
}

pub fn set_device_index(device_index: Option<usize>) {
    if let Some(device_index) = device_index {
        GPU_DEVICE_START_INDEX.store(device_index, Ordering::Relaxed);
    }
}

pub fn set_devices_count(devices_count: Option<usize>) {
    if let Some(devices_count) = devices_count {
        GPU_DEVICES_COUNT.store(devices_count, Ordering::Relaxed);
    }
}

pub fn set_device_filter(device_filter: &str) {
    let mut filter = GPU_DEVICE_FILER.lock();
    *filter = device_filter.to_string();
}

pub fn set_gpu_parallel_indexes(parallel_indexes: usize) {
    GPU_PARALLEL_INDEXES.store(parallel_indexes, Ordering::Relaxed);
}

pub fn get_gpu_min_points_count() -> usize {
    GPU_MIN_POINTS_COUNT.load(Ordering::Relaxed)
}

pub fn create_graph_layers_builder(
    batched_points: &BatchedPoints,
    num_vectors: usize,
    m: usize,
    m0: usize,
    ef: usize,
    entry_points_num: usize,
) -> OperationResult<GraphLayersBuilder> {
    // create graph layers builder
    let mut graph_layers_builder =
        GraphLayersBuilder::new(num_vectors, m, m0, ef, entry_points_num, true);

    // mark all vectors as ready
    graph_layers_builder.ready_list = RwLock::new(BitVec::repeat(true, num_vectors));

    // set first entry point
    graph_layers_builder.set_levels(
        batched_points.first_point_id,
        batched_points.levels_count - 1,
    );
    graph_layers_builder.entry_points.lock().new_point(
        batched_points.first_point_id,
        batched_points.levels_count - 1,
        |_| true,
    );

    // set levels
    for linking_point in &batched_points.points {
        graph_layers_builder.set_levels(linking_point.point_id, linking_point.level);
        let mut entry_points = graph_layers_builder.entry_points.lock();
        entry_points.new_point(linking_point.point_id, linking_point.level, |_| true);
    }

    Ok(graph_layers_builder)
}

#[cfg(test)]
mod tests {
    use ahash::HashSet;
    use common::types::PointOffsetType;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tempfile::TempDir;

    use super::batched_points::BatchedPoints;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::data_types::vectors::DenseVector;
    use crate::fixtures::index_fixtures::{FakeFilterContext, TestRawScorerProducer};
    use crate::fixtures::payload_fixtures::random_vector;
    use crate::index::hnsw_index::graph_layers::GraphLayers;
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::graph_links::GraphLinksRam;
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::spaces::simple::CosineMetric;
    use crate::types::Distance;
    use crate::vector_storage::chunked_vector_storage::VectorOffsetType;
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;
    use crate::vector_storage::{VectorStorage, VectorStorageEnum};

    #[allow(dead_code)]
    pub struct GpuGraphTestData {
        pub dir: TempDir,
        pub vector_storage: VectorStorageEnum,
        pub vector_holder: TestRawScorerProducer<CosineMetric>,
        pub graph_layers_builder: GraphLayersBuilder,
        pub search_vectors: Vec<DenseVector>,
    }

    pub fn create_gpu_graph_test_data(
        num_vectors: usize,
        dim: usize,
        m: usize,
        m0: usize,
        ef: usize,
        search_counts: usize,
    ) -> GpuGraphTestData {
        // Generate random vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder = TestRawScorerProducer::<CosineMetric>::new(dim, num_vectors, &mut rng);

        // upload vectors to storage
        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = open_simple_dense_vector_storage(
            db,
            DB_VECTOR_CF,
            dim,
            Distance::Cosine,
            &false.into(),
        )
        .unwrap();
        for idx in 0..num_vectors {
            let v = vector_holder.get_vector(idx as PointOffsetType);
            storage
                .insert_vector(idx as PointOffsetType, v.as_vec_ref())
                .unwrap();
        }

        // Build HNSW index
        let mut graph_layers_builder = GraphLayersBuilder::new(num_vectors, m, m0, ef, 1, true);
        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx, level);
        }

        let mut ids: Vec<_> = (0..num_vectors as PointOffsetType).collect();
        BatchedPoints::sort_points_by_level(
            &|point_id| graph_layers_builder.get_point_level(point_id),
            &mut ids,
        );

        for &idx in &ids {
            let fake_filter_context = FakeFilterContext {};
            let added_vector = vector_holder.vectors.get(idx as VectorOffsetType).to_vec();
            let raw_scorer = vector_holder.get_raw_scorer(added_vector.clone()).unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));
            graph_layers_builder.link_new_point(idx, scorer);
            raw_scorer.take_hardware_counter().discard_results();
        }

        let search_vectors = (0..search_counts)
            .map(|_| random_vector(&mut rng, dim))
            .collect();

        GpuGraphTestData {
            dir,
            vector_storage: storage,
            vector_holder,
            graph_layers_builder,
            search_vectors,
        }
    }

    pub fn compare_graph_layers_builders(
        graph_a: &GraphLayersBuilder,
        graph_b: &GraphLayersBuilder,
    ) {
        assert_eq!(graph_a.links_layers.len(), graph_b.links_layers.len());
        let num_vectors = graph_a.links_layers.len();
        for point_id in 0..num_vectors as PointOffsetType {
            let levels_a = graph_a.get_point_level(point_id);
            let levels_b = graph_b.get_point_level(point_id);
            assert_eq!(levels_a, levels_b);

            for level in (0..levels_a + 1).rev() {
                let links_a = graph_a.links_layers[point_id as usize][level]
                    .read()
                    .clone();
                let links_b = graph_b.links_layers[point_id as usize][level]
                    .read()
                    .clone();
                if links_a != links_b {
                    log::error!("Wrong links point_id={} at level {}", point_id, level);
                }
                assert_eq!(links_a, links_b);
            }
        }
    }

    pub fn check_graph_layers_builders_quality(
        graph: GraphLayersBuilder,
        test: GpuGraphTestData,
        top: usize,
        ef: usize,
        accuracy: f32,
    ) {
        let graph: GraphLayers<GraphLinksRam> = graph.into_graph_layers(None).unwrap();
        let ref_graph: GraphLayers<GraphLinksRam> =
            test.graph_layers_builder.into_graph_layers(None).unwrap();

        let mut total_sames = 0;
        let total_top = top * test.search_vectors.len();
        for search_vector in &test.search_vectors {
            let fake_filter_context = FakeFilterContext {};
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(search_vector.clone())
                .unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let search_result_gpu = graph.search(top, ef, scorer, None);
            raw_scorer.take_hardware_counter().discard_results();

            let fake_filter_context = FakeFilterContext {};
            let raw_scorer = test
                .vector_holder
                .get_raw_scorer(search_vector.clone())
                .unwrap();
            let scorer = FilteredScorer::new(raw_scorer.as_ref(), Some(&fake_filter_context));

            let search_result_cpu = ref_graph.search(top, ef, scorer, None);
            raw_scorer.take_hardware_counter().discard_results();

            let mut gpu_set = HashSet::default();
            let mut cpu_set = HashSet::default();
            for (gpu_id, cpu_id) in search_result_gpu.iter().zip(search_result_cpu.iter()) {
                gpu_set.insert(gpu_id.idx);
                cpu_set.insert(cpu_id.idx);
            }

            total_sames += gpu_set.intersection(&cpu_set).count();
        }
        log::debug!(
            "total_sames: {}, total_top: {}, div {}",
            total_sames,
            total_top,
            total_sames as f32 / total_top as f32
        );
        assert!(
            total_sames as f32 >= total_top as f32 * accuracy,
            "sames: {}, total_top: {}",
            total_sames,
            total_top
        );
    }
}
