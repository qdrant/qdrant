pub mod batched_points;
pub mod gpu_devices_manager;
pub mod gpu_graph_builder;
pub mod gpu_insert_context;
pub mod gpu_level_builder;
pub mod gpu_links;
pub mod gpu_vector_storage;
pub mod gpu_visited_flags;
pub mod shader_builder;

#[cfg(test)]
mod gpu_heap_tests;

#[cfg(test)]
mod relock_diagnostic_tests;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use batched_points::BatchedPoints;
use gpu_devices_manager::GpuDevicesMaganer;
use parking_lot::RwLock;

use super::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::HnswM;

pub static GPU_DEVICES_MANAGER: RwLock<Option<GpuDevicesMaganer>> = RwLock::new(None);

/// Bounded wait for `GpuDevicesMaganer::lock_device()` to acquire a free device — see that
/// function's own doc comment. Was previously unbounded: a device whose holder is stuck in a
/// lower-level driver call that never returns (not a clean Vulkan error, which already
/// recovers via the GPU_TIMEOUT/DEVICE_LOST path below) would hang every other caller forever
/// with no visibility.
///
/// Must stay above GPU_TIMEOUT: a lock holder may legitimately run a single GPU operation for
/// that long, so a shorter bound here would misreport healthy contention as a stuck device.
/// Derived from GPU_TIMEOUT rather than an independent literal so the two can't drift back out
/// of this relationship if either is tuned later.
static GPU_LOCK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(GPU_TIMEOUT.as_secs() * 2);

/// Each GPU operation has a timeout by Vulkan API specification.
/// Choose large enough timeout.
/// We cannot use too small timeout and check stopper in the loop because
/// GPU resources should be alive while GPU operation is in progress.
/// Bumped from 60s: some workloads' individual GPU dispatches (e.g. large multivector segments)
/// can genuinely take multiple minutes — not confirmed as an observed failure, just headroom.
static GPU_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// Warps count for GPU.
/// In other words, how many parallel points can be indexed by GPU.
static GPU_GROUPS_COUNT: AtomicUsize = AtomicUsize::new(GPU_GROUPS_COUNT_DEFAULT);
pub const GPU_GROUPS_COUNT_DEFAULT: usize = 512;

/// Global option from settings to force half precision on GPU for `f32` values.
static GPU_FORCE_HALF_PRECISION: AtomicBool = AtomicBool::new(false);

pub fn set_gpu_force_half_precision(force_half_precision: bool) {
    GPU_FORCE_HALF_PRECISION.store(force_half_precision, Ordering::Relaxed);
}

pub fn get_gpu_force_half_precision() -> bool {
    GPU_FORCE_HALF_PRECISION.load(Ordering::Relaxed)
}

pub fn set_gpu_groups_count(groups_count: Option<usize>) {
    if let Some(groups_count) = groups_count {
        GPU_GROUPS_COUNT.store(groups_count, Ordering::Relaxed);
    }
}

pub fn get_gpu_groups_count() -> usize {
    GPU_GROUPS_COUNT.load(Ordering::Relaxed)
}

fn create_graph_layers_builder(
    batched_points: &BatchedPoints,
    num_vectors: usize,
    hnsw_m: HnswM,
    ef: usize,
    entry_points_num: usize,
) -> GraphLayersBuilder {
    // create graph layers builder
    let mut graph_layers_builder =
        GraphLayersBuilder::new(num_vectors, hnsw_m, ef, entry_points_num, true);

    if let Some(first_point_id) = batched_points.first_point_id() {
        // set first entry point
        graph_layers_builder.get_entry_points().new_point(
            first_point_id,
            batched_points.levels_count() - 1,
            |_| true,
        );

        graph_layers_builder.set_ready(first_point_id);

        // set levels
        graph_layers_builder.set_levels(first_point_id, batched_points.levels_count() - 1);
        for batch in batched_points.iter_batches(0) {
            for linking_point in batch.points {
                graph_layers_builder.set_levels(linking_point.point_id, batch.level);
            }
        }
    }

    graph_layers_builder
}

#[cfg(test)]
mod tests {
    use ahash::HashSet;
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::types::PointOffsetType;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::batched_points::BatchedPoints;
    use crate::data_types::vectors::DenseVector;
    use crate::fixtures::index_fixtures::TestRawScorerProducer;
    use crate::fixtures::payload_fixtures::random_vector;
    use crate::index::hnsw_index::HnswM;
    use crate::index::hnsw_index::graph_layers::{GraphLayers, SearchAlgorithm};
    use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
    use crate::index::hnsw_index::graph_links::GraphLinksFormatParam;
    use crate::types::Distance;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::{
        DEFAULT_STOPPED, VectorStorage, VectorStorageEnum, VectorStorageRead,
    };

    pub struct GpuGraphTestData {
        pub vector_storage: VectorStorageEnum,
        pub vector_holder: TestRawScorerProducer,
        pub graph_layers_builder: GraphLayersBuilder,
        pub search_vectors: Vec<DenseVector>,
    }

    pub fn create_gpu_graph_test_data(
        num_vectors: usize,
        dim: usize,
        hnsw_m: HnswM,
        ef: usize,
        search_counts: usize,
    ) -> GpuGraphTestData {
        // Generate random vectors
        let mut rng = StdRng::seed_from_u64(42);
        let vector_holder =
            TestRawScorerProducer::new(dim, Distance::Cosine, num_vectors, false, &mut rng);

        // upload vectors to storage
        let mut storage = new_volatile_dense_vector_storage(dim, Distance::Cosine);
        for idx in 0..num_vectors as PointOffsetType {
            let v = vector_holder.storage().get_vector::<Random>(idx);
            storage
                .insert_vector(idx, v.as_vec_ref(), &HardwareCounterCell::new())
                .unwrap();
        }

        // Build HNSW index
        let mut graph_layers_builder = GraphLayersBuilder::new(num_vectors, hnsw_m, ef, 1, true);
        for idx in 0..(num_vectors as PointOffsetType) {
            let level = graph_layers_builder.get_random_layer(&mut rng);
            graph_layers_builder.set_levels(idx, level);
        }

        let mut ids: Vec<_> = (0..num_vectors as PointOffsetType).collect();
        BatchedPoints::sort_points_by_level(
            |point_id| graph_layers_builder.get_point_level(point_id),
            &mut ids,
        );

        for &idx in &ids {
            let scorer = vector_holder.internal_scorer(idx);
            graph_layers_builder.link_new_point(idx, scorer);
        }

        let search_vectors = (0..search_counts)
            .map(|_| random_vector(&mut rng, dim))
            .collect();

        GpuGraphTestData {
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
        assert_eq!(graph_a.links_layers().len(), graph_b.links_layers().len());
        let num_vectors = graph_a.links_layers().len();
        for point_id in 0..num_vectors as PointOffsetType {
            let levels_a = graph_a.get_point_level(point_id);
            let levels_b = graph_b.get_point_level(point_id);
            assert_eq!(levels_a, levels_b);

            for level in (0..levels_a + 1).rev() {
                let links_a = graph_a.links_layers()[point_id as usize][level]
                    .read()
                    .links()
                    .to_vec();
                let links_b = graph_b.links_layers()[point_id as usize][level]
                    .read()
                    .links()
                    .to_vec();
                if links_a != links_b {
                    log::error!("Wrong links point_id={point_id} at level {level}");
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
        let graph: GraphLayers = graph.into_graph_layers_ram(GraphLinksFormatParam::Plain);
        let ref_graph: GraphLayers = test
            .graph_layers_builder
            .into_graph_layers_ram(GraphLinksFormatParam::Plain);

        let mut total_sames = 0;
        let total_top = top * test.search_vectors.len();
        for search_vector in &test.search_vectors {
            let mut scorer = test.vector_holder.scorer(search_vector.clone());

            let search_result_gpu = graph
                .search(
                    top,
                    ef,
                    SearchAlgorithm::Hnsw,
                    &mut scorer,
                    graph.unfiltered_entry_point(),
                    &DEFAULT_STOPPED,
                )
                .unwrap();

            let mut scorer = test.vector_holder.scorer(search_vector.clone());

            let search_result_cpu = ref_graph
                .search(
                    top,
                    ef,
                    SearchAlgorithm::Hnsw,
                    &mut scorer,
                    ref_graph.unfiltered_entry_point(),
                    &DEFAULT_STOPPED,
                )
                .unwrap();

            let mut gpu_set = HashSet::default();
            let mut cpu_set = HashSet::default();
            for (gpu_id, cpu_id) in search_result_gpu.iter().zip(search_result_cpu.iter()) {
                gpu_set.insert(gpu_id.idx);
                cpu_set.insert(cpu_id.idx);
            }

            total_sames += gpu_set.intersection(&cpu_set).count();
        }
        assert!(
            total_sames as f32 >= total_top as f32 * accuracy,
            "sames: {total_sames}, total_top: {total_top}, div {}",
            total_sames as f32 / total_top as f32,
        );
    }
}
