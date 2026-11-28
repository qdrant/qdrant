use common::ambient;
use common::types::PointOffsetType;
use itertools::Itertools;
use rand::SeedableRng as _;
use rand::seq::IteratorRandom as _;

use super::utils::{Result, insert_distributed_vectors, sampler};
use crate::data_types::vectors::QueryVector;
use crate::fixtures::payload_context_fixture::create_id_tracker_fixture;
use crate::id_tracker::IdTrackerRead;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::segment_constructor::batched_reader::merge_from_single_source;
use crate::types::{Distance, Memory};
use crate::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;

/// A point scores bit for bit the same in a batch as alone: replicas compare scores exactly,
/// and a point is scored in batches of every size and kind.
#[test]
fn batch_scores_equal_single_scores() -> Result<()> {
    // One block of 64 floats and a tail
    const DIM: usize = 100;
    const POINTS: usize = 1000;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    let dir = tempfile::Builder::new()
        .prefix("immutable-storage")
        .tempdir()?;
    let mut storage = open_dense_vector_storage(dir.path(), DIM, Distance::Dot, Memory::Cold)?;
    {
        let mut volatile_storage = new_volatile_dense_vector_storage(DIM, Distance::Dot);
        insert_distributed_vectors(DIM, &mut volatile_storage, POINTS, &mut sampler(&mut rng))?;
        merge_from_single_source(&mut storage, &volatile_storage, POINTS as PointOffsetType)?;
    }
    let id_tracker = create_id_tracker_fixture(POINTS);

    let query: QueryVector = sampler(&mut rng).take(DIM).collect_vec().into();
    let _scope = ambient::test_guard();
    let mut scorer =
        FilteredScorer::new_for_test(query, &storage, id_tracker.deleted_point_bitslice());

    // Consecutive ids, then scattered ids: chunks of both kinds and one mixed chunk
    let mut ids: Vec<PointOffsetType> = (100..180).collect();
    ids.extend((0..POINTS as PointOffsetType).sample(&mut rng, 150));

    let batch = scorer.score_points_unfiltered(&ids)?.collect_vec();
    assert_eq!(batch.len(), ids.len());
    for point in batch {
        let single = scorer.score_point(point.idx)?;
        assert_eq!(
            point.score.to_bits(),
            single.to_bits(),
            "point {}",
            point.idx
        );
    }
    Ok(())
}
