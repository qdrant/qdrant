use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::top_k::TopK;
use common::types::{ScoreType, ScoredPointOffset};
use itertools::Itertools;
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::{QueryVector, VectorInternal};
use segment::types::{ScoredPoint, VectorNameBuf};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
use segment::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;
use segment::vector_storage::{VectorStorage, new_raw_scorer};
use tokio::runtime::Handle;

use crate::config::CollectionParams;
use crate::operations::types::{CollectionError, CollectionResult};
use crate::operations::universal_query::shard_query::MmrInternal;

/// Calculate the MMR (Maximal Marginal Relevance) score for a set of points with vectors.
///
/// Assumes the points have vectors attached. If not, the entire point will be discarded.
///
/// # Arguments
///
/// * `collection_params` - The parameters of the collection. Used to determine the right distance metric, or multivec config.
/// * `points_with_vector` - The points with vectors.
/// * `mmr` - The MMR parameters.
/// * `limit` - The maximum number of points to return.
/// * `search_runtime_handle` - The runtime handle for searching.
/// * `timeout` - The timeout for the operation.
/// * `hw_measurement_acc` - The hardware measurement accumulator.
///
/// # Returns
///
/// A vector of scored points.
pub async fn mmr_from_points_with_vector(
    collection_params: &CollectionParams,
    points_with_vector: impl IntoIterator<Item = ScoredPoint>,
    mmr: MmrInternal,
    limit: usize,
    search_runtime_handle: &Handle,
    timeout: Duration,
    hw_measurement_acc: HwMeasurementAcc,
) -> Result<Vec<ScoredPoint>, CollectionError> {
    let (vectors, candidates): (Vec<_>, Vec<_>) = points_with_vector
        .into_iter()
        .sorted_unstable_by_key(|p| p.id)
        .dedup_by(|a, b| a.id == b.id)
        .filter_map(|mut p| {
            let vector = p
                .vector
                .take()
                // silently ignore points without this named vector
                .and_then(|v| v.take(&mmr.using))?;
            Some((vector, p))
        })
        .unzip();

    debug_assert_eq!(vectors.len(), candidates.len());

    if candidates.len() < 2 {
        // can't compute MMR for less than 2 points, just return as is
        return Ok(candidates);
    }

    let max_similarities = max_similarities(
        collection_params,
        vectors,
        mmr.using,
        search_runtime_handle,
        timeout,
        hw_measurement_acc,
    )
    .await?;

    Ok(maximum_marginal_relevance(
        candidates,
        max_similarities,
        mmr.lambda,
        limit,
    ))
}

/// Selects the maximal similarity for each point in the provided set,
/// compared to each other within the same set.
///
/// Panics if there are less than 2 vectors.
async fn max_similarities(
    collection_params: &CollectionParams,
    vectors: Vec<VectorInternal>,
    using: VectorNameBuf,
    search_runtime_handle: &Handle,
    timeout: Duration,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<ScoreType>> {
    let num_vectors = vectors.len();

    // if we have less than 2 points, we can't build a matrix
    if num_vectors < 2 {
        panic!("There should be at least two vectors to calculate max similarities")
    }

    // Create temporary vector storage
    let mut volatile_storage = {
        let distance = collection_params.get_distance(&using)?;
        match &vectors[0] {
            VectorInternal::Dense(vector) => {
                new_volatile_dense_vector_storage(vector.len(), distance)
            }
            VectorInternal::Sparse(_sparse_vector) => new_volatile_sparse_vector_storage(),
            VectorInternal::MultiDense(typed_multi_dense_vector) => {
                let multivec_config = collection_params
                        .vectors
                        .get_params(&using)
                        .and_then(|vector_params| vector_params.multivector_config)
                        .ok_or_else(|| CollectionError::service_error(format!("multivectors are present for {using}, but no multivector config is defined")))?;
                new_volatile_multi_dense_vector_storage(
                    typed_multi_dense_vector.dim,
                    distance,
                    multivec_config,
                )
            }
        }
    };

    // Populate storage with vectors
    let hw_counter = HardwareCounterCell::disposable();
    for (key, vector) in (0..).zip(&vectors) {
        volatile_storage.insert_vector(key, vector.as_vector_ref(), &hw_counter)?;
    }

    let compute_max_scores = move || {
        // Prepare scorers
        let raw_scorers = vectors
            .into_iter()
            .map(|vector| {
                let query = QueryVector::Nearest(vector);
                new_raw_scorer(
                    query,
                    &volatile_storage,
                    hw_measurement_acc.get_counter_cell(),
                )
            })
            .collect::<OperationResult<Vec<_>>>()?;

        // Compute all scores, retain only the top score which isn't the same vector
        let all_offsets = (0..num_vectors as u32).collect::<Vec<_>>();
        let max_scores = raw_scorers
            .into_iter()
            .enumerate()
            .map(|(tmp_id, scorer)| {
                let mut scores = vec![0.0; num_vectors];
                scorer.score_points(&all_offsets, &mut scores);
                scores
                    .into_iter()
                    .enumerate()
                    // exclude the vector score against itself
                    .filter_map(|(i, score)| (i != tmp_id).then_some(score))
                    .max_by(|a, b| a.partial_cmp(b).expect("No NaN"))
                    .expect("There should be at least two vectors")
            })
            .collect::<Vec<_>>();

        Ok(max_scores)
    };

    tokio::time::timeout(
        timeout,
        search_runtime_handle.spawn_blocking(compute_max_scores),
    )
    .await
    .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "max_similarities"))??
}

/// Apply Maximum Marginal Relevance algorithm.
///
/// # Arguments
///
/// * `candidates` - the list of points to select from. Must be parallel with the `max_similarities` list.
/// * `max_similarities` - the list of maximum similarities for each point. Must be parallel with the `candidates` list.
/// * `lambda` - the lambda parameter for the MMR algorithm.
/// * `limit` - the maximum number of points to select.
fn maximum_marginal_relevance(
    candidates: Vec<ScoredPoint>,
    max_similarities: Vec<ScoreType>,
    lambda: f32,
    limit: usize,
) -> Vec<ScoredPoint> {
    let mut top_k = TopK::new(limit);

    for (idx, candidate) in (0..).zip(&candidates) {
        let relevance_score = candidate.score;

        let max_similarity = max_similarities[idx];

        // Calculate MMR score: λ * relevance - (1 - λ) * max_similarity
        let mmr_score = lambda * relevance_score - (1.0 - lambda) * max_similarity;

        top_k.push(ScoredPointOffset {
            score: mmr_score,
            idx: idx as u32,
        });
    }

    top_k
        .into_vec()
        .into_iter()
        .map(|ScoredPointOffset { idx, score }| {
            let mut selected = candidates[idx as usize].clone();
            selected.score = score;
            selected
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use common::types::ScoreType;
    use rstest::rstest;
    use segment::data_types::vectors::{
        MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
    };
    use segment::types::{
        Distance, MultiVectorComparator, MultiVectorConfig, PointIdType, ScoredPoint, VectorNameBuf,
    };
    use sparse::common::sparse_vector::SparseVector;
    use strum::IntoEnumIterator;
    use tokio::runtime::Handle;

    use crate::collection::mmr::mmr_from_points_with_vector;
    use crate::config::CollectionParams;
    use crate::operations::types::VectorsConfig;
    use crate::operations::universal_query::shard_query::MmrInternal;
    use crate::operations::vector_params_builder::VectorParamsBuilder;

    /// Create a simple collection configuration with a single default vector.
    fn create_test_collection_params(distance: Distance, dim: usize) -> CollectionParams {
        CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(dim as u64, distance).build()),
            ..CollectionParams::empty()
        }
    }

    /// Create a collection configuration with a named vector.
    fn create_test_collection_params_named(
        distance: Distance,
        dim: usize,
        name: &str,
    ) -> CollectionParams {
        let mut vectors_map = BTreeMap::new();
        vectors_map.insert(
            name.to_string(),
            VectorParamsBuilder::new(dim as u64, distance).build(),
        );

        CollectionParams {
            vectors: VectorsConfig::Multi(vectors_map),
            ..CollectionParams::empty()
        }
    }

    /// Create a ScoredPoint with a dense vector attached.
    fn create_scored_point_with_vector(
        id: PointIdType,
        score: ScoreType,
        vector: Vec<f32>,
        vector_name: Option<&str>,
    ) -> ScoredPoint {
        let vector_internal = VectorInternal::Dense(vector);
        let mut vectors = HashMap::new();

        let name = vector_name.unwrap_or("");
        vectors.insert(name.to_string(), vector_internal);

        ScoredPoint {
            id,
            version: 0,
            score,
            payload: None,
            vector: Some(VectorStructInternal::Named(vectors)),
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a ScoredPoint without any vector attached.
    fn create_scored_point_without_vector(id: PointIdType, score: ScoreType) -> ScoredPoint {
        ScoredPoint {
            id,
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a ScoredPoint with a sparse vector attached.
    fn create_scored_point_with_sparse_vector(
        id: PointIdType,
        score: ScoreType,
        indices: Vec<u32>,
        values: Vec<f32>,
        vector_name: Option<&str>,
    ) -> ScoredPoint {
        let sparse_vector = SparseVector::new(indices, values).expect("Valid sparse vector");
        let vector_internal = VectorInternal::Sparse(sparse_vector);
        let mut vectors = HashMap::new();

        let name = vector_name.unwrap_or("");
        vectors.insert(name.to_string(), vector_internal);

        ScoredPoint {
            id,
            version: 0,
            score,
            payload: None,
            vector: Some(VectorStructInternal::Named(vectors)),
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a ScoredPoint with a multi-dense vector attached.
    fn create_scored_point_with_multi_vector(
        id: PointIdType,
        score: ScoreType,
        vectors: Vec<Vec<f32>>,
        vector_name: Option<&str>,
    ) -> ScoredPoint {
        let multi_vector = MultiDenseVectorInternal::new_unchecked(vectors);
        let vector_internal = VectorInternal::MultiDense(multi_vector);
        let mut vector_map = HashMap::new();

        let name = vector_name.unwrap_or("");
        vector_map.insert(name.to_string(), vector_internal);

        ScoredPoint {
            id,
            version: 0,
            score,
            payload: None,
            vector: Some(VectorStructInternal::Named(vector_map)),
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a collection configuration with multi-vectors.
    fn create_test_collection_params_multi(
        distance: Distance,
        dim: usize,
        name: &str,
    ) -> CollectionParams {
        let mut vectors_map = BTreeMap::new();
        let multi_config = MultiVectorConfig {
            comparator: MultiVectorComparator::MaxSim,
        };
        vectors_map.insert(
            name.to_string(),
            VectorParamsBuilder::new(dim as u64, distance)
                .with_multivector_config(multi_config)
                .build(),
        );

        CollectionParams {
            vectors: VectorsConfig::Multi(vectors_map),
            ..CollectionParams::empty()
        }
    }

    /// Test the basic MMR functionality with multiple lambda values
    #[tokio::test]
    #[rstest]
    #[case::full_relevance(1.0, &[1, 2, 3])]
    #[case::balanced(0.5, &[1, 3, 4])]
    #[case::full_diversity(0.0, &[3, 4, 1])]
    async fn test_mmr_lambda(#[case] lambda: f32, #[case] expected_order: &[u64]) {
        let collection_params = create_test_collection_params(Distance::Dot, 3);
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        let points = vec![
            create_scored_point_with_vector(1.into(), 0.98, vec![1.0, 0.0, 0.0], None),
            create_scored_point_with_vector(2.into(), 0.87, vec![0.2, 0.1, 0.1], None), // Intersecting all points
            create_scored_point_with_vector(3.into(), 0.86, vec![0.0, 1.0, 0.0], None), // Orthogonal to Point 1
            create_scored_point_with_vector(4.into(), 0.84, vec![0.0, 0.01, 1.0], None), // Orthogonal to Points 1 and small intersection with Point 3
        ];

        let mmr = MmrInternal {
            using: VectorNameBuf::from(""),
            lambda,
        };

        let result = mmr_from_points_with_vector(
            &collection_params,
            points.clone(),
            mmr,
            3,
            &handle,
            Duration::from_secs(10),
            hw_acc,
        )
        .await;

        let scored_points = result.unwrap();
        assert_eq!(scored_points.len(), 3);

        // With MMR, we should get 3 diverse points, and the first should be the highest original score
        // The exact order depends on the similarity calculations, but we should get all different points
        let selected_ids: Vec<_> = scored_points.iter().map(|p| p.id).collect();
        let expected_ids = expected_order
            .into_iter()
            .map(|&id| id.into())
            .collect::<Vec<_>>();

        assert_eq!(selected_ids, expected_ids);

        // The scores should be modified by MMR (different from original scores)
        assert_ne!(scored_points[0].score, 0.9); // MMR should modify the original scores
    }

    /// Test MMR behavior with insufficient points (< 2) for similarity computation.
    #[tokio::test]
    async fn test_mmr_less_than_two_points() {
        let collection_params = create_test_collection_params(Distance::Cosine, 3);
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        // Test with empty points
        let empty_points = vec![];
        let mmr = MmrInternal {
            using: VectorNameBuf::from(""),
            lambda: 0.5,
        };

        let result = mmr_from_points_with_vector(
            &collection_params,
            empty_points,
            mmr.clone(),
            5,
            &handle,
            Duration::from_secs(10),
            hw_acc.clone(),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        // Test with one point
        let single_point = vec![create_scored_point_with_vector(
            1.into(),
            0.9,
            vec![1.0, 0.0, 0.0],
            None,
        )];

        let result = mmr_from_points_with_vector(
            &collection_params,
            single_point,
            mmr,
            5,
            &handle,
            Duration::from_secs(10),
            hw_acc,
        )
        .await;

        assert!(result.is_ok());
        let scored_points = result.unwrap();
        assert_eq!(scored_points.len(), 1);
        assert_eq!(scored_points[0].id, 1.into());
    }

    #[tokio::test]
    async fn test_mmr_points_without_required_vector() {
        let collection_params = create_test_collection_params_named(Distance::Cosine, 3, "custom");
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        let points = vec![
            create_scored_point_with_vector(1.into(), 0.9, vec![1.0, 0.0, 0.0], Some("custom")),
            create_scored_point_without_vector(2.into(), 0.8), // No vector
            create_scored_point_with_vector(3.into(), 0.7, vec![0.0, 1.0, 0.0], Some("other")), // Wrong vector name
            create_scored_point_with_vector(4.into(), 0.6, vec![0.0, 0.0, 1.0], Some("custom")),
        ];

        let mmr = MmrInternal {
            using: VectorNameBuf::from("custom"),
            lambda: 0.5,
        };

        let result = mmr_from_points_with_vector(
            &collection_params,
            points,
            mmr,
            5,
            &handle,
            Duration::from_secs(10),
            hw_acc,
        )
        .await;

        assert!(result.is_ok());
        let scored_points = result.unwrap();
        // Only points 1 and 4 should remain (they have the "custom" vector)
        assert_eq!(scored_points.len(), 2);

        let selected_ids: Vec<_> = scored_points.iter().map(|p| p.id).collect();
        assert!(selected_ids.contains(&(1.into())));
        assert!(selected_ids.contains(&(4.into())));
    }

    #[tokio::test]
    async fn test_mmr_duplicate_points() {
        let collection_params = create_test_collection_params(Distance::Cosine, 3);
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        // Include duplicate point IDs
        let points = vec![
            create_scored_point_with_vector(1.into(), 0.9, vec![1.0, 0.0, 0.0], None),
            create_scored_point_with_vector(1.into(), 0.8, vec![0.5, 0.5, 0.0], None), // Duplicate ID
            create_scored_point_with_vector(2.into(), 0.7, vec![0.0, 1.0, 0.0], None),
        ];

        let mmr = MmrInternal {
            using: VectorNameBuf::from(""),
            lambda: 0.5,
        };

        let result = mmr_from_points_with_vector(
            &collection_params,
            points,
            mmr,
            5,
            &handle,
            Duration::from_secs(10),
            hw_acc,
        )
        .await;

        assert!(result.is_ok());
        let scored_points = result.unwrap();
        // Duplicates should be removed, so we should have 2 unique points
        assert_eq!(scored_points.len(), 2);

        let unique_ids: std::collections::HashSet<_> = scored_points.iter().map(|p| p.id).collect();
        assert_eq!(unique_ids.len(), 2);
    }

    #[tokio::test]
    async fn test_mmr_dense_vectors() {
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        // Test dense vectors with all distance metrics
        let dense_points = vec![
            create_scored_point_with_vector(1.into(), 0.9, vec![1.0, 0.0, 0.0], None),
            create_scored_point_with_vector(2.into(), 0.8, vec![0.0, 1.0, 0.0], None),
            create_scored_point_with_vector(3.into(), 0.7, vec![0.0, 0.0, 1.0], None),
        ];

        let mmr = MmrInternal {
            using: VectorNameBuf::from(""),
            lambda: 0.5,
        };

        // Test with all distance metrics for dense vectors
        for distance in Distance::iter() {
            let collection_params = create_test_collection_params(distance, 3);

            let result = mmr_from_points_with_vector(
                &collection_params,
                dense_points.clone(),
                mmr.clone(),
                3,
                &handle,
                Duration::from_secs(10),
                hw_acc.clone(),
            )
            .await;

            assert!(
                result.is_ok(),
                "Dense vectors failed for distance metric: {:?}",
                distance
            );
            let scored_points = result.unwrap();
            assert_eq!(scored_points.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_mmr_sparse_vectors() {
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        // Test sparse vectors with dot product (only supported distance for sparse)
        let sparse_vector_name = "sparse";
        let sparse_points = vec![
            create_scored_point_with_sparse_vector(
                4.into(),
                0.9,
                vec![0, 2, 5],
                vec![1.0, 0.5, 0.3],
                Some(sparse_vector_name),
            ),
            create_scored_point_with_sparse_vector(
                5.into(),
                0.8,
                vec![1, 3, 4],
                vec![0.8, 0.6, 0.4],
                Some(sparse_vector_name),
            ),
            create_scored_point_with_sparse_vector(
                6.into(),
                0.7,
                vec![0, 1, 6],
                vec![0.7, 0.9, 0.2],
                Some(sparse_vector_name),
            ),
        ];

        let sparse_collection_params =
            create_test_collection_params_named(Distance::Dot, 10, sparse_vector_name);
        let sparse_mmr = MmrInternal {
            using: VectorNameBuf::from(sparse_vector_name),
            lambda: 0.5,
        };

        let sparse_result = mmr_from_points_with_vector(
            &sparse_collection_params,
            sparse_points,
            sparse_mmr,
            3,
            &handle,
            Duration::from_secs(10),
            hw_acc.clone(),
        )
        .await
        .unwrap();

        assert_eq!(sparse_result.len(), 3);
    }

    #[tokio::test]
    async fn test_mmr_multi_vector() {
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        // Test multi-vectors with all supported distance metrics
        let multi_vector_name = "multi";
        let multi_points = vec![
            create_scored_point_with_multi_vector(
                7.into(),
                0.9,
                vec![vec![1.0, 0.0], vec![0.0, 1.0]],
                Some(multi_vector_name),
            ),
            create_scored_point_with_multi_vector(
                8.into(),
                0.8,
                vec![vec![0.0, 1.0], vec![1.0, 0.0]],
                Some(multi_vector_name),
            ),
            create_scored_point_with_multi_vector(
                9.into(),
                0.7,
                vec![vec![1.0, 1.0], vec![0.0, 0.0]],
                Some(multi_vector_name),
            ),
        ];

        let multi_mmr = MmrInternal {
            using: VectorNameBuf::from(multi_vector_name),
            lambda: 0.5,
        };

        for distance in Distance::iter() {
            let multi_collection_params =
                create_test_collection_params_multi(distance, 2, multi_vector_name);

            let multi_result = mmr_from_points_with_vector(
                &multi_collection_params,
                multi_points.clone(),
                multi_mmr.clone(),
                3,
                &handle,
                Duration::from_secs(10),
                hw_acc.clone(),
            )
            .await;

            assert!(
                multi_result.is_ok(),
                "Multi-vectors failed for distance metric: {:?}",
                distance
            );
            let multi_scored_points = multi_result.unwrap();
            assert_eq!(multi_scored_points.len(), 3);
        }
    }
}
