use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::ScoreType;
use indexmap::IndexSet;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use segment::common::operation_error::OperationResult;
use segment::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use segment::types::{ScoredPoint, VectorNameBuf};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
use segment::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;
use segment::vector_storage::{VectorStorage, VectorStorageEnum, new_raw_scorer};
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
    let (vectors, mut candidates): (Vec<_>, Vec<_>) = points_with_vector
        .into_iter()
        .unique_by(|p| p.id)
        .filter_map(|p| {
            let vector = p
                .vector
                .as_ref()
                // silently ignore points without this named vector
                .and_then(|v| v.get(&mmr.using))
                .map(|v| v.to_owned())?;
            Some((vector, p))
        })
        .unzip();

    debug_assert_eq!(vectors.len(), candidates.len());

    if candidates.is_empty() {
        return Ok(candidates);
    }

    let volatile_storage = create_volatile_storage(
        collection_params,
        &vectors,
        mmr.using,
        hw_measurement_acc.get_counter_cell(),
    )?;

    if candidates.len() < 2 {
        // can't compute MMR for less than 2 points, return with relevance score
        let scores = relevance_similarities(
            &volatile_storage,
            mmr.vector,
            hw_measurement_acc.get_counter_cell(),
        )?;

        for (p, score) in candidates.iter_mut().zip(scores) {
            p.score = score;
        }

        return Ok(candidates);
    }

    let compute_similarities = move || {
        // get similarities against query
        let query_similarities = relevance_similarities(
            &volatile_storage,
            mmr.vector,
            hw_measurement_acc.get_counter_cell(),
        )?;

        // get similarity matrix between candidates
        let similarity_matrix = similarity_matrix(&volatile_storage, vectors, hw_measurement_acc)?;

        CollectionResult::Ok((query_similarities, similarity_matrix))
    };

    let (query_similarities, similarity_matrix) = tokio::time::timeout(
        timeout,
        search_runtime_handle.spawn_blocking(compute_similarities),
    )
    .await
    .map_err(|_| CollectionError::timeout(timeout.as_secs() as usize, "mmr"))???;

    Ok(maximum_marginal_relevance(
        candidates,
        query_similarities,
        &similarity_matrix,
        mmr.lambda,
        limit,
    ))
}

/// Creates a volatile (in-memory and not persistent) vector storage and inserts the vectors in the provided order.
fn create_volatile_storage(
    collection_params: &CollectionParams,
    vectors: &[VectorInternal],
    using: VectorNameBuf,
    hw_counter: HardwareCounterCell,
) -> CollectionResult<VectorStorageEnum> {
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
    for (key, vector) in (0..).zip(vectors) {
        volatile_storage.insert_vector(key, VectorRef::from(vector), &hw_counter)?;
    }

    Ok(volatile_storage)
}

/// Compute the "relevance" similarity between a query vector and all vectors in the storage.
fn relevance_similarities(
    volatile_storage: &VectorStorageEnum,
    query_vector: VectorInternal,
    hw_counter: HardwareCounterCell,
) -> CollectionResult<Vec<ScoreType>> {
    let query = QueryVector::Nearest(query_vector);
    let query_scorer = new_raw_scorer(query, volatile_storage, hw_counter)?;

    // get similarity between candidates and query
    let ids: Vec<_> = (0..volatile_storage.total_vector_count() as u32).collect();
    let mut similarities = vec![0.0; ids.len()];
    query_scorer.score_points(&ids, &mut similarities);

    Ok(similarities)
}

/// Returns a symmetric matrix where entry (i,j) represents the similarity
/// between vector i and vector j. Diagonal entries are 0 (self-similarity is not calculated).
/// Only computes each pair once for efficiency since similarity is symmetric.
///
/// Errors if there are less than 2 vectors.
fn similarity_matrix(
    volatile_storage: &VectorStorageEnum,
    vectors: Vec<VectorInternal>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<Vec<ScoreType>>> {
    let num_vectors = vectors.len();

    // if we have less than 2 points, we can't build a matrix
    debug_assert!(
        num_vectors >= 2,
        "There should be at least two vectors to calculate similarity matrix"
    );
    if num_vectors < 2 {
        return Err(CollectionError::service_error(
            "There should be at least two vectors to calculate similarity matrix",
        ));
    }

    // Initialize similarity matrix with zeros
    let mut similarity_matrix = vec![vec![0.0; num_vectors]; num_vectors];

    // Prepare all scorers
    let raw_scorers = vectors
        .into_iter()
        .map(|vector| {
            let query = QueryVector::Nearest(vector);
            new_raw_scorer(
                query,
                volatile_storage,
                hw_measurement_acc.get_counter_cell(),
            )
        })
        .collect::<OperationResult<Vec<_>>>()?;

    // Compute similarities only for upper triangle to optimize (i < j)
    // Since similarity is symmetric: sim(i,j) == sim(j,i), we can avoid duplicate computation
    for i in 0..num_vectors {
        // Only compute scores for the upper triangle
        let upper_offsets: Vec<u32> = ((i + 1)..num_vectors).map(|j| j as u32).collect();

        if !upper_offsets.is_empty() {
            let mut scores = vec![0.0; upper_offsets.len()];
            raw_scorers[i].score_points(&upper_offsets, &mut scores);

            for (&vector_idx, similarity) in upper_offsets.iter().zip(scores) {
                let j = vector_idx as usize;
                // Set both (i,j) and (j,i) since similarity is symmetric
                similarity_matrix[i][j] = similarity;
                similarity_matrix[j][i] = similarity;
            }
        }
        // Diagonal elements remain 0.0 (self-similarity excluded)
    }

    Ok(similarity_matrix)
}

/// Maximal Marginal Relevance (MMR) algorithm
///
/// Iteratively selects points by considering their similarity to
/// already selected points, combining diversity and relevance.
///
/// # Arguments
///
/// * `candidates` - the list of points to select from
/// * `query_similarities` - similarities to the query for each candidate. Offsets refer to the index of the candidate in the `candidates` vector.
/// * `similarity_matrix` - full pairwise similarity matrix between candidates
/// * `lambda` - the lambda parameter for the MMR algorithm (0.0 = max diversity, 1.0 = max relevance)
/// * `limit` - the maximum number of points to select
pub fn maximum_marginal_relevance(
    candidates: Vec<ScoredPoint>,
    query_similarities: Vec<ScoreType>,
    similarity_matrix: &[Vec<ScoreType>],
    lambda: f32,
    limit: usize,
) -> Vec<ScoredPoint> {
    let num_candidates = candidates.len();
    if num_candidates == 0 || limit == 0 {
        return Vec::new();
    }

    let mut selected_indices = Vec::with_capacity(limit);
    let mut remaining_indices: IndexSet<usize, ahash::RandomState> = (0..num_candidates).collect();

    // Select first point with highest relevance score
    if let Some(best_idx) = remaining_indices
        .iter()
        .max_by_key(|&candidate_idx| OrderedFloat(query_similarities[*candidate_idx]))
        .copied()
    {
        selected_indices.push(best_idx);
        remaining_indices.swap_remove(&best_idx);
    }

    // Iteratively select remaining points using MMR
    while selected_indices.len() < limit && !remaining_indices.is_empty() {
        let best_candidate = remaining_indices
            .iter()
            .map(|&candidate_idx| {
                let relevance_score = query_similarities[candidate_idx];

                debug_assert!(
                    selected_indices
                        .iter()
                        .all(|&selected_idx| selected_idx != candidate_idx)
                );

                // Find maximum similarity to any already selected point
                let max_similarity_to_selected = selected_indices
                    .iter()
                    .map(|selected_idx| similarity_matrix[candidate_idx][*selected_idx])
                    .max_by_key(|&sim| OrderedFloat(sim))
                    .unwrap_or(0.0);

                // Calculate MMR score: λ * relevance - (1 - λ) * max_similarity_to_selected
                let mmr_score =
                    lambda * relevance_score - (1.0 - lambda) * max_similarity_to_selected;

                (candidate_idx, mmr_score)
            })
            .max_by_key(|(_candidate_idx, mmr_score)| OrderedFloat(*mmr_score));

        if let Some((selected_idx, _mmr_score)) = best_candidate {
            // Select the best candidate and remove from remaining
            remaining_indices.swap_remove(&selected_idx);
            selected_indices.push(selected_idx);
        } else {
            break;
        }
    }

    // Convert selected indices to ScoredPoint results
    selected_indices
        .into_iter()
        .map(|idx| {
            let mut selected = candidates[idx].clone();
            // Use query similarity as score, without post-processing.
            //
            // We prefer this over MMR score because:
            // - We already selected the top candidates based on MMR score.
            // - If this is performed at collection level, we will pass this score to the user, which is arguably more meaningful.
            // - If this is performed at local shard, it might be combined with other shards' results.
            //    - MMR does not make sense to compare by score with a different set of MMR results
            //    - It makes more sense to compare by query score.
            //    - If this isn't the last rescore before sending to collection,
            //        we are only interested in the selection of points, not the score itself.
            selected.score = query_similarities[idx];
            selected
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use common::counter::hardware_accumulator::HwMeasurementAcc;
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
            score: 0.0,
            payload: None,
            vector: Some(VectorStructInternal::Named(vectors)),
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a ScoredPoint without any vector attached.
    fn create_scored_point_without_vector(id: PointIdType) -> ScoredPoint {
        ScoredPoint {
            id,
            version: 0,
            score: 0.0,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a ScoredPoint with a sparse vector attached.
    fn create_scored_point_with_sparse_vector(
        id: PointIdType,
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
            score: 0.0,
            payload: None,
            vector: Some(VectorStructInternal::Named(vectors)),
            shard_key: None,
            order_value: None,
        }
    }

    /// Create a ScoredPoint with a multi-dense vector attached.
    fn create_scored_point_with_multi_vector(
        id: PointIdType,
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
            score: 0.0,
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
    #[case::balanced(0.5, &[1, 3, 2])]
    #[case::more_diversity(0.01, &[1, 5, 4])]
    async fn test_mmr_lambda(#[case] lambda: f32, #[case] expected_order: &[u64]) {
        let collection_params = create_test_collection_params(Distance::Euclid, 2);
        let handle = Handle::current();
        let hw_acc = HwMeasurementAcc::new();

        let points = vec![
            create_scored_point_with_vector(1.into(), vec![1.0, 0.05], None),
            create_scored_point_with_vector(2.into(), vec![0.95, 0.15], None),
            create_scored_point_with_vector(3.into(), vec![0.8, 0.0], None),
            create_scored_point_with_vector(4.into(), vec![0.85, 0.25], None),
            create_scored_point_with_vector(5.into(), vec![1.0, 0.5], None),
        ];

        let mmr = MmrInternal {
            vector: vec![1.0, 0.0].into(),
            using: VectorNameBuf::from(""),
            lambda,
            candidate_limit: 100,
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
            .iter()
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
            vector: vec![1.0, 0.0].into(),
            using: VectorNameBuf::from(""),
            lambda: 0.5,
            candidate_limit: 100,
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
            create_scored_point_with_vector(1.into(), vec![1.0, 0.0, 0.0], Some("custom")),
            create_scored_point_without_vector(2.into()), // No vector
            create_scored_point_with_vector(3.into(), vec![0.0, 1.0, 0.0], Some("other")), // Wrong vector name
            create_scored_point_with_vector(4.into(), vec![0.0, 0.0, 1.0], Some("custom")),
        ];

        let mmr = MmrInternal {
            vector: vec![1.0, 0.0, 0.0].into(),
            using: VectorNameBuf::from("custom"),
            lambda: 0.5,
            candidate_limit: 100,
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
            create_scored_point_with_vector(1.into(), vec![1.0, 0.0, 0.0], None),
            create_scored_point_with_vector(1.into(), vec![0.5, 0.5, 0.0], None), // Duplicate ID
            create_scored_point_with_vector(2.into(), vec![0.0, 1.0, 0.0], None),
        ];

        let mmr = MmrInternal {
            vector: vec![1.0, 0.0, 0.0].into(),
            using: VectorNameBuf::from(""),
            lambda: 0.5,
            candidate_limit: 100,
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
            create_scored_point_with_vector(1.into(), vec![1.0, 0.0, 0.0], None),
            create_scored_point_with_vector(2.into(), vec![0.0, 1.0, 0.0], None),
            create_scored_point_with_vector(3.into(), vec![0.0, 0.0, 1.0], None),
        ];

        let mmr = MmrInternal {
            vector: vec![1.0, 0.0, 0.0].into(),
            using: VectorNameBuf::from(""),
            lambda: 0.5,
            candidate_limit: 100,
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
                "Dense vectors failed for distance metric: {distance:?}"
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
                vec![0, 2, 5],
                vec![1.0, 0.5, 0.3],
                Some(sparse_vector_name),
            ),
            create_scored_point_with_sparse_vector(
                5.into(),
                vec![1, 3, 4],
                vec![0.8, 0.6, 0.4],
                Some(sparse_vector_name),
            ),
            create_scored_point_with_sparse_vector(
                6.into(),
                vec![0, 1, 6],
                vec![0.7, 0.9, 0.2],
                Some(sparse_vector_name),
            ),
        ];

        let sparse_collection_params =
            create_test_collection_params_named(Distance::Dot, 10, sparse_vector_name);
        let sparse_mmr = MmrInternal {
            vector: SparseVector::new(vec![0, 2, 5], vec![1.0, 0.5, 0.3])
                .unwrap()
                .into(),
            using: VectorNameBuf::from(sparse_vector_name),
            lambda: 0.5,
            candidate_limit: 100,
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
                vec![vec![1.0, 0.0], vec![0.0, 1.0]],
                Some(multi_vector_name),
            ),
            create_scored_point_with_multi_vector(
                8.into(),
                vec![vec![0.0, 1.0], vec![1.0, 0.0]],
                Some(multi_vector_name),
            ),
            create_scored_point_with_multi_vector(
                9.into(),
                vec![vec![1.0, 1.0], vec![0.0, 0.0]],
                Some(multi_vector_name),
            ),
        ];

        let multi_mmr = MmrInternal {
            vector: MultiDenseVectorInternal::new(vec![1.0, 0.0, 0.0, 1.0], 2).into(),
            using: VectorNameBuf::from(multi_vector_name),
            lambda: 0.5,
            candidate_limit: 100,
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
                "Multi-vectors failed for distance metric: {distance:?}"
            );
            let multi_scored_points = multi_result.unwrap();
            assert_eq!(multi_scored_points.len(), 3);
        }
    }
}
