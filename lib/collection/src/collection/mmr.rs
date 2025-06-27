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
