mod similarity;

#[cfg(test)]
mod tests;

use common::types::{PointOffsetType, ScoreType};
use indexmap::IndexSet;
use itertools::Itertools as _;
use ordered_float::OrderedFloat;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::{QueryVector, VectorInternal, VectorRef};
use segment::types::{Distance, MultiVectorConfig, ScoredPoint};
use segment::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use segment::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
use segment::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;
use segment::vector_storage::{
    VectorStorage as _, VectorStorageEnum, VectorStorageRead as _, new_raw_scorer,
};

use self::similarity::SimilarityToSelected;
use super::MmrInternal;

/// Calculate the MMR (Maximal Marginal Relevance) score for a set of points with vectors.
///
/// Assumes the points have vectors attached. If not, the entire point will be discarded.
///
/// # Arguments
///
/// * `points_with_vector` - The points with vectors.
/// * `mmr` - The MMR parameters.
/// * `distance` - The distance metric of the collection.
/// * `multivector_config` - The multivector configuration of the collection, if any.
/// * `limit` - The maximum number of points to return.
///
/// # Returns
///
/// A vector of scored points.
pub fn mmr_from_points_with_vector(
    points_with_vector: impl IntoIterator<Item = ScoredPoint>,
    mmr: MmrInternal,
    distance: Distance,
    multivector_config: Option<MultiVectorConfig>,
    limit: usize,
) -> OperationResult<Vec<ScoredPoint>> {
    let (vectors, candidates): (Vec<_>, Vec<_>) = points_with_vector
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

    let volatile_storage = create_volatile_storage(&vectors, distance, multivector_config)?;

    if candidates.len() < 2 {
        // can't compute MMR for less than 2 points, return with original score
        return Ok(candidates);
    }

    // get similarities against query
    let query_similarities = relevance_similarities(&volatile_storage, mmr.vector)?;

    // similarities between candidates
    let similarity = similarity_to_selected(&volatile_storage, vectors)?;

    // compute MMR
    maximal_marginal_relevance(
        candidates,
        query_similarities,
        similarity,
        mmr.lambda.0,
        limit,
    )
}

/// Creates a volatile (in-memory and not persistent) vector storage and inserts the vectors in the provided order.
fn create_volatile_storage(
    vectors: &[VectorInternal],
    distance: Distance,
    multivector_config: Option<MultiVectorConfig>,
) -> OperationResult<VectorStorageEnum> {
    // Create temporary vector storage
    let mut volatile_storage = {
        match &vectors[0] {
            VectorInternal::Dense(vector) => {
                new_volatile_dense_vector_storage(vector.len(), distance)
            }

            VectorInternal::MultiDense(typed_multi_dense_vector) => {
                let multivector_config = multivector_config.ok_or_else(|| {
                    OperationError::service_error(
                        "multivectors are present, but no multivector config provided",
                    )
                })?;

                new_volatile_multi_dense_vector_storage(
                    typed_multi_dense_vector.dim,
                    distance,
                    multivector_config,
                )
            }

            VectorInternal::Sparse(_) => new_volatile_sparse_vector_storage(),
        }
    };

    // Populate storage with vectors
    for (key, vector) in (0..).zip(vectors) {
        volatile_storage.insert_vector(key, VectorRef::from(vector))?;
    }

    Ok(volatile_storage)
}

/// Compute the "relevance" similarity between a query vector and all vectors in the storage.
fn relevance_similarities(
    volatile_storage: &VectorStorageEnum,
    query_vector: VectorInternal,
) -> OperationResult<Vec<ScoreType>> {
    let query = QueryVector::Nearest(query_vector);
    let query_scorer = new_raw_scorer(query, volatile_storage)?;

    // get similarity between candidates and query
    let ids: Vec<_> = (0..volatile_storage.total_vector_count() as u32).collect();
    let mut similarities = vec![0.0; ids.len()];
    query_scorer.score_points(&ids, &mut similarities)?;

    Ok(similarities)
}

/// Scores candidates against a selected candidate, see [`SimilarityToSelected`].
///
/// Errors if there are less than 2 vectors.
fn similarity_to_selected(
    volatile_storage: &VectorStorageEnum,
    vectors: Vec<VectorInternal>,
) -> OperationResult<SimilarityToSelected<'_>> {
    let num_vectors = vectors.len();

    // if we have less than 2 points, we can't compare them
    debug_assert!(
        num_vectors >= 2,
        "There should be at least two vectors to calculate similarities"
    );

    if num_vectors < 2 {
        return Err(OperationError::service_error(
            "There should be at least two vectors to calculate similarities",
        ));
    }

    SimilarityToSelected::new(vectors, volatile_storage)
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
/// * `similarity` - similarities between candidates
/// * `lambda` - the lambda parameter for the MMR algorithm (0.0 = max diversity, 1.0 = max relevance)
/// * `limit` - the maximum number of points to select
fn maximal_marginal_relevance(
    candidates: Vec<ScoredPoint>,
    query_similarities: Vec<ScoreType>,
    similarity: SimilarityToSelected,
    lambda: f32,
    limit: usize,
) -> OperationResult<Vec<ScoredPoint>> {
    let num_candidates = candidates.len();
    if num_candidates == 0 || limit == 0 {
        return Ok(Vec::new());
    }

    let mut selected_indices = Vec::with_capacity(limit.min(num_candidates));
    let mut remaining_indices: IndexSet<usize, ahash::RandomState> = (0..num_candidates).collect();

    // Maximum similarity of each candidate to any already selected point. Only
    // the newest selected point can raise it, so each round scores the
    // remaining candidates against that point alone.
    let mut max_similarity_to_selected = vec![ScoreType::NEG_INFINITY; num_candidates];
    let mut remaining_ids = Vec::with_capacity(num_candidates);
    let mut similarities = Vec::with_capacity(num_candidates);

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
        let newest_selected = *selected_indices
            .last()
            .expect("the first point is selected above");

        remaining_ids.clear();
        remaining_ids.extend(remaining_indices.iter().map(|&idx| idx as PointOffsetType));
        similarities.clear();
        similarities.resize(remaining_ids.len(), 0.0);
        similarity.score(
            newest_selected as PointOffsetType,
            &remaining_ids,
            &mut similarities,
        )?;

        for (&candidate_idx, &similarity) in remaining_indices.iter().zip(&similarities) {
            // `>=` keeps the last of equal maxima, as `max_by_key` does
            let max_similarity = &mut max_similarity_to_selected[candidate_idx];
            if OrderedFloat(similarity) >= OrderedFloat(*max_similarity) {
                *max_similarity = similarity;
            }
        }

        let best_candidate = remaining_indices
            .iter()
            .map(|&candidate_idx| {
                let relevance_score = query_similarities[candidate_idx];

                // Calculate MMR score: λ * relevance - (1 - λ) * max_similarity_to_selected
                let mmr_score = lambda * relevance_score
                    - (1.0 - lambda) * max_similarity_to_selected[candidate_idx];

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
    Ok(selected_indices
        .into_iter()
        .map(|idx| {
            // Use original score, already postprocessed.
            //
            // We prefer this over MMR score because:
            // - We already selected the top candidates based on MMR score.
            // - If this is performed at collection level, we will pass this score to the user, which is arguably more meaningful.
            // - If this is performed at local shard, it might be combined with other shards' results.
            //    - MMR does not make sense to compare by score with a different set of MMR results
            //    - It makes more sense to compare by query score.
            //    - If this isn't the last rescore before sending to collection,
            //        we are only interested in the selection of points, not the score itself.
            candidates[idx].clone()
        })
        .collect())
}
