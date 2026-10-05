use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::ScoredPointOffset;
use itertools::Itertools;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{QueryVector, VectorInternal};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{
    Distance, QuantizationConfig, SearchParams, default_quantization_ignore_value,
    default_quantization_oversampling_value,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsRead;
use crate::vector_storage::{RawScorerBuilder, VectorStorageRead};

pub fn is_quantized_search<Q: QuantizedVectorsRead>(
    quantized_storage: Option<&Q>,
    params: Option<&SearchParams>,
) -> bool {
    let ignore_quantization = params
        .and_then(|p| p.quantization)
        .map(|q| q.ignore)
        .unwrap_or(default_quantization_ignore_value());
    let exact = params.is_some_and(|p| p.exact);
    quantized_storage.is_some() && !ignore_quantization && !exact
}

/// Returns whether the scores use Binary Quantization's XOR-based representation.
fn is_binary_quantization(config: &QuantizationConfig) -> bool {
    matches!(config, QuantizationConfig::Binary(_))
}

pub fn get_oversampled_top<Q: QuantizedVectorsRead>(
    quantized_storage: Option<&Q>,
    params: Option<&SearchParams>,
    top: usize,
) -> usize {
    let quantization_enabled = is_quantized_search(quantized_storage, params);

    let oversampling_value = params
        .and_then(|p| p.quantization)
        .map(|q| q.oversampling)
        .unwrap_or(default_quantization_oversampling_value());

    match oversampling_value {
        Some(oversampling) if quantization_enabled && oversampling > 1.0 => {
            (oversampling * top as f64) as usize
        }
        _ => top,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn postprocess_search_result<V, Q>(
    mut search_result: Vec<ScoredPointOffset>,
    point_deleted: &BitSlice,
    vector_storage: &V,
    quantized_vectors: Option<&Q>,
    vector: &QueryVector,
    params: Option<&SearchParams>,
    top: usize,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Vec<ScoredPointOffset>>
where
    V: VectorStorageRead + RawScorerBuilder,
    Q: QuantizedVectorsRead,
{
    let quantization_enabled = is_quantized_search(quantized_vectors, params);

    let default_rescoring = quantized_vectors
        .as_ref()
        .map(|q| q.default_rescoring())
        .unwrap_or(false);
    let rescore = quantization_enabled
        && params
            .and_then(|p| p.quantization)
            .and_then(|q| q.rescore)
            .unwrap_or(default_rescoring);
    if rescore {
        let mut scorer = FilteredScorer::new(
            vector.to_owned(),
            vector_storage,
            None::<&Q>,
            None,
            point_deleted,
            hardware_counter,
        )?;

        search_result = scorer
            .score_points(&mut search_result.iter().map(|x| x.idx).collect_vec(), 0)
            .collect();
        search_result.sort_unstable();
        search_result.reverse();
    } else if quantization_enabled
        && vector_storage.distance() == Distance::Euclid
        && matches!(vector, QueryVector::Nearest(VectorInternal::Dense(_)))
        && let Some(quantized_vectors) = quantized_vectors
        && is_binary_quantization(&quantized_vectors.config().quantization_config)
    {
        // BQ returns dim - 2 * h, where h is the (possibly weighted) XOR count.
        // Convert to a non-positive quantized distance proxy (-4 * h) before
        // segment aggregation and Euclidean sqrt(abs(score)) postprocessing.
        // This preserves the BQ ranking, but does not recover the original
        // Euclidean distance. For symmetric one-bit encoding, -4 * h is the negative
        // squared distance between sign vectors; other encodings retain only the
        // proxy interpretation.
        // Multi-vector queries need an offset for each query sub-vector.
        let dim = quantized_vectors.config().vector_parameters.dim as f32;
        for point in &mut search_result {
            point.score = 2.0 * (point.score - dim);
        }
    }
    search_result.truncate(top);
    Ok(search_result)
}
