use bitvec::slice::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::ScoredPointOffset;
use itertools::Itertools;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::QueryVector;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{
    SearchParams, default_quantization_ignore_value, default_quantization_oversampling_value,
};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

pub fn is_quantized_search(
    quantized_storage: Option<&QuantizedVectors>,
    params: Option<&SearchParams>,
) -> bool {
    let ignore_quantization = params
        .and_then(|p| p.quantization)
        .map(|q| q.ignore)
        .unwrap_or(default_quantization_ignore_value());
    let exact = params.map(|p| p.exact).unwrap_or(false);
    quantized_storage.is_some() && !ignore_quantization && !exact
}

pub fn get_oversampled_top(
    quantized_storage: Option<&QuantizedVectors>,
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
pub fn postprocess_search_result(
    mut search_result: Vec<ScoredPointOffset>,
    point_deleted: &BitSlice,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: Option<&QuantizedVectors>,
    vector: &QueryVector,
    params: Option<&SearchParams>,
    top: usize,
    hardware_counter: HardwareCounterCell,
) -> OperationResult<Vec<ScoredPointOffset>> {
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
            None,
            None,
            point_deleted,
            hardware_counter,
        )?;

        search_result = scorer
            .score_points(&mut search_result.iter().map(|x| x.idx).collect_vec(), 0)
            .collect();
        search_result.sort_unstable();
        search_result.reverse();
    }
    search_result.truncate(top);
    Ok(search_result)
}
