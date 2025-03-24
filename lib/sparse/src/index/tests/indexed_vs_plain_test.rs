use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;

use crate::common::sparse_vector::RemappedSparseVector;
use crate::index::inverted_index::InvertedIndex;
use crate::index::search_context::SearchContext;
use crate::index::tests::common::{build_index, get_pooled_scores, match_all};

fn query<I: InvertedIndex>(index: &I, query: RemappedSparseVector) {
    let is_stopped = AtomicBool::new(false);
    let accumulator = HwMeasurementAcc::new();
    let hardware_counter = accumulator.get_counter_cell();
    let top = 10;
    let mut search_context = SearchContext::new(
        query.clone(),
        top,
        index,
        get_pooled_scores(),
        &is_stopped,
        &hardware_counter,
    );

    let result = search_context.search(&match_all);
    let docs: Vec<_> = result.iter().map(|x| x.idx).collect();

    let mut search_context = SearchContext::new(
        query,
        top,
        index,
        get_pooled_scores(),
        &is_stopped,
        &hardware_counter,
    );
    let plain_result = search_context.plain_search(&docs);

    assert_eq!(result, plain_result);
}

/// Run regular search and then palin search and compare the results
#[test]
fn test_search_vs_plain() {
    let count = 1000;
    let density = 32;
    let vocab1 = 32;
    let vocab2 = 512;

    // Expected posting length = count * density / vocab1 / 2 = 1000 * 32 / 32 / 2 = 500
    // Expected posting length = count * density / vocab2 / 2 = 1000 * 32 / 512 / 2 = 62.5

    let index = build_index::<f32>(count, density, vocab1, vocab2);

    let freq_query = RemappedSparseVector {
        indices: vec![1],
        values: vec![1.0],
    };

    let freq_query2 = RemappedSparseVector {
        indices: vec![0, 1, 2],
        values: vec![1.0, 1.0, 1.0],
    };

    let infreq_query = RemappedSparseVector {
        indices: vec![100],
        values: vec![1.0],
    };

    let infreq_query2 = RemappedSparseVector {
        indices: vec![101, 102, 103],
        values: vec![1.0, 1.0, 1.0],
    };

    query(&index.index, freq_query);
    query(&index.index, freq_query2);
    query(&index.index, infreq_query);
    query(&index.index, infreq_query2);
}
