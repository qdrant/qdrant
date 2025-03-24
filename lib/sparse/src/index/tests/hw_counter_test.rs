use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::PointOffsetType;
use itertools::Itertools;

use crate::common::sparse_vector::RemappedSparseVector;
use crate::index::inverted_index::InvertedIndex;
use crate::index::search_context::SearchContext;
use crate::index::tests::common::{build_index, get_pooled_scores, match_all};

fn do_search<I: InvertedIndex>(index: &I, query: RemappedSparseVector) -> HwMeasurementAcc {
    let is_stopped = AtomicBool::new(false);
    let accumulator = HwMeasurementAcc::new();
    let hardware_counter = accumulator.get_counter_cell();
    let top = 10;
    let mut search_context = SearchContext::new(
        query,
        top,
        index,
        get_pooled_scores(),
        &is_stopped,
        &hardware_counter,
    );

    let result = search_context.search(&match_all);

    assert_eq!(result.len(), top);

    accumulator
}

fn do_plain_search<I: InvertedIndex>(
    index: &I,
    query: RemappedSparseVector,
    docs: &[PointOffsetType],
) -> HwMeasurementAcc {
    let is_stopped = AtomicBool::new(false);
    let accumulator = HwMeasurementAcc::new();
    let hardware_counter = accumulator.get_counter_cell();
    let top = 10;
    let mut search_context = SearchContext::new(
        query,
        top,
        index,
        get_pooled_scores(),
        &is_stopped,
        &hardware_counter,
    );

    let result = search_context.plain_search(docs);

    assert_eq!(result.len(), top);

    accumulator
}

#[test]
fn test_hw_counter_for_sparse_search() {
    let count = 1000;
    let density = 32;
    let vocab1 = 32;
    let vocab2 = 512;

    // Expected posting length = count * density / vocab1 / 2 = 1000 * 32 / 32 / 2 = 500
    // Expected posting length = count * density / vocab2 / 2 = 1000 * 32 / 512 / 2 = 62.5

    let index_f32 = build_index::<f32>(count, density, vocab1, vocab2);
    let index_f16 = build_index::<half::f16>(count, density, vocab1, vocab2);
    let index_u8 = build_index::<u8>(count, density, vocab1, vocab2);

    let freq_query = RemappedSparseVector {
        indices: vec![1],
        values: vec![1.0],
    };

    let freq_query2 = RemappedSparseVector {
        indices: vec![0, 1, 2],
        values: vec![1.0, 1.0, 1.0],
    };

    let infrequent_query = RemappedSparseVector {
        indices: vec![100],
        values: vec![1.0],
    };

    let infrequent_query2 = RemappedSparseVector {
        indices: vec![0, 100, 200],
        values: vec![1.0, 1.0, 1.0],
    };

    let acc_f32_freq_1 = do_search(&index_f32.index, freq_query.clone());
    let acc_f16_freq_1 = do_search(&index_f16.index, freq_query.clone());
    let acc_u8_freq_1 = do_search(&index_u8.index, freq_query.clone());

    let acc_f32_freq_3 = do_search(&index_f32.index, freq_query2.clone());
    let acc_f16_freq_3 = do_search(&index_f16.index, freq_query2.clone());
    let acc_u8_freq_3 = do_search(&index_u8.index, freq_query2.clone());

    let acc_f32_infreq_1 = do_search(&index_f32.index, infrequent_query.clone());
    let acc_f16_infreq_1 = do_search(&index_f16.index, infrequent_query.clone());
    let acc_u8_infreq_1 = do_search(&index_u8.index, infrequent_query.clone());

    let acc_f32_infreq_3 = do_search(&index_f32.index, infrequent_query2.clone());
    let acc_f16_infreq_3 = do_search(&index_f16.index, infrequent_query2.clone());
    let acc_u8_infreq_3 = do_search(&index_u8.index, infrequent_query2.clone());

    // Higher precision floats cost more CPU and IO
    assert!(acc_f32_freq_1.get_cpu() > acc_f16_freq_1.get_cpu());
    assert!(acc_f16_freq_1.get_cpu() > acc_u8_freq_1.get_cpu());

    assert!(acc_f32_freq_1.get_vector_io_read() > acc_f16_freq_1.get_vector_io_read());
    assert!(acc_f16_freq_1.get_vector_io_read() > acc_u8_freq_1.get_vector_io_read());

    // More indices are more expensive than less indices
    assert!(acc_f32_freq_3.get_cpu() > acc_f32_freq_1.get_cpu());
    assert!(acc_f16_freq_3.get_cpu() > acc_f16_freq_1.get_cpu());
    assert!(acc_u8_freq_3.get_cpu() > acc_u8_freq_1.get_cpu());

    assert!(acc_f32_freq_3.get_vector_io_read() > acc_f32_freq_1.get_vector_io_read());
    assert!(acc_f16_freq_3.get_vector_io_read() > acc_f16_freq_1.get_vector_io_read());
    assert!(acc_u8_freq_3.get_vector_io_read() > acc_u8_freq_1.get_vector_io_read());

    // Frequent terms are more expensive than infrequent terms

    assert!(acc_f32_freq_1.get_cpu() > acc_f32_infreq_1.get_cpu());
    assert!(acc_f16_freq_1.get_cpu() > acc_f16_infreq_1.get_cpu());
    assert!(acc_u8_freq_1.get_cpu() > acc_u8_infreq_1.get_cpu());

    assert!(acc_f32_freq_1.get_vector_io_read() > acc_f32_infreq_1.get_vector_io_read());
    assert!(acc_f16_freq_1.get_vector_io_read() > acc_f16_infreq_1.get_vector_io_read());
    assert!(acc_u8_freq_1.get_vector_io_read() > acc_u8_infreq_1.get_vector_io_read());

    // More indices are more expensive than less indices

    assert!(acc_f32_infreq_3.get_cpu() > acc_f32_infreq_1.get_cpu());
    assert!(acc_f16_infreq_3.get_cpu() > acc_f16_infreq_1.get_cpu());
    assert!(acc_u8_infreq_3.get_cpu() > acc_u8_infreq_1.get_cpu());

    assert!(acc_f32_infreq_3.get_vector_io_read() > acc_f32_infreq_1.get_vector_io_read());
    assert!(acc_f16_infreq_3.get_vector_io_read() > acc_f16_infreq_1.get_vector_io_read());
    assert!(acc_u8_infreq_3.get_vector_io_read() > acc_u8_infreq_1.get_vector_io_read());
}

#[test]
fn test_hw_counter_for_plain_sparse_search() {
    let count = 1000;
    let density = 32;
    let vocab1 = 32;
    let vocab2 = 512;

    // Expected posting length = count * density / vocab1 / 2 = 1000 * 32 / 32 / 2 = 500
    // Expected posting length = count * density / vocab2 / 2 = 1000 * 32 / 512 / 2 = 62.5

    let index_f32 = build_index::<f32>(count, density, vocab1, vocab2);
    let index_f16 = build_index::<half::f16>(count, density, vocab1, vocab2);
    let index_u8 = build_index::<u8>(count, density, vocab1, vocab2);

    let documents_to_score = 500;

    // Plain search requires list of document ids
    // Generate random document ids
    let document_ids: Vec<PointOffsetType> = (0..documents_to_score)
        .map(|_| rand::random::<PointOffsetType>() % count as PointOffsetType)
        .unique()
        .collect();

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

    let acc_f32_freq_1 = do_plain_search(&index_f32.index, freq_query.clone(), &document_ids);
    let acc_f16_freq_1 = do_plain_search(&index_f16.index, freq_query.clone(), &document_ids);
    let acc_u8_freq_1 = do_plain_search(&index_u8.index, freq_query.clone(), &document_ids);

    let acc_f32_freq_3 = do_plain_search(&index_f32.index, freq_query2.clone(), &document_ids);
    let acc_f16_freq_3 = do_plain_search(&index_f16.index, freq_query2.clone(), &document_ids);
    let acc_u8_freq_3 = do_plain_search(&index_u8.index, freq_query2.clone(), &document_ids);

    let acc_f32_infreq_1 = do_plain_search(&index_f32.index, infreq_query.clone(), &document_ids);
    let acc_f16_infreq_1 = do_plain_search(&index_f16.index, infreq_query.clone(), &document_ids);
    let acc_u8_infreq_1 = do_plain_search(&index_u8.index, infreq_query.clone(), &document_ids);

    let acc_f32_infreq_3 = do_plain_search(&index_f32.index, infreq_query2.clone(), &document_ids);
    let acc_f16_infreq_3 = do_plain_search(&index_f16.index, infreq_query2.clone(), &document_ids);
    let acc_u8_infreq_3 = do_plain_search(&index_u8.index, infreq_query2.clone(), &document_ids);

    // Higher precision floats cost more IO, but might have same CPU
    assert!(acc_f32_freq_1.get_cpu() >= acc_f16_freq_1.get_cpu());
    assert!(acc_f16_freq_1.get_cpu() >= acc_u8_freq_1.get_cpu());

    assert!(acc_f32_freq_1.get_vector_io_read() > acc_f16_freq_1.get_vector_io_read());
    assert!(acc_f16_freq_1.get_vector_io_read() > acc_u8_freq_1.get_vector_io_read());

    // More indices are more expensive than less indices
    assert!(acc_f32_freq_3.get_cpu() > acc_f32_freq_1.get_cpu());
    assert!(acc_f16_freq_3.get_cpu() > acc_f16_freq_1.get_cpu());
    assert!(acc_u8_freq_3.get_cpu() > acc_u8_freq_1.get_cpu());

    assert!(acc_f32_freq_3.get_vector_io_read() > acc_f32_freq_1.get_vector_io_read());
    assert!(acc_f16_freq_3.get_vector_io_read() > acc_f16_freq_1.get_vector_io_read());
    assert!(acc_u8_freq_3.get_vector_io_read() > acc_u8_freq_1.get_vector_io_read());

    // Frequent terms are more expensive than infrequent terms

    assert!(acc_f32_freq_1.get_cpu() > acc_f32_infreq_1.get_cpu());
    assert!(acc_f16_freq_1.get_cpu() > acc_f16_infreq_1.get_cpu());
    assert!(acc_u8_freq_1.get_cpu() > acc_u8_infreq_1.get_cpu());

    assert!(acc_f32_freq_1.get_vector_io_read() > acc_f32_infreq_1.get_vector_io_read());
    assert!(acc_f16_freq_1.get_vector_io_read() > acc_f16_infreq_1.get_vector_io_read());
    assert!(acc_u8_freq_1.get_vector_io_read() > acc_u8_infreq_1.get_vector_io_read());

    // More indices are more expensive than less indices

    assert!(acc_f32_infreq_3.get_cpu() > acc_f32_infreq_1.get_cpu());
    assert!(acc_f16_infreq_3.get_cpu() > acc_f16_infreq_1.get_cpu());
    assert!(acc_u8_infreq_3.get_cpu() > acc_u8_infreq_1.get_cpu());

    assert!(acc_f32_infreq_3.get_vector_io_read() > acc_f32_infreq_1.get_vector_io_read());
    assert!(acc_f16_infreq_3.get_vector_io_read() > acc_f16_infreq_1.get_vector_io_read());
    assert!(acc_u8_infreq_3.get_vector_io_read() > acc_u8_infreq_1.get_vector_io_read());
}
