#![cfg(test)]

pub mod batch_search_test;
pub mod disbalanced_vectors_test;
pub mod exact_search_test;
pub mod fail_recovery_test;
pub mod filtering_context_check;
pub mod filtrable_hnsw_test;
pub mod fixtures;
pub mod hnsw_discover_test;
pub mod hnsw_quantized_search_test;
mod multivector_filtrable_hnsw_test;
mod multivector_hnsw_test;
pub mod nested_filtering_test;
pub mod payload_index_test;
pub mod scroll_filtering_test;
pub mod segment_builder_test;
pub mod segment_tests;
mod sparse_discover_test;
mod sparse_vector_index_search_tests;
pub mod utils;
