//! Fluent builders for user-facing edge crate configuration.

pub mod edge_config;
pub mod edge_sparse_vector_params;
pub mod edge_vector_params;

pub use edge_config::EdgeConfigBuilder;
pub use edge_sparse_vector_params::EdgeSparseVectorParamsBuilder;
pub use edge_vector_params::EdgeVectorParamsBuilder;
