//! Fluent builders for user-facing edge crate configuration and requests.

pub mod count_request;
pub mod edge_config;
pub mod edge_sparse_vector_params;
pub mod edge_vector_params;
pub mod facet_request;
pub mod group_request;
pub mod prefetch;
pub mod query_request;
pub mod retrieve_request;
pub mod scroll_request;
pub mod search_matrix_request;
pub mod search_request;

pub use count_request::CountRequestBuilder;
pub use edge_config::EdgeConfigBuilder;
pub use edge_sparse_vector_params::EdgeSparseVectorParamsBuilder;
pub use edge_vector_params::EdgeVectorParamsBuilder;
pub use facet_request::FacetRequestBuilder;
pub use group_request::GroupRequestBuilder;
pub use prefetch::PrefetchBuilder;
pub use query_request::QueryRequestBuilder;
pub use retrieve_request::RetrieveRequestBuilder;
pub use scroll_request::ScrollRequestBuilder;
pub use search_matrix_request::SearchMatrixRequestBuilder;
pub use search_request::SearchRequestBuilder;
