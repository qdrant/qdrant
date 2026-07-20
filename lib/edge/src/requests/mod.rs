//! Edge-specific request types for [`EdgeShardRead`](crate::EdgeShardRead), one file per request.
//!
//! Every request is an edge-owned struct with public fields: a `new` constructor takes the
//! required parameters and defaults the rest, so new optional parameters can be added without
//! breaking callers. Fluent builders live in [`crate::builders`]. Conversions into the internal
//! request types executed by the read path are grouped per request in [`conversions`], with
//! exhaustive destructuring on both sides, so a field added to an internal request type fails
//! compilation there instead of being silently dropped.

pub mod conversions;
pub mod count;
pub mod facet;
pub mod grouping;
pub mod matrix;
pub mod query;
pub mod retrieve;
pub mod scroll;
pub mod search;

pub use count::CountRequest;
pub use facet::FacetRequest;
pub use grouping::GroupRequest;
pub use matrix::SearchMatrixRequest;
pub use query::{Prefetch, QueryRequest};
pub use retrieve::RetrieveRequest;
pub use scroll::ScrollRequest;
pub use search::SearchRequest;
