//! Per-operation read surface of the FFI [`EdgeShard`](crate::EdgeShard):
//! one file per operation, mirroring `edge`'s `read_view/ops` layout.
//!
//! Each file owns the operation's boundary request/response records, their
//! fallible conversions into the `edge` request types, and the exported
//! `EdgeShard` method that runs the operation.

pub mod count;
pub mod facet;
pub mod formula;
pub mod grouping;
pub mod info;
/// Gated behind the `matrix` feature, which is **on by default**. The mobile
/// (Swift/Kotlin) bindgen must build `--no-default-features` to keep this op off
/// the mobile binding surface. See the module docs.
#[cfg(feature = "matrix")]
pub mod matrix;
pub mod query;
pub mod retrieve;
pub mod scroll;
pub mod search;

pub use self::count::CountRequest;
pub use self::facet::{FacetHit, FacetRequest, FacetResponse};
pub use self::formula::{DecayKind, Expression};
pub use self::grouping::{Group, GroupId, GroupRequest};
pub use self::info::ShardInfo;
#[cfg(feature = "matrix")]
pub use self::matrix::{SearchMatrixRequest, SearchMatrixResponse};
pub use self::query::{
    ContextPair, Direction, FeedbackCoefficients, FeedbackItem, Fusion, OrderBy, Prefetch, Query,
    QueryRequest, RecommendStrategy, Sample, ScoringQuery, SearchParams, StartFrom,
};
pub use self::retrieve::RetrieveRequest;
pub use self::scroll::{ScrollRequest, ScrollResponse};
pub use self::search::SearchRequest;
