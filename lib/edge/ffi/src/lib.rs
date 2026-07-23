//! UniFFI boundary of Qdrant Edge: the annotated Rust surface from which the
//! Swift and Kotlin bindings are generated.
//!
//! Module layout mirrors the `edge` crate: the [`EdgeShard`] object and its
//! lifecycle live in [`shard`], per-operation read methods (with their
//! request/response records) in [`ops`] — one file per operation — and update
//! construction in [`update`]. Shared value types, filters, config,
//! payload-index parameters, and the boundary error type live in [`types`],
//! [`filter`], [`config`], [`payload_index`], and [`error`]. The crate root
//! re-exports the whole public surface.

pub mod config;
pub mod error;
pub mod filter;
pub mod ops;
pub mod payload_index;
pub mod shard;
pub mod types;
pub mod update;

uniffi::setup_scaffolding!();

pub use crate::config::OptimizersConfig;
pub use crate::ops::{
    ContextPair, CountRequest, DecayKind, Direction, Expression, FacetHit, FacetRequest,
    FacetResponse, FeedbackCoefficients, FeedbackItem, Fusion, Group, GroupId, GroupRequest,
    OrderBy, Prefetch, Query, QueryRequest, RecommendStrategy, RetrieveRequest, Sample,
    ScoringQuery, ScrollRequest, ScrollResponse, SearchParams, SearchRequest, ShardInfo, StartFrom,
};
#[cfg(feature = "matrix")]
pub use crate::ops::{SearchMatrixRequest, SearchMatrixResponse};
pub use crate::payload_index::{
    BoolIndexParams, DatetimeIndexParams, FloatIndexParams, GeoIndexParams, IntegerIndexParams,
    KeywordIndexParams, Language, PayloadIndexInfo, PayloadIndexParams, SnowballLanguage, Stemmer,
    Stopwords, TextIndexParams, TokenizerType, UuidIndexParams,
};
pub use crate::shard::{EdgeShard, unpack_snapshot};
pub use crate::update::{PayloadSchemaType, UpdateMode, UpdateOperation};
