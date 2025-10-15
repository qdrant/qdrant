//! ## Universal query request types
//!
//! Provides a common interface for querying points in a collection
//!
//! Pipeline of type conversion:
//!
//! 1. [`api::rest::QueryRequest`], [`api::grpc::qdrant::QueryPoints`]:
//!    Rest or grpc request. Used in API.
//! 2. [`collection_query::CollectionQueryRequest`]:
//!    Direct representation of the API request, but to be used as a single type. Created at API to enter ToC.
//! 3. [`ShardQueryRequest`]:
//!    Same as the common request, but all point ids have been substituted with vectors. Created at Collection.
//! 4. [`api::grpc::qdrant::QueryShardPoints`]:
//!    to be used in the internal service. Created for [`RemoteShard`], converts to and from [`ShardQueryRequest`].
//! 5. [`planned_query::PlannedQuery`]:
//!    An easier-to-execute representation of a batch of [`ShardQueryRequest`]. Created in [`LocalShard`].
//!
//! [`LocalShard`]: crate::shards::local_shard::LocalShard
//! [`RemoteShard`]: crate::shards::remote_shard::RemoteShard
//! [`ShardQueryRequest`]: shard_query::ShardQueryRequest
//! [`QueryShardPoints`]: api::grpc::qdrant::QueryShardPoints

pub mod collection_query;
pub mod planned_query;
pub mod shard_query;

pub mod formula {
    pub use shard::query::formula::*;
}
