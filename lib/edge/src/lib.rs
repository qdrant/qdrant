pub mod bm25_embed;
mod builders;
pub mod config;
#[cfg(feature = "serverless")]
mod delete_only;
mod edge_shard;
#[cfg(feature = "serverless")]
mod read_only;
mod read_view;
mod reexports;
mod requests;
mod types;
#[cfg(feature = "serverless")]
mod update_only;
pub use types::*;

#[cfg(test)]
mod test_helpers;

pub use builders::{
    CountRequestBuilder, EdgeConfigBuilder, EdgeSparseVectorParamsBuilder, EdgeVectorParamsBuilder,
    FacetRequestBuilder, GroupRequestBuilder, PrefetchBuilder, QueryRequestBuilder,
    RetrieveRequestBuilder, ScrollRequestBuilder, SearchMatrixRequestBuilder, SearchRequestBuilder,
};
pub use config::optimizers::EdgeOptimizersConfig;
pub use config::shard::EdgeConfig;
pub use config::vectors::{EdgeSparseVectorParams, EdgeVectorParams};
#[cfg(feature = "serverless")]
pub use delete_only::DeleteOnlyEdgeShard;
pub use edge_shard::EdgeShard;
#[cfg(feature = "serverless")]
pub use read_only::{
    ListedSegment, LiveReloadOutcome, LocalSegmentEnumerator, ManifestSegmentEnumerator,
    ReadOnlyEdgeShard, ReadOnlyEdgeShardPools, SegmentEnumerator,
};
#[cfg(feature = "serverless")]
pub use read_view::EdgeShardReadWithCancellation;
pub use read_view::{EdgeShardRead, Group, ReadSegmentHandle, SearchMatrixResponse, ShardInfo};
pub use reexports::*;
pub use requests::{
    CountRequest, FacetRequest, GroupRequest, Prefetch, QueryBatchRequest, QueryRequest,
    RetrieveRequest, ScrollRequest, SearchMatrixRequest, SearchRequest,
};
pub use shard::files::WAL_PATH;
pub use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
#[cfg(feature = "serverless")]
pub use update_only::{
    PointAction, PointApplyKind, PointApplyRecord, PointCopy, PointPreview, PointUpdates,
    SegmentConfigInfo, UpdateBatchOutcome, UpdateBatchPlan, UpdateBatchPreview,
    UpdateOnlyEdgeShard,
};
