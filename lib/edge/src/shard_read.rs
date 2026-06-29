use std::path::Path;
use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::ScoreType;
use segment::common::operation_error::OperationResult;
use segment::data_types::facets::FacetResponse;
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::{ExtendedPointId, PointIdType, ScoredPoint, WithPayloadInterface, WithVector};
use shard::count::CountRequestInternal;
use shard::facet::FacetRequestInternal;
use shard::locked_segment::LockedSegment;
use shard::query::ShardQueryRequest;
use shard::query::scroll::QueryScrollRequestInternal;
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequest;

use crate::read_view::EdgeShardRead;
use crate::{EdgeConfig, EdgeShard, ShardInfo};

/// The read-write shard's segments are heterogeneous — `Segment` and `ProxySegment` coexist during
/// optimization — so the handle is the [`LockedSegment`] enum (static enum dispatch), and the read
/// view is built over that.
impl EdgeShardRead for EdgeShard {
    type Handle = LockedSegment;

    fn read_segments(&self) -> Vec<LockedSegment> {
        self.segments
            .read()
            .non_appendable_then_appendable_segments()
            .collect()
    }

    fn config_snapshot(&self) -> Arc<EdgeConfig> {
        Arc::new(self.config.read().clone())
    }

    fn search_pool(&self) -> Arc<rayon::ThreadPool> {
        self.search_pool.clone()
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

/// Inherent read API of [`EdgeShard`], kept for backwards compatibility (`edge` is a public crate):
/// callers can use these without importing [`EdgeShardRead`]. Each just forwards to the trait's
/// shared implementation.
impl EdgeShard {
    /// This method is DEPRECATED and should be replaced with query.
    pub fn search(&self, search: CoreSearchRequest) -> OperationResult<Vec<ScoredPoint>> {
        EdgeShardRead::search(self, search)
    }

    pub fn query(&self, request: ShardQueryRequest) -> OperationResult<Vec<ScoredPoint>> {
        EdgeShardRead::query(self, request)
    }

    pub fn query_scroll(
        &self,
        request: &QueryScrollRequestInternal,
    ) -> OperationResult<Vec<ScoredPoint>> {
        EdgeShardRead::query_scroll(self, request)
    }

    pub fn scroll(
        &self,
        request: ScrollRequestInternal,
    ) -> OperationResult<(Vec<RecordInternal>, Option<PointIdType>)> {
        EdgeShardRead::scroll(self, request)
    }

    pub fn retrieve(
        &self,
        point_ids: &[ExtendedPointId],
        with_payload: Option<WithPayloadInterface>,
        with_vector: Option<WithVector>,
    ) -> OperationResult<Vec<RecordInternal>> {
        EdgeShardRead::retrieve(self, point_ids, with_payload, with_vector)
    }

    pub fn count(&self, request: CountRequestInternal) -> OperationResult<usize> {
        EdgeShardRead::count(self, request)
    }

    pub fn facet(&self, request: FacetRequestInternal) -> OperationResult<FacetResponse> {
        EdgeShardRead::facet(self, request)
    }

    pub fn info(&self) -> ShardInfo {
        EdgeShardRead::info(self)
    }

    pub fn rescore_with_formula(
        &self,
        formula: ParsedFormula,
        prefetches_results: Vec<Vec<ScoredPoint>>,
        limit: usize,
        score_threshold: Option<ScoreType>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> OperationResult<Vec<ScoredPoint>> {
        EdgeShardRead::rescore_with_formula(
            self,
            formula,
            prefetches_results,
            limit,
            score_threshold,
            hw_measurement_acc,
        )
    }
}
