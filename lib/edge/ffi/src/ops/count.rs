//! [`EdgeShard::count`] — counting points matching a filter.

use segment::types::Filter as SegmentFilter;

use crate::EdgeShard;
use crate::error::Result;
use crate::filter::Filter;

#[uniffi::export]
impl EdgeShard {
    /// Counts the points matching the request filter.
    ///
    /// When `request.exact` is `true`, every matching point is visited; when
    /// `false`, an approximate count may be returned faster.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the filter
    /// references a payload key without an index.
    pub fn count(&self, request: CountRequest) -> Result<u64> {
        self.with_shard(|shard| {
            let count = shard.count(request.try_into()?)?;
            Ok(count as u64)
        })
    }
}

// ── CountRequest ────────────────────────────────────────────────────────────

/// A point-counting request.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CountRequest {
    /// Optional filter. `None`/`null` counts all points in the shard.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
    /// If `true`, return an exact count (scans every matching point);
    /// otherwise, the shard is free to return a fast estimate.
    #[uniffi(default = true)]
    pub exact: bool,
}

impl TryFrom<CountRequest> for edge::CountRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: CountRequest) -> Result<Self, Self::Error> {
        let CountRequest { filter, exact } = r;
        Ok(edge::CountRequest {
            filter: filter.map(SegmentFilter::try_from).transpose()?,
            exact,
        })
    }
}
