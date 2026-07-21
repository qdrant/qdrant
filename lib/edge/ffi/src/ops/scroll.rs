//! [`EdgeShard::scroll`] — batched iteration over the shard's points.

use segment::data_types::order_by::{OrderBy as SegmentOrderBy, OrderByInterface};
use segment::types::{
    Filter as SegmentFilter, PointIdType, WithPayloadInterface, WithVector as SegmentWithVector,
};

use crate::EdgeShard;
use crate::error::Result;
use crate::filter::Filter;
use crate::ops::query::OrderBy;
use crate::types::{PointId, Record, WithPayload, WithVector};

#[uniffi::export]
impl EdgeShard {
    /// Iterates over points in the shard in batches.
    ///
    /// Useful for exporting all points, paginating through a filter match,
    /// or implementing custom offline processing. Pass the returned
    /// `next_offset` back as `request.offset` to fetch the next page; when
    /// `next_offset` is `nil`/`null`, iteration is complete.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the request
    /// is malformed.
    pub fn scroll(&self, request: ScrollRequest) -> Result<ScrollResponse> {
        self.with_shard(|shard| {
            let (records, next_offset) = shard.scroll(request.try_into()?)?;
            Ok(ScrollResponse {
                records: records.into_iter().map(Record::from).collect(),
                next_offset: next_offset.map(PointId::from),
            })
        })
    }
}

// ── ScrollRequest ───────────────────────────────────────────────────────────

/// A batched iteration request.
///
/// Pass the returned `next_offset` from [`ScrollResponse`] as `offset` on the
/// next call to page through all matching points.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScrollRequest {
    /// Opaque cursor from a previous scroll, or `None`/`null` to start
    /// from the beginning.
    #[uniffi(default = None)]
    pub offset: Option<PointId>,
    /// Maximum points per page. Defaults to a server-side value when unset.
    #[uniffi(default = None)]
    pub limit: Option<u64>,
    /// Optional filter applied to the iteration.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
    /// Include payload in each record.
    #[uniffi(default = None)]
    pub with_payload: Option<WithPayload>,
    /// Include vectors in each record.
    #[uniffi(default = None)]
    pub with_vector: Option<WithVector>,
    /// Iterate in payload-key order instead of ID order. Requires a
    /// payload index on the key.
    #[uniffi(default = None)]
    pub order_by: Option<OrderBy>,
}

impl TryFrom<ScrollRequest> for edge::ScrollRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: ScrollRequest) -> Result<Self, Self::Error> {
        let ScrollRequest {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = r;
        Ok(edge::ScrollRequest {
            offset: offset.map(PointIdType::try_from).transpose()?,
            limit: limit
                .map(|v| crate::error::bounded_limit("limit", v))
                .transpose()?,
            filter: filter.map(SegmentFilter::try_from).transpose()?,
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?,
            with_vector: with_vector.map(SegmentWithVector::from).unwrap_or_default(),
            order_by: order_by
                .map(|o| SegmentOrderBy::try_from(o).map(OrderByInterface::Struct))
                .transpose()?,
        })
    }
}

// ── ScrollResponse ──────────────────────────────────────────────────────────

/// A single page of results returned by [`EdgeShard::scroll`].
///
/// When `next_offset` is `Some`/non-`null`, more records are available;
/// pass it back as `ScrollRequest.offset` on the next call to continue
/// iteration. When `next_offset` is `None`/`null`, the scroll is exhausted.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScrollResponse {
    /// The records in this page, in scroll order.
    pub records: Vec<Record>,
    /// Opaque cursor to pass as the `offset` on the next scroll call, or
    /// `None`/`null` if the scroll is complete.
    pub next_offset: Option<PointId>,
}
