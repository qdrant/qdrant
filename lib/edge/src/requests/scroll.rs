use segment::data_types::load_profile::LoadProfile;
use segment::data_types::order_by::OrderByInterface;
use segment::types::{Filter, PointIdType, WithPayloadInterface, WithVector};
use shard::scroll::ScrollRequestInternal;

/// Scroll request — paginate over all points which match the given conditions.
#[derive(Clone, Debug, PartialEq)]
pub struct ScrollRequest {
    /// Start ID to read points from.
    pub offset: Option<PointIdType>,
    /// Page size. Default: 10.
    pub limit: Option<usize>,
    /// Look only for points which satisfy these conditions. If not provided — all points.
    pub filter: Option<Filter>,
    /// Select which payload to return with the response. Default is true.
    pub with_payload: Option<WithPayloadInterface>,
    /// Options for specifying which vectors to include into the response. Default is false.
    pub with_vector: WithVector,
    /// Order the records by a payload field instead of by id.
    pub order_by: Option<OrderByInterface>,
}

impl ScrollRequest {
    pub fn new() -> Self {
        Self {
            offset: None,
            limit: None,
            filter: None,
            with_payload: None,
            with_vector: WithVector::Bool(false),
            order_by: None,
        }
    }

    /// Request-specific [`LoadProfile`] for opening a read-only shard to serve exactly
    /// this scroll: no vector components are warmed, and only the field indexes the
    /// filter and `order_by` read keep their configured placement.
    pub fn load_profile(&self) -> LoadProfile {
        ScrollRequestInternal::from(self.clone()).load_profile()
    }
}

impl Default for ScrollRequest {
    fn default() -> Self {
        Self::new()
    }
}
