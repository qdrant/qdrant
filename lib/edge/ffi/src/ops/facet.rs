//! [`EdgeShard::facet`] — aggregating distinct payload values with counts.

use segment::data_types::facets::{
    FacetHit as SegmentFacetHit, FacetResponse as SegmentFacetResponse, FacetValue,
};
use segment::types::Filter as SegmentFilter;

use crate::EdgeShard;
use crate::error::Result;
use crate::filter::Filter;

#[uniffi::export]
impl EdgeShard {
    /// Aggregates distinct payload values for a given key, with counts.
    ///
    /// Faceting requires the payload `key` to have a payload index; call
    /// [`UpdateOperation::set_payload`](crate::update::UpdateOperation::set_payload)
    /// and index the field beforehand.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the payload
    /// key is not indexed.
    pub fn facet(&self, request: FacetRequest) -> Result<FacetResponse> {
        self.with_shard(|shard| {
            let SegmentFacetResponse { hits } = shard.facet(request.try_into()?)?;
            let hits = hits
                .into_iter()
                .map(|SegmentFacetHit { value, count }| FacetHit {
                    value: match value {
                        FacetValue::Keyword(s) => s,
                        FacetValue::Int(i) => i.to_string(),
                        FacetValue::Uuid(u) => uuid::Uuid::from_u128(u).to_string(),
                        FacetValue::Bool(b) => b.to_string(),
                    },
                    count: count as u64,
                })
                .collect();
            Ok(FacetResponse { hits })
        })
    }
}

// ── FacetRequest ────────────────────────────────────────────────────────────

/// A facet (grouped count) request.
///
/// Facets aggregate distinct payload values with counts, useful for
/// building UI filter sidebars.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FacetRequest {
    /// Payload key to facet on. Must have a payload index.
    pub key: String,
    /// Maximum number of distinct values to return.
    #[uniffi(default = 10)]
    pub limit: u64,
    /// If `true`, count every matching point; otherwise a fast estimate
    /// may be returned.
    #[uniffi(default = false)]
    pub exact: bool,
    /// Optional filter restricting which points participate in the facet.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
}

impl TryFrom<FacetRequest> for edge::FacetRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: FacetRequest) -> Result<Self, Self::Error> {
        let FacetRequest {
            key,
            limit,
            exact,
            filter,
        } = r;
        Ok(edge::FacetRequest {
            key: crate::error::parse_json_path(&key)?,
            limit: crate::error::bounded_limit("limit", limit)?,
            filter: filter.map(SegmentFilter::try_from).transpose()?,
            exact,
        })
    }
}

// ── FacetResponse ───────────────────────────────────────────────────────────

/// A single (value, count) row in a facet response.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FacetHit {
    /// Facet value, stringified. Integer/UUID/bool values are rendered as
    /// their canonical string form.
    pub value: String,
    /// Number of points with this facet value.
    pub count: u64,
}

/// The result of a facet aggregation.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FacetResponse {
    /// Hits in descending count order, up to `FacetRequest.limit` entries.
    pub hits: Vec<FacetHit>,
}
