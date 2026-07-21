//! [`EdgeShard::search_matrix`] — pairwise distance matrix over sampled
//! points.

use edge::EdgeShardRead as _;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::Filter as SegmentFilter;

use crate::EdgeShard;
use crate::error::Result;
use crate::filter::Filter;
use crate::types::{PointId, ScoredPoint};

#[uniffi::export]
impl EdgeShard {
    /// Samples `request.sample_size` random points that carry the target
    /// vector and returns each sample's nearest neighbours *within the
    /// sampled set* — a distance matrix useful for clustering and
    /// visualisation.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the filter
    /// is invalid or the vector field does not exist.
    pub fn search_matrix(&self, request: SearchMatrixRequest) -> Result<SearchMatrixResponse> {
        self.with_shard(|shard| {
            let response = shard.search_matrix(request.try_into()?)?;
            let edge::SearchMatrixResponse {
                sample_ids,
                nearests,
            } = response;
            Ok(SearchMatrixResponse {
                sample_ids: sample_ids.into_iter().map(PointId::from).collect(),
                nearests: nearests
                    .into_iter()
                    .map(|row| row.into_iter().map(ScoredPoint::from).collect())
                    .collect(),
            })
        })
    }
}

// ── SearchMatrixRequest ─────────────────────────────────────────────────────

/// A distance-matrix request: sample random points, then relate each sample
/// to its nearest neighbours among the other samples.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchMatrixRequest {
    /// How many random points to sample.
    pub sample_size: u64,
    /// How many nearest neighbours to return per sampled point.
    pub limit_per_sample: u64,
    /// Only sample points satisfying this filter.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
    /// Name of the dense vector field the samples are compared by;
    /// `None`/`null` targets the default (unnamed) field.
    #[uniffi(default = None)]
    pub using: Option<String>,
}

impl TryFrom<SearchMatrixRequest> for edge::SearchMatrixRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: SearchMatrixRequest) -> Result<Self, Self::Error> {
        let SearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = r;
        Ok(edge::SearchMatrixRequest {
            sample_size: crate::error::bounded_limit("sample_size", sample_size)?,
            limit_per_sample: crate::error::bounded_limit("limit_per_sample", limit_per_sample)?,
            filter: filter.map(SegmentFilter::try_from).transpose()?,
            using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string()),
        })
    }
}

// ── SearchMatrixResponse ────────────────────────────────────────────────────

/// The result of [`EdgeShard::search_matrix`].
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchMatrixResponse {
    /// The sampled point IDs, in sample order.
    pub sample_ids: Vec<PointId>,
    /// For each sampled point (aligned with `sample_ids`), its nearest
    /// neighbours within the sampled set, best first.
    pub nearests: Vec<Vec<ScoredPoint>>,
}
