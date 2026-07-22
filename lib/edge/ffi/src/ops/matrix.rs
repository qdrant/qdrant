//! [`EdgeShard::search_matrix`] — pairwise distance matrix over sampled
//! points.
//!
//! # Kept off the mobile surface
//!
//! This whole module is gated behind the off-by-default `matrix` Cargo
//! feature. Because the exported items self-register at compile time (UniFFI
//! proc-macro mode, no UDL), gating the module removes them from the generated
//! bindings entirely when the feature is off. The mobile Swift/Kotlin bindgen
//! (added in a follow-up PR) must build with default features so this op stays
//! off the mobile surface; it exists for non-mobile UniFFI consumers
//! (desktop / server-side / Rust callers) that opt in explicitly.
//!
//! # Cost — the opting-in consumer owns the bound
//!
//! `search_matrix` is an analytics operation: it computes each sampled point's
//! neighbours *within the sample*, i.e. **O(sample_size²)** distance
//! evaluations, and materializes up to `sample_size × limit_per_sample`
//! results. The per-field [`bounded_limit`](crate::error::bounded_limit) checks
//! cap each of `sample_size`/`limit_per_sample` at
//! [`MAX_RESULT_COUNT`](crate::error::MAX_RESULT_COUNT) (1,048,576) — which is
//! itself far too large for an O(n²) op — so they do **not** bound the
//! quadratic compute: a large `sample_size` on a large shard can hang and
//! exhaust memory. This is deliberately left uncapped here. The operation is
//! opt-in (feature-gated, off the mobile surface), so the consumer that enables
//! it owns the responsibility of constraining `sample_size`/`limit_per_sample`
//! to what its environment can afford; there is no bound enforced in this crate.

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
