use super::simd;

/// Pre-processed query for efficient TurboQuant scoring.
pub struct EncodedQueryTQ {
    /// Rotated query (padded_dim elements).
    pub(crate) rotated_query: Vec<f32>,
    /// Original query (dim elements). Needed for L1 scoring which is not
    /// rotation-invariant.
    pub(crate) original_query: Vec<f32>,
    /// ||q||². Used for L2 scoring: ||q−x̃||² = ||q||² + γ² − 2γ⟨Rq, ŷ⟩.
    pub(crate) query_norm_sq: f32,
    /// S · weighted_query, precomputed for O(d) QJL correction in Dot/L2 scoring.
    /// Present only when correction == Qjl (not QjlNormalization).
    pub(crate) qjl_projected_query: Option<Vec<f32>>,
    /// Effective query for on-the-fly dot product with codebook centroids:
    ///   effective_query[i] = rotated_query[i] / scales[i]  (TQ+)
    ///   effective_query[i] = rotated_query[i]               (non-TQ+)
    /// None for QjlNormalization/QjlShortNormalization (fallback to decode_rotated).
    pub(crate) effective_query: Option<Vec<f32>>,
    /// Precomputed Σ_i rotated_query[i] * medians[i]. Zero when no TQ+.
    pub(crate) median_dot: f32,
    /// SIMD-quantized query for 4-bit codebook dot. Created when bits == 4 and SIMD available.
    pub(crate) simd_query: Option<simd::SimdQuery4>,
}
