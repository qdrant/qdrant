//! SIMD-accelerated 4-bit codebook dot product.
//!
//! For 4-bit quantization, the codebook has exactly 16 centroid values.
//! Both centroids and query values are quantized to i16 for integer SIMD
//! multiply-accumulate, then converted back to f32 using precomputed scales.
//!
//! The codebook centroids (16 × i16) are stored as split byte tables
//! for efficient SIMD table lookup from 4-bit packed indices.
//!
//! All kernels accumulate in i64 and convert to float once at the end.

#[cfg(target_arch = "x86_64")]
mod x86;

#[cfg(target_arch = "aarch64")]
mod arm;

/// SIMD-ready codebook for 4-bit quantization (16 centroids).
///
/// Centroids are quantized to i16 and stored as two 16-byte tables
/// (low bytes and high bytes) for SIMD shuffle-based lookup.
pub struct SimdCodebook4 {
    /// Low bytes of i16 centroids: `lo[j] = (centroid_i16[j] as u16 & 0xFF) as u8`
    pub centroid_bytes_lo: [u8; 16],
    /// High bytes of i16 centroids: `hi[j] = ((centroid_i16[j] as u16) >> 8) as u8`
    pub centroid_bytes_hi: [u8; 16],
    /// Quantization scale: `f32_centroid ≈ i16_centroid / centroid_scale`
    pub centroid_scale: f32,
}

/// SIMD-ready quantized query for 4-bit codebook dot product.
pub struct SimdQuery4 {
    /// Query values quantized to i16 (padded to multiple of 32 for AVX2).
    pub values: Vec<i16>,
    /// Quantization scale: `f32_query ≈ i16_query / query_scale`
    pub query_scale: f32,
}

impl SimdCodebook4 {
    /// Create from f32 centroids. Returns None if not exactly 16 centroids.
    pub fn new(centroids: &[f32]) -> Option<Self> {
        if centroids.len() != 16 {
            return None;
        }

        let max_abs = centroids
            .iter()
            .map(|&c| c.abs())
            .fold(0.0f32, f32::max);

        if max_abs < f32::EPSILON {
            return Some(Self {
                centroid_bytes_lo: [0; 16],
                centroid_bytes_hi: [0; 16],
                centroid_scale: 1.0,
            });
        }

        let scale = 32767.0 / max_abs;
        let mut lo = [0u8; 16];
        let mut hi = [0u8; 16];

        for (j, &c) in centroids.iter().enumerate() {
            let i16_val = (c * scale).round() as i16;
            let u16_bits = i16_val as u16;
            lo[j] = (u16_bits & 0xFF) as u8;
            hi[j] = (u16_bits >> 8) as u8;
        }

        Some(Self {
            centroid_bytes_lo: lo,
            centroid_bytes_hi: hi,
            centroid_scale: scale,
        })
    }
}

impl SimdQuery4 {
    /// Quantize effective_query to i16 values. Pads to multiple of 32 for AVX2.
    pub fn new(effective_query: &[f32]) -> Self {
        let max_abs = effective_query
            .iter()
            .map(|&q| q.abs())
            .fold(0.0f32, f32::max);

        let scale = if max_abs > f32::EPSILON {
            32767.0 / max_abs
        } else {
            1.0
        };

        // Pad to multiple of 32 for AVX2 alignment
        let padded_len = (effective_query.len() + 31) & !31;
        let mut values = vec![0i16; padded_len];
        for (i, &q) in effective_query.iter().enumerate() {
            values[i] = (q * scale).round() as i16;
        }

        Self {
            values,
            query_scale: scale,
        }
    }
}

/// Reconstruct i16 centroids from split byte tables.
fn reconstruct_centroids_i16(codebook: &SimdCodebook4) -> [i16; 16] {
    let mut centroids = [0i16; 16];
    for j in 0..16 {
        centroids[j] =
            (codebook.centroid_bytes_lo[j] as u16 | ((codebook.centroid_bytes_hi[j] as u16) << 8))
                as i16;
    }
    centroids
}

/// SIMD-accelerated dot product for 4-bit packed indices with i16 codebook/query.
///
/// Computes `Σ_i effective_query[i] * centroids[unpack_4bit(packed, i)]`
/// using integer SIMD with i64 accumulation, converts to float once at the end.
pub fn codebook_dot_4bit(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            return unsafe { x86::codebook_dot_avx2(packed, codebook, query, padded_dim) };
        }
        if std::arch::is_x86_feature_detected!("ssse3") {
            return unsafe { x86::codebook_dot_ssse3(packed, codebook, query, padded_dim) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { arm::codebook_dot_neon(packed, codebook, query, padded_dim) };
    }

    #[allow(unreachable_code)]
    codebook_dot_scalar(packed, codebook, query, padded_dim)
}

/// Scalar fallback for platforms without SIMD.
fn codebook_dot_scalar(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    let centroids_i16 = reconstruct_centroids_i16(codebook);

    let mut sum = 0i64;
    for k in 0..(padded_dim / 2) {
        let byte = packed[k];
        let coord = k * 2;
        sum += query.values[coord] as i64 * centroids_i16[(byte >> 4) as usize] as i64;
        sum += query.values[coord + 1] as i64 * centroids_i16[(byte & 0x0F) as usize] as i64;
    }

    (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
}
