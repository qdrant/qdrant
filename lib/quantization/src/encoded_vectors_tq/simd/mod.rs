//! SIMD-accelerated 4-bit codebook dot product.
//!
//! For 4-bit quantization, the codebook has exactly 16 centroid values.
//! Both centroids and query values are quantized to i16 for integer SIMD
//! multiply-accumulate, then converted back to f32 using precomputed scales.
//!
//! The codebook centroids (16 × i16) are stored as split byte tables
//! for efficient SIMD table lookup from 4-bit packed indices.

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
    /// Query values quantized to i16 (padded to multiple of 16).
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
    /// Quantize effective_query to i16 values. Pads to multiple of 16.
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

        // Pad to multiple of 16 for SIMD alignment
        let padded_len = (effective_query.len() + 15) & !15;
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

/// SIMD-accelerated dot product for 4-bit packed indices with i16 codebook/query.
///
/// Computes `Σ_i effective_query[i] * centroids[unpack_4bit(packed, i)]`
/// using integer SIMD and converts back to f32.
///
/// `packed` must contain at least `padded_dim / 2` bytes of 4-bit packed indices.
/// Falls back to scalar if SIMD is not available.
pub fn codebook_dot_4bit(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
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
    // Reconstruct i16 centroids from byte tables
    let mut centroids_i16 = [0i16; 16];
    for j in 0..16 {
        centroids_i16[j] =
            (codebook.centroid_bytes_lo[j] as u16 | ((codebook.centroid_bytes_hi[j] as u16) << 8))
                as i16;
    }

    let mut sum = 0i64;
    let num_pairs = padded_dim / 2;
    for byte_idx in 0..num_pairs {
        let byte = packed[byte_idx];
        let idx_even = (byte >> 4) as usize;
        let idx_odd = (byte & 0x0F) as usize;
        let coord_even = byte_idx * 2;
        let coord_odd = coord_even + 1;
        sum += query.values[coord_even] as i64 * centroids_i16[idx_even] as i64;
        sum += query.values[coord_odd] as i64 * centroids_i16[idx_odd] as i64;
    }

    sum as f32 / (codebook.centroid_scale * query.query_scale)
}
