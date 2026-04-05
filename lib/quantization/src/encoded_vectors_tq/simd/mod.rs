//! SIMD-accelerated codebook dot product for 4-bit and 2-bit quantization.
//!
//! Both centroids and query values are quantized to i16 for integer SIMD
//! multiply-accumulate, then converted back to f32 using precomputed scales.
//!
//! 4-bit: 16-entry shuffle table, nibble → centroid.
//! 2-bit: fused kernel with two 16-entry tables (even/odd) and deinterleaved
//!        query. Single pass over packed data, shared scales, one i64 accumulator.
//!
//! All kernels accumulate in i64 and convert to float once at the end.

#[cfg(target_arch = "x86_64")]
mod x86;

#[cfg(target_arch = "aarch64")]
mod arm;

// ============================================================================
// 4-bit types
// ============================================================================

/// SIMD-ready 16-entry lookup table for shuffle-based centroid lookup.
pub struct SimdCodebook4 {
    pub centroid_bytes_lo: [u8; 16],
    pub centroid_bytes_hi: [u8; 16],
    pub centroid_scale: f32,
}

/// SIMD-ready quantized query vector for 4-bit.
pub struct SimdQuery4 {
    pub values: Vec<i16>,
    pub query_scale: f32,
}

// ============================================================================
// 2-bit types — fused even/odd with shared scales
// ============================================================================

/// Fused 2-bit codebook: two 16-entry tables with a shared centroid scale.
///
/// Each nibble encodes two 2-bit indices: `nibble = idx_even << 2 | idx_odd`.
/// `even` maps nibble → `centroid[nibble >> 2]`, `odd` maps nibble → `centroid[nibble & 3]`.
/// Both tables contain the same 4 centroid values rearranged, so they share one scale.
pub struct SimdCodebook2 {
    pub even_lo: [u8; 16],
    pub even_hi: [u8; 16],
    pub odd_lo: [u8; 16],
    pub odd_hi: [u8; 16],
    pub centroid_scale: f32,
}

/// Fused 2-bit query: deinterleaved even/odd streams with a shared query scale.
pub struct SimdQuery2 {
    /// even_values[k] = quantize(effective_query[2k])
    pub even_values: Vec<i16>,
    /// odd_values[k] = quantize(effective_query[2k+1])
    pub odd_values: Vec<i16>,
    pub query_scale: f32,
}

// ============================================================================
// Constructors
// ============================================================================

fn quantize_table_to_i16_bytes(
    values: &[f32; 16],
    scale: f32,
) -> ([u8; 16], [u8; 16]) {
    let mut lo = [0u8; 16];
    let mut hi = [0u8; 16];
    for (j, &c) in values.iter().enumerate() {
        let i16_val = (c * scale).round() as i16;
        let u16_bits = i16_val as u16;
        lo[j] = (u16_bits & 0xFF) as u8;
        hi[j] = (u16_bits >> 8) as u8;
    }
    (lo, hi)
}

impl SimdCodebook4 {
    /// Create from f32 centroid values. Must be exactly 16 entries.
    fn from_f32_table(centroids: &[f32; 16]) -> Self {
        let max_abs = centroids.iter().map(|&c| c.abs()).fold(0.0f32, f32::max);
        if max_abs < f32::EPSILON {
            return Self {
                centroid_bytes_lo: [0; 16],
                centroid_bytes_hi: [0; 16],
                centroid_scale: 1.0,
            };
        }
        let scale = 32767.0 / max_abs;
        let (lo, hi) = quantize_table_to_i16_bytes(centroids, scale);
        Self {
            centroid_bytes_lo: lo,
            centroid_bytes_hi: hi,
            centroid_scale: scale,
        }
    }
}

impl SimdCodebook2 {
    /// Create from 4 f32 centroids. Builds two 16-entry tables with shared scale.
    fn new(centroids: &[f32]) -> Option<Self> {
        if centroids.len() != 4 {
            return None;
        }
        let max_abs = centroids.iter().map(|&c| c.abs()).fold(0.0f32, f32::max);
        if max_abs < f32::EPSILON {
            return Some(Self {
                even_lo: [0; 16],
                even_hi: [0; 16],
                odd_lo: [0; 16],
                odd_hi: [0; 16],
                centroid_scale: 1.0,
            });
        }
        let scale = 32767.0 / max_abs;

        let mut even_table = [0.0f32; 16];
        let mut odd_table = [0.0f32; 16];
        for j in 0..16 {
            even_table[j] = centroids[j >> 2];
            odd_table[j] = centroids[j & 3];
        }

        let (even_lo, even_hi) = quantize_table_to_i16_bytes(&even_table, scale);
        let (odd_lo, odd_hi) = quantize_table_to_i16_bytes(&odd_table, scale);
        Some(Self {
            even_lo,
            even_hi,
            odd_lo,
            odd_hi,
            centroid_scale: scale,
        })
    }
}

impl SimdQuery4 {
    /// Quantize f32 query values to i16. Pads to multiple of 32 for AVX2.
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

impl SimdQuery2 {
    /// Deinterleave and quantize with a shared scale across even/odd.
    fn new(effective_query: &[f32]) -> Self {
        // Compute shared scale from all values
        let max_abs = effective_query
            .iter()
            .map(|&q| q.abs())
            .fold(0.0f32, f32::max);
        let scale = if max_abs > f32::EPSILON {
            32767.0 / max_abs
        } else {
            1.0
        };

        let half_len = (effective_query.len() + 1) / 2;
        let padded_half = (half_len + 31) & !31;

        let mut even_values = vec![0i16; padded_half];
        let mut odd_values = vec![0i16; padded_half];
        for (i, &q) in effective_query.iter().enumerate() {
            let val = (q * scale).round() as i16;
            if i % 2 == 0 {
                even_values[i / 2] = val;
            } else {
                odd_values[i / 2] = val;
            }
        }

        Self {
            even_values,
            odd_values,
            query_scale: scale,
        }
    }
}

// ============================================================================
// 1-bit types — RaBitQ-style AND+popcount with u16 bit-plane transposition
// ============================================================================

/// 1-bit codebook: symmetric centroids ±c.
pub struct SimdCodebook1 {
    /// The positive centroid value (centroids are -c, +c).
    pub c: f32,
}

/// 1-bit query: u16 bit-plane transposed for AND+popcount scoring.
///
/// Layout: `planes[chunk * 16 + b]` is a u64 containing bit `b` of all 64
/// u16-quantized query values in that chunk. Bit ordering within each u64
/// matches the MSB-first packing used by `packing.rs`.
pub struct SimdQuery1 {
    pub planes: Vec<u64>,
    pub num_chunks: usize,
    /// Converts u16 sum back to float: `(2 * max_abs) / 65535`
    pub inv_scale: f64,
    /// Bias correction per set bit in vector.
    pub max_abs: f64,
    /// Precomputed `Σ effective_query[i]`.
    pub total_sum: f64,
}

impl SimdCodebook1 {
    fn new(centroids: &[f32]) -> Option<Self> {
        if centroids.len() != 2 {
            return None;
        }
        // centroids[1] is the positive one (c0 = -c, c1 = +c)
        Some(Self { c: centroids[1] })
    }
}

impl SimdQuery1 {
    fn new(effective_query: &[f32]) -> Self {
        let max_abs = effective_query
            .iter()
            .map(|&q| q.abs())
            .fold(0.0f32, f32::max);

        let total_sum: f64 = effective_query.iter().map(|&q| q as f64).sum();

        let two_max = 2.0 * max_abs;
        let u16_scale = if two_max > f32::EPSILON {
            65535.0 / two_max
        } else {
            1.0
        };

        // Pad to multiple of 64 for chunk alignment
        let padded_len = (effective_query.len() + 63) & !63;
        let num_chunks = padded_len / 64;
        let mut planes = vec![0u64; num_chunks * 16];

        for (i, &eq) in effective_query.iter().enumerate() {
            let u16_val = ((eq + max_abs) * u16_scale).round() as u16;
            let chunk = i / 64;
            let local = i % 64;
            // MSB-first bit position matching packing.rs layout
            let bit_in_u64 = (local / 8) * 8 + (7 - (local % 8));
            for b in 0..16u16 {
                if (u16_val >> b) & 1 != 0 {
                    planes[chunk * 16 + b as usize] |= 1u64 << bit_in_u64;
                }
            }
        }

        Self {
            planes,
            num_chunks,
            inv_scale: if two_max > f32::EPSILON {
                two_max as f64 / 65535.0
            } else {
                0.0
            },
            max_abs: max_abs as f64,
            total_sum,
        }
    }
}

// ============================================================================
// Unified SIMD acceleration types
// ============================================================================

/// SIMD-accelerated codebook for 1, 2, or 4-bit quantization.
pub enum SimdAccel {
    Bits4(SimdCodebook4),
    Bits2(SimdCodebook2),
    Bits1(SimdCodebook1),
}

/// SIMD-accelerated query, matches the corresponding `SimdAccel` variant.
pub enum SimdQueryAccel {
    Bits4(SimdQuery4),
    Bits2(SimdQuery2),
    Bits1(SimdQuery1),
}

impl SimdAccel {
    /// Create for 4-bit quantization (16 centroids).
    pub fn new_4bit(centroids: &[f32]) -> Option<Self> {
        if centroids.len() != 16 {
            return None;
        }
        let mut table = [0.0f32; 16];
        table.copy_from_slice(centroids);
        Some(Self::Bits4(SimdCodebook4::from_f32_table(&table)))
    }

    /// Create for 2-bit quantization (4 centroids).
    pub fn new_2bit(centroids: &[f32]) -> Option<Self> {
        SimdCodebook2::new(centroids).map(Self::Bits2)
    }

    /// Create for 1-bit quantization (2 symmetric centroids ±c).
    pub fn new_1bit(centroids: &[f32]) -> Option<Self> {
        SimdCodebook1::new(centroids).map(Self::Bits1)
    }
}

impl SimdQueryAccel {
    pub fn new_4bit(effective_query: &[f32]) -> Self {
        Self::Bits4(SimdQuery4::new(effective_query))
    }

    pub fn new_2bit(effective_query: &[f32]) -> Self {
        Self::Bits2(SimdQuery2::new(effective_query))
    }

    pub fn new_1bit(effective_query: &[f32]) -> Self {
        Self::Bits1(SimdQuery1::new(effective_query))
    }
}

/// Dispatch SIMD dot product for the appropriate bit width.
pub fn codebook_dot_simd(
    packed: &[u8],
    accel: &SimdAccel,
    query: &SimdQueryAccel,
    padded_dim: usize,
) -> f32 {
    match (accel, query) {
        (SimdAccel::Bits4(codebook), SimdQueryAccel::Bits4(q)) => {
            codebook_dot_4bit(packed, codebook, q, padded_dim)
        }
        (SimdAccel::Bits2(codebook), SimdQueryAccel::Bits2(q)) => {
            codebook_dot_2bit(packed, codebook, q, padded_dim)
        }
        (SimdAccel::Bits1(codebook), SimdQueryAccel::Bits1(q)) => {
            codebook_dot_1bit(packed, codebook, q)
        }
        _ => unreachable!("mismatched SimdAccel and SimdQueryAccel variants"),
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

/// Reconstruct i16 centroids from split byte tables.
fn reconstruct_centroids_i16(lo: &[u8; 16], hi: &[u8; 16]) -> [i16; 16] {
    let mut centroids = [0i16; 16];
    for j in 0..16 {
        centroids[j] = (lo[j] as u16 | ((hi[j] as u16) << 8)) as i16;
    }
    centroids
}

// ============================================================================
// 4-bit kernel dispatch
// ============================================================================

fn codebook_dot_4bit(
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
    codebook_dot_4bit_scalar(packed, codebook, query, padded_dim)
}

fn codebook_dot_4bit_scalar(
    packed: &[u8],
    codebook: &SimdCodebook4,
    query: &SimdQuery4,
    padded_dim: usize,
) -> f32 {
    let centroids_i16 =
        reconstruct_centroids_i16(&codebook.centroid_bytes_lo, &codebook.centroid_bytes_hi);
    let mut sum = 0i64;
    for k in 0..(padded_dim / 2) {
        let byte = packed[k];
        let coord = k * 2;
        sum += query.values[coord] as i64 * centroids_i16[(byte >> 4) as usize] as i64;
        sum += query.values[coord + 1] as i64 * centroids_i16[(byte & 0x0F) as usize] as i64;
    }
    (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
}

// ============================================================================
// 2-bit kernel dispatch
// ============================================================================

fn codebook_dot_2bit(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    padded_dim: usize,
) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            return unsafe { x86::codebook_dot_2bit_avx2(packed, codebook, query, padded_dim) };
        }
        if std::arch::is_x86_feature_detected!("ssse3") {
            return unsafe { x86::codebook_dot_2bit_ssse3(packed, codebook, query, padded_dim) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { arm::codebook_dot_2bit_neon(packed, codebook, query, padded_dim) };
    }

    #[allow(unreachable_code)]
    codebook_dot_2bit_scalar(packed, codebook, query, padded_dim)
}

fn codebook_dot_2bit_scalar(
    packed: &[u8],
    codebook: &SimdCodebook2,
    query: &SimdQuery2,
    padded_dim: usize,
) -> f32 {
    let even_i16 = reconstruct_centroids_i16(&codebook.even_lo, &codebook.even_hi);
    let odd_i16 = reconstruct_centroids_i16(&codebook.odd_lo, &codebook.odd_hi);
    let mut sum = 0i64;
    // Each nibble encodes 2 coords: padded_dim coords → padded_dim/2 nibbles → padded_dim/4 bytes
    let num_nibbles = padded_dim / 2;
    for nib_idx in 0..num_nibbles {
        let byte = packed[nib_idx / 2];
        let nibble = if nib_idx % 2 == 0 {
            (byte >> 4) as usize
        } else {
            (byte & 0x0F) as usize
        };
        sum += query.even_values[nib_idx] as i64 * even_i16[nibble] as i64;
        sum += query.odd_values[nib_idx] as i64 * odd_i16[nibble] as i64;
    }
    (sum as f64 / (codebook.centroid_scale as f64 * query.query_scale as f64)) as f32
}

// ============================================================================
// 1-bit kernel dispatch
// ============================================================================

fn codebook_dot_1bit(
    packed: &[u8],
    codebook: &SimdCodebook1,
    query: &SimdQuery1,
) -> f32 {
    let (s1_u16, popcnt_v) = and_popcount_u16(packed, query);
    let s1 = s1_u16 as f64 * query.inv_scale - popcnt_v as f64 * query.max_abs;
    let dot = codebook.c as f64 * (2.0 * s1 - query.total_sum);
    dot as f32
}

fn and_popcount_u16(packed: &[u8], query: &SimdQuery1) -> (u64, u32) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("popcnt") {
            return unsafe { x86::and_popcount_u16_x86(packed, query) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        return unsafe { arm::and_popcount_u16_neon(packed, query) };
    }

    #[allow(unreachable_code)]
    and_popcount_u16_scalar(packed, query)
}

fn and_popcount_u16_scalar(packed: &[u8], query: &SimdQuery1) -> (u64, u32) {
    let mut s1 = 0u64;
    let mut popcnt_v = 0u32;
    for chunk in 0..query.num_chunks {
        let offset = chunk * 8;
        let v = u64::from_ne_bytes(packed[offset..offset + 8].try_into().unwrap());
        popcnt_v += v.count_ones();
        let base = chunk * 16;
        for b in 0..16u64 {
            s1 += (v & query.planes[base + b as usize]).count_ones() as u64 * (1u64 << b);
        }
    }
    (s1, popcnt_v)
}
