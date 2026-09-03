//! Lookup-table scoring for 2-bit TurboQuant.
//!
//! An alternative to the [`QuerySimd`]-based scoring of
//! `TurboQuantizer::score_precomputed_batch`, used by
//! `EncodedVectorsTQ::score_points` for long contiguous runs when the
//! `QDRANT_TQ_LUT_SCAN` env flag is set (see [`scan_enabled`]).
//!
//! The algorithm differs from the production kernel in every axis:
//!
//! * **Layout**: codes are transposed into blocks of 32 vectors. Per block
//!   and per *dim group* (4 dims at 2 bits) each vector contributes one
//!   byte, split into two nibbles (2 dims each); the block stores 32 bytes
//!   per group, nibble-interleaved so a single `pshufb` looks up 32 vectors
//!   at once. Per-vector scaling factors move to a separate array.
//! * **Query**: instead of an integer-quantized query multiplied against a
//!   codebook, the query is baked into per-group lookup tables of
//!   `Σ q[d]·centroid[code]` over each nibble's 16 possible values,
//!   quantized to 7 bits (min-shifted, global scale, `bias = Σ mins`).
//! * **Accumulation**: `u8` LUT values accumulate in 16 `u16` lanes per
//!   register (one lane per vector), flushed into `f32` accumulators every
//!   [`U16_ACC_GROUPS`] groups. No per-vector horizontal reduction.
//!
//! Numeric contract: LUT entries are ≤ 127 and a flush covers ≤ 256 groups,
//! so a `u16` lane accumulates ≤ 127 · 256 = 32 512 per half-table and
//! ≤ 65 024 after the two half-tables are folded — no overflow.
//!
//! [`QuerySimd`]: super::query::QuerySimd

use common::bitpacking::BitReader;

use crate::turboquant::TQBits;

/// Vectors per transposed block.
const BLOCK_VECTORS: usize = 32;

/// Dim groups (4 dims sharing a code byte) accumulated in `u16` lanes
/// before flushing to `f32`.
/// See the module docs for the overflow bound tied to this value.
const U16_ACC_GROUPS: usize = 256;

/// Largest quantized LUT entry. 127 (not 255) keeps the `u16` lane bound
/// valid for a full [`U16_ACC_GROUPS`]-group run of both half-tables.
const LUT_ENTRY_MAX: f32 = 127.0;

/// Runtime toggle for the LUT scan path. Read from the
/// environment on every call so tests and benches can flip it; the callers
/// cache the outcome in per-query state (`EncodedQueryTQ`'s LUT) and
/// per-storage state (the blocked shadow).
pub fn scan_enabled() -> bool {
    std::env::var_os("QDRANT_TQ_LUT_SCAN").is_some_and(|value| !value.is_empty() && value != "0")
}

/// Whether the SIMD scan kernel is available on this CPU.
pub fn is_supported() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

/// Vector id scored by accumulator `r`'s `f32` lane `i` within a block.
///
/// The pack below places vector `w < 16` of a block into the *low* nibble of
/// group byte `w % 16` (looked up via the low-nibble mask, accumulators 0/1)
/// and vector `w >= 16` into the *high* nibble (accumulators 2/3); even byte
/// positions land in the even accumulator of the pair.
#[inline]
fn lane_id(r: usize, i: usize) -> usize {
    debug_assert!(r < 4 && i < 8);
    match r {
        0 => 2 * i,
        1 => 2 * i + 1,
        2 => 16 + 2 * i,
        _ => 17 + 2 * i,
    }
}

/// Inverse of [`lane_id`]: flat lane slot `r * 8 + i` of block vector `w`.
#[inline]
fn lane_slot(w: usize) -> usize {
    debug_assert!(w < BLOCK_VECTORS);
    if w < 16 {
        (w % 2) * 8 + w / 2
    } else {
        (2 + w % 2) * 8 + (w - 16) / 2
    }
}

/// 2-bit codes of one record's packed section, one code (0..4) per dim,
/// in dim order — the exact inverse of `TurboQuantizer::pack_vector`'s
/// `BitWriter` packing.
fn read_codes(packed: &[u8], padded_dim: usize) -> Vec<u8> {
    let mut reader = BitReader::new(packed);
    reader.set_bits(2);
    (0..padded_dim).map(|_| reader.read::<u8>()).collect()
}

/// 2-bit TurboQuant codes transposed into the block layout, with
/// the per-vector scaling factors extracted into a lane-ordered side array.
pub struct Blocked2bit {
    /// `n_blocks * n_groups * 32` bytes; see [`Self::pack`] for the layout.
    blocked: Vec<u8>,
    /// `n_blocks * 32` scaling factors, ordered by [`lane_slot`] within each
    /// block so the kernel epilogue loads them sequentially. Padding lanes
    /// hold `0.0`, which zeroes their scores.
    scales: Vec<f32>,
    n_groups: usize,
    n_blocks: usize,
    /// Real (un-padded) vector count.
    count: usize,
}

impl Blocked2bit {
    /// Transpose `count` row-major TurboQuant records (`stride` bytes each,
    /// 2-bit codes in the first `codes_len` bytes, `f32` scaling factor
    /// right after — the Dot/Cosine extras layout) into the block layout.
    ///
    /// Per block `b` of 32 vectors and dim group `g` (dims `4g..4g+4`),
    /// bytes `(b * n_groups + g) * 32 ..+32` hold:
    ///
    /// * byte `j` (j < 16): low-dim-pair nibble (`c[4g] | c[4g+1] << 2`) of
    ///   vector `j` in the low half, of vector `16 + j` in the high half;
    /// * byte `16 + j`: same for the high dim pair (`c[4g+2] | c[4g+3] << 2`).
    pub fn pack(
        records: &[u8],
        stride: usize,
        codes_len: usize,
        padded_dim: usize,
        count: usize,
    ) -> Self {
        assert!(codes_len + size_of::<f32>() <= stride);
        assert!(records.len() >= count * stride);
        Self::pack_records(count, codes_len, padded_dim, |v| {
            &records[v * stride..(v + 1) * stride]
        })
    }

    /// [`Self::pack`] over records fetched one by one — for storages that
    /// don't expose their records as one contiguous slice. `fetch(v)` must
    /// return record `v`: at least `codes_len` bytes of 2-bit codes followed
    /// by the `f32` scaling factor.
    pub fn pack_records<R: AsRef<[u8]>>(
        count: usize,
        codes_len: usize,
        padded_dim: usize,
        mut fetch: impl FnMut(usize) -> R,
    ) -> Self {
        assert_eq!(padded_dim % 4, 0, "2-bit dim groups hold 4 dims");

        let n_groups = padded_dim / 4;
        let n_blocks = count.div_ceil(BLOCK_VECTORS);
        let mut blocked = vec![0u8; n_blocks * n_groups * BLOCK_VECTORS];
        let mut scales = vec![0.0f32; n_blocks * BLOCK_VECTORS];

        for v in 0..count {
            let record = fetch(v);
            let record = record.as_ref();
            let codes = read_codes(&record[..codes_len], padded_dim);
            let sf = f32::from_le_bytes(
                record[codes_len..codes_len + size_of::<f32>()]
                    .try_into()
                    .expect("record too short for a scaling factor"),
            );

            let b = v / BLOCK_VECTORS;
            let w = v % BLOCK_VECTORS;
            let j = w % 16;
            let shift = 4 * (w / 16) as u8;
            for g in 0..n_groups {
                let nib_lo = codes[4 * g] | (codes[4 * g + 1] << 2);
                let nib_hi = codes[4 * g + 2] | (codes[4 * g + 3] << 2);
                let base = (b * n_groups + g) * BLOCK_VECTORS;
                blocked[base + j] |= nib_lo << shift;
                blocked[base + 16 + j] |= nib_hi << shift;
            }
            scales[b * BLOCK_VECTORS + lane_slot(w)] = sf;
        }

        Self {
            blocked,
            scales,
            n_groups,
            n_blocks,
            count,
        }
    }

    /// Heap bytes owned by the shadow copy.
    pub fn heap_size_bytes(&self) -> usize {
        self.blocked.capacity() + self.scales.capacity() * size_of::<f32>()
    }

    /// Score the contiguous id range `start_id..start_id + out.len()` into
    /// `out`, in id order. The range must lie within the packed vectors.
    /// Blocks only partially covered by the range are still scored whole;
    /// long runs (the full-scan case) waste nothing.
    ///
    /// Panics when [`is_supported`] is false.
    pub fn score_range(&self, lut: &QueryLut2bit, start_id: usize, out: &mut [f32]) {
        assert!(is_supported(), "query2bit_lut requires AVX2 + FMA");
        assert_eq!(lut.n_groups, self.n_groups, "query/storage dim mismatch");
        assert!(start_id + out.len() <= self.count, "range out of bounds");
        if out.is_empty() {
            return;
        }
        #[cfg(target_arch = "x86_64")]
        {
            // SAFETY: `is_supported` verified AVX2 + FMA above.
            unsafe { x64::score_range(self, lut, start_id, out) };
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            unreachable!("is_supported is false off x86_64")
        }
    }

    /// Score every packed vector against `lut`; `scores[id]` is the vector's
    /// approximate `(query ⋅ vector)` (raw LUT dot × scaling factor).
    ///
    /// Panics when [`is_supported`] is false.
    pub fn score_all(&self, lut: &QueryLut2bit) -> Vec<f32> {
        let mut scores = vec![0.0f32; self.count];
        self.score_range(lut, 0, &mut scores);
        scores
    }

    /// Fused scan + top-k: scores every vector and keeps the `k` best,
    /// gating the per-block heap maintenance on a SIMD compare against the
    /// current k-th best. Returns `(score, id)` sorted by descending score.
    ///
    /// Panics when [`is_supported`] is false.
    pub fn scan_top_k(&self, lut: &QueryLut2bit, k: usize) -> Vec<(f32, u32)> {
        assert!(is_supported(), "query2bit_lut requires AVX2 + FMA");
        assert_eq!(lut.n_groups, self.n_groups, "query/storage dim mismatch");
        if k == 0 {
            return Vec::new();
        }
        #[cfg(target_arch = "x86_64")]
        {
            let mut top = TopK::new(k);
            // SAFETY: `is_supported` verified AVX2 + FMA above.
            unsafe { x64::scan_top_k(self, lut, &mut top) };
            let mut entries = top.entries;
            entries.sort_by(|a, b| b.partial_cmp(a).expect("scores are not NaN"));
            entries
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            unreachable!("is_supported is false off x86_64")
        }
    }
}

/// Minimal top-k accumulator: unsorted entries plus the index of the current
/// minimum, so the hot path is one compare against [`Self::threshold`].
struct TopK {
    entries: Vec<(f32, u32)>,
    k: usize,
    min_idx: usize,
}

impl TopK {
    fn new(k: usize) -> Self {
        debug_assert!(k > 0);
        Self {
            entries: Vec::with_capacity(k),
            k,
            min_idx: 0,
        }
    }

    /// Score below which a candidate cannot enter the top-k.
    fn threshold(&self) -> f32 {
        if self.entries.len() < self.k {
            f32::NEG_INFINITY
        } else {
            self.entries[self.min_idx].0
        }
    }

    fn push(&mut self, score: f32, id: u32) {
        if self.entries.len() < self.k {
            self.entries.push((score, id));
            if self.entries.len() == self.k {
                self.refresh_min();
            }
        } else if score > self.entries[self.min_idx].0 {
            self.entries[self.min_idx] = (score, id);
            self.refresh_min();
        }
    }

    fn refresh_min(&mut self) {
        self.min_idx = self
            .entries
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.partial_cmp(b.1).expect("scores are not NaN"))
            .map(|(idx, _)| idx)
            .unwrap_or(0);
    }
}

/// Per-group lookup tables of one query, quantized to 7 bits.
///
/// For every dim group `g` there are two 16-entry half-tables over the
/// nibble values of the group's low dim pair and high dim pair:
/// `T[nib] = q[d0] · centroid[nib & 3] + q[d1] · centroid[(nib >> 2) & 3]`.
/// Each half-table is shifted by its minimum (the minima summed into `bias`)
/// and scaled by a global `scale` so entries fit `0..=127`. The raw dot of a
/// vector reconstructs as `bias + scale · Σ entries`.
pub struct QueryLut2bit {
    /// `n_groups * 32` bytes: per group, 16 low-pair entries then 16
    /// high-pair entries — matching one 32-byte kernel load.
    luts: Vec<u8>,
    scale: f32,
    bias: f32,
    n_groups: usize,
}

impl QueryLut2bit {
    /// Build the tables from a query already rotated into the quantizer's
    /// space (`TurboQuantizer::precompute_query`'s rotation), `padded_dim`
    /// values long.
    ///
    /// `bias_correction` is a constant added to every raw dot before the
    /// scaling factor — TQ+ callers pass the query's `ec_correction`
    /// (`⟨Q, M⟩`) so a scanned score reconstructs
    /// `(raw_dot + ec_correction) · scaling_factor`, the same formula the
    /// production Dot/Cosine path applies. Pass `0.0` otherwise.
    pub fn new(rotated_query: &[f32], bias_correction: f32) -> Self {
        assert_eq!(rotated_query.len() % 4, 0, "2-bit dim groups hold 4 dims");
        let n_groups = rotated_query.len() / 4;
        let centroids = TQBits::Bits2.get_centroids();

        // First pass: float half-tables, per-half-table minima, global span.
        let mut tables = vec![0.0f32; n_groups * 32];
        let mut mins = vec![0.0f32; n_groups * 2];
        let mut widest_span = 0.0f32;
        for g in 0..n_groups {
            for t in 0..2 {
                let d0 = 4 * g + 2 * t;
                let q0 = rotated_query[d0];
                let q1 = rotated_query[d0 + 1];
                let mut min = f32::INFINITY;
                let mut max = f32::NEG_INFINITY;
                for nib in 0..16 {
                    let value = q0 * centroids[nib & 3] + q1 * centroids[(nib >> 2) & 3];
                    tables[g * 32 + t * 16 + nib] = value;
                    min = min.min(value);
                    max = max.max(value);
                }
                mins[g * 2 + t] = min;
                widest_span = widest_span.max(max - min);
            }
        }

        // Second pass: min-shift and quantize with one global scale.
        let scale = if widest_span > f32::EPSILON {
            widest_span / LUT_ENTRY_MAX
        } else {
            1.0
        };
        let bias: f32 = bias_correction + mins.iter().sum::<f32>();
        let inv_scale = 1.0 / scale;
        let luts: Vec<u8> = tables
            .iter()
            .enumerate()
            .map(|(idx, &value)| {
                let min = mins[idx / 16];
                ((value - min) * inv_scale)
                    .round()
                    .clamp(0.0, LUT_ENTRY_MAX) as u8
            })
            .collect();

        Self {
            luts,
            scale,
            bias,
            n_groups,
        }
    }
}

/// Scalar reference: scores `count` row-major records against `lut` with the
/// same integer LUT arithmetic as the SIMD kernel, but straight from the
/// records (no transposed layout) — an independent path for parity tests.
pub fn score_all_scalar(
    records: &[u8],
    stride: usize,
    codes_len: usize,
    padded_dim: usize,
    count: usize,
    lut: &QueryLut2bit,
) -> Vec<f32> {
    let n_groups = padded_dim / 4;
    assert_eq!(lut.n_groups, n_groups, "query/storage dim mismatch");
    (0..count)
        .map(|v| {
            let record = &records[v * stride..(v + 1) * stride];
            let codes = read_codes(&record[..codes_len], padded_dim);
            let sf = f32::from_le_bytes(
                record[codes_len..codes_len + size_of::<f32>()]
                    .try_into()
                    .expect("record too short for a scaling factor"),
            );
            let mut sum = 0u32;
            for g in 0..n_groups {
                let nib_lo = (codes[4 * g] | (codes[4 * g + 1] << 2)) as usize;
                let nib_hi = (codes[4 * g + 2] | (codes[4 * g + 3] << 2)) as usize;
                sum += u32::from(lut.luts[g * 32 + nib_lo]);
                sum += u32::from(lut.luts[g * 32 + 16 + nib_hi]);
            }
            (lut.bias + lut.scale * sum as f32) * sf
        })
        .collect()
}

#[cfg(target_arch = "x86_64")]
mod x64 {
    use core::arch::x86_64::*;

    use super::{
        BLOCK_VECTORS, Blocked2bit, QueryLut2bit, TopK, U16_ACC_GROUPS, lane_id, lane_slot,
    };

    /// Scores of one block, in lane order (`out[r]` lane `i` is block vector
    /// [`lane_id`]`(r, i)`), already multiplied by the per-vector scaling
    /// factors.
    ///
    /// # Safety
    /// CPU must support `avx2` and `fma`; `block_codes` must hold
    /// `n_groups * 32` bytes, `luts` `n_groups * 32` bytes and `scales` 32
    /// floats.
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn score_block(
        block_codes: *const u8,
        luts: *const u8,
        n_groups: usize,
        lut_scale: f32,
        lut_bias: f32,
        scales: *const f32,
    ) -> [__m256; 4] {
        let mask_low4 = _mm256_set1_epi8(0x0f);
        let scale = _mm256_set1_ps(lut_scale);
        // One f32 accumulator lane per vector; the bias is per vector, so it
        // seeds the accumulator once per block, not once per flush.
        let mut facc = [_mm256_set1_ps(lut_bias); 4];

        let mut g = 0;
        while g < n_groups {
            let flush_end = (g + U16_ACC_GROUPS).min(n_groups);
            let mut acc = [_mm256_setzero_si256(); 4];
            while g < flush_end {
                let codes =
                    unsafe { _mm256_loadu_si256(block_codes.add(g * 32).cast::<__m256i>()) };
                let lut = unsafe { _mm256_loadu_si256(luts.add(g * 32).cast::<__m256i>()) };
                let lo_nibbles = _mm256_and_si256(codes, mask_low4);
                let hi_nibbles = _mm256_and_si256(_mm256_srli_epi16::<4>(codes), mask_low4);
                let r0 = _mm256_shuffle_epi8(lut, lo_nibbles);
                let r1 = _mm256_shuffle_epi8(lut, hi_nibbles);
                // Even byte positions accumulate as `even + 256 · odd` in
                // acc[0]/acc[2]; the odd-only acc[1]/acc[3] undo the carry-in
                // at flush time (exact modulo 2^16).
                acc[0] = _mm256_add_epi16(acc[0], r0);
                acc[1] = _mm256_add_epi16(acc[1], _mm256_srli_epi16::<8>(r0));
                acc[2] = _mm256_add_epi16(acc[2], r1);
                acc[3] = _mm256_add_epi16(acc[3], _mm256_srli_epi16::<8>(r1));
                g += 1;
            }
            acc[0] = _mm256_sub_epi16(acc[0], _mm256_slli_epi16::<8>(acc[1]));
            acc[2] = _mm256_sub_epi16(acc[2], _mm256_slli_epi16::<8>(acc[3]));
            for (acc, facc) in acc.iter().zip(facc.iter_mut()) {
                // Lane k of the low half is a vector's low-pair half-table
                // total, lane k of the high half the same vector's high-pair
                // total; their u16 sum is ≤ 65 024 (see module docs).
                let lo = _mm256_castsi256_si128(*acc);
                let hi = _mm256_extracti128_si256::<1>(*acc);
                let sums = _mm_add_epi16(lo, hi);
                let f = _mm256_cvtepi32_ps(_mm256_cvtepu16_epi32(sums));
                *facc = _mm256_fmadd_ps(scale, f, *facc);
            }
        }

        let mut out = [_mm256_setzero_ps(); 4];
        for (r, out) in out.iter_mut().enumerate() {
            let sf = unsafe { _mm256_loadu_ps(scales.add(r * 8)) };
            *out = _mm256_mul_ps(facc[r], sf);
        }
        out
    }

    /// Scores of the contiguous id range `start_id..start_id + out.len()`,
    /// written to `out` in id order. Partially-covered boundary blocks are
    /// scored whole and the surplus lanes discarded.
    ///
    /// # Safety
    /// CPU must support `avx2` and `fma`; the range must be non-empty and
    /// lie within `vectors.count`.
    #[target_feature(enable = "avx2,fma")]
    pub(super) unsafe fn score_range(
        vectors: &Blocked2bit,
        lut: &QueryLut2bit,
        start_id: usize,
        out: &mut [f32],
    ) {
        debug_assert!(!out.is_empty());
        let end_id = start_id + out.len();
        debug_assert!(end_id <= vectors.count);
        for b in start_id / BLOCK_VECTORS..=(end_id - 1) / BLOCK_VECTORS {
            let scores = unsafe { scores_for_block(vectors, lut, b) };
            let mut lanes = [0.0f32; BLOCK_VECTORS];
            for (r, scores) in scores.iter().enumerate() {
                unsafe { _mm256_storeu_ps(lanes.as_mut_ptr().add(r * 8), *scores) };
            }
            let covered_from = start_id.max(b * BLOCK_VECTORS);
            let covered_to = end_id.min((b + 1) * BLOCK_VECTORS);
            for id in covered_from..covered_to {
                out[id - start_id] = lanes[lane_slot(id % BLOCK_VECTORS)];
            }
        }
    }

    /// # Safety
    /// CPU must support `avx2` and `fma`.
    #[target_feature(enable = "avx2,fma")]
    pub(super) unsafe fn scan_top_k(vectors: &Blocked2bit, lut: &QueryLut2bit, top: &mut TopK) {
        let mut threshold = top.threshold();
        let mut threshold_v = _mm256_set1_ps(threshold);
        for b in 0..vectors.n_blocks {
            let out = unsafe { scores_for_block(vectors, lut, b) };
            for (r, scores) in out.iter().enumerate() {
                // Gate the scalar heap maintenance on one SIMD compare: in a
                // full scan almost every 8-lane group loses to the current
                // k-th best and is skipped wholesale.
                let mask = _mm256_movemask_ps(_mm256_cmp_ps::<_CMP_GT_OQ>(*scores, threshold_v));
                if mask == 0 {
                    continue;
                }
                let mut lanes = [0.0f32; 8];
                unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), *scores) };
                for (i, &score) in lanes.iter().enumerate() {
                    if mask & (1 << i) == 0 {
                        continue;
                    }
                    let id = b * BLOCK_VECTORS + lane_id(r, i);
                    // Padding lanes score 0.0 (scaling factor 0); their ids
                    // are past `count` and must not enter the heap.
                    if id < vectors.count {
                        top.push(score, id as u32);
                    }
                }
                let new_threshold = top.threshold();
                if new_threshold > threshold {
                    threshold = new_threshold;
                    threshold_v = _mm256_set1_ps(threshold);
                }
            }
        }
    }

    /// [`score_block`] over block `b` of `vectors`.
    ///
    /// # Safety
    /// CPU must support `avx2` and `fma`; `b < vectors.n_blocks`.
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn scores_for_block(vectors: &Blocked2bit, lut: &QueryLut2bit, b: usize) -> [__m256; 4] {
        debug_assert!(b < vectors.n_blocks);
        unsafe {
            score_block(
                vectors
                    .blocked
                    .as_ptr()
                    .add(b * vectors.n_groups * BLOCK_VECTORS),
                lut.luts.as_ptr(),
                vectors.n_groups,
                lut.scale,
                lut.bias,
                vectors.scales.as_ptr().add(b * BLOCK_VECTORS),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use common::bitpacking::BitWriter;
    use rand::prelude::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::DistanceType;
    use crate::turboquant::quantization::TurboQuantizer;
    use crate::turboquant::rotation::HadamardRotation;
    use crate::turboquant::{TQMode, TQRotation};

    /// Row-major record: 2-bit `codes` packed LSB-first (the production
    /// `BitWriter` layout) followed by the `f32` scaling factor.
    fn make_record(codes: &[u8], sf: f32) -> Vec<u8> {
        let mut out = Vec::new();
        let mut writer = BitWriter::new(&mut out);
        for &c in codes {
            writer.write(c, 2);
        }
        writer.finish();
        out.extend_from_slice(&sf.to_le_bytes());
        out
    }

    fn random_records(
        rng: &mut StdRng,
        padded_dim: usize,
        count: usize,
    ) -> (Vec<u8>, Vec<Vec<u8>>, Vec<f32>, usize, usize) {
        let mut records = Vec::new();
        let mut all_codes = Vec::new();
        let mut sfs = Vec::new();
        let mut stride = 0;
        for _ in 0..count {
            let codes: Vec<u8> = (0..padded_dim).map(|_| rng.random_range(0..4u8)).collect();
            let sf = rng.random_range(0.5..2.0f32);
            let record = make_record(&codes, sf);
            stride = record.len();
            records.extend_from_slice(&record);
            all_codes.push(codes);
            sfs.push(sf);
        }
        let codes_len = stride - size_of::<f32>();
        (records, all_codes, sfs, stride, codes_len)
    }

    fn random_query(rng: &mut StdRng, padded_dim: usize) -> Vec<f32> {
        (0..padded_dim)
            .map(|_| rng.random_range(-1.0..1.0))
            .collect()
    }

    /// Exact (float) raw LUT-free dot: `Σ q[d] · centroid[code[d]]`.
    fn exact_raw_dot(query: &[f32], codes: &[u8]) -> f32 {
        let centroids = TQBits::Bits2.get_centroids();
        query
            .iter()
            .zip(codes)
            .map(|(&q, &c)| q * centroids[c as usize])
            .sum()
    }

    /// The pack must place every nibble at the documented byte position and
    /// permute the scaling factors by `lane_slot`.
    #[test]
    fn pack_layout_tiny() {
        let padded_dim = 4; // one dim group
        let count = 32; // one full block
        let all_codes: Vec<Vec<u8>> = (0..count)
            .map(|w| {
                vec![
                    (w % 4) as u8,
                    ((w >> 2) % 4) as u8,
                    ((w + 1) % 4) as u8,
                    ((w + 3) % 4) as u8,
                ]
            })
            .collect();
        let mut records = Vec::new();
        let mut stride = 0;
        for (w, codes) in all_codes.iter().enumerate() {
            let record = make_record(codes, w as f32);
            stride = record.len();
            records.extend_from_slice(&record);
        }
        let codes_len = stride - size_of::<f32>();

        let blocked = Blocked2bit::pack(&records, stride, codes_len, padded_dim, count);

        assert_eq!(blocked.n_groups, 1);
        assert_eq!(blocked.n_blocks, 1);
        assert_eq!(blocked.blocked.len(), 32);

        let nib_lo = |w: usize| all_codes[w][0] | (all_codes[w][1] << 2);
        let nib_hi = |w: usize| all_codes[w][2] | (all_codes[w][3] << 2);
        for j in 0..16 {
            assert_eq!(
                blocked.blocked[j],
                nib_lo(j) | (nib_lo(16 + j) << 4),
                "low-pair byte {j}"
            );
            assert_eq!(
                blocked.blocked[16 + j],
                nib_hi(j) | (nib_hi(16 + j) << 4),
                "high-pair byte {j}"
            );
        }

        for w in 0..count {
            assert_eq!(
                blocked.scales[lane_slot(w)],
                w as f32,
                "scaling factor of vector {w}"
            );
        }
    }

    /// The 7-bit LUT quantization error per half-table entry is at most
    /// `scale / 2`, so the scalar scorer must stay within
    /// `2 · n_groups · scale / 2` of the exact float dot.
    #[test]
    fn scalar_lut_error_bounded() {
        let padded_dim = 64;
        let n_groups = padded_dim / 4;
        let count = 64;
        let mut rng = StdRng::seed_from_u64(7);

        // Unit scaling factors keep the bound clean.
        let mut records = Vec::new();
        let mut all_codes = Vec::new();
        let mut stride = 0;
        for _ in 0..count {
            let codes: Vec<u8> = (0..padded_dim).map(|_| rng.random_range(0..4u8)).collect();
            let record = make_record(&codes, 1.0);
            stride = record.len();
            records.extend_from_slice(&record);
            all_codes.push(codes);
        }
        let codes_len = stride - size_of::<f32>();

        let query = random_query(&mut rng, padded_dim);
        let lut = QueryLut2bit::new(&query, 0.0);
        let scores = score_all_scalar(&records, stride, codes_len, padded_dim, count, &lut);

        let bound = 2.0 * n_groups as f32 * lut.scale / 2.0 * 1.01 + 1e-4;
        for (v, &score) in scores.iter().enumerate() {
            let exact = exact_raw_dot(&query, &all_codes[v]);
            assert!(
                (score - exact).abs() <= bound,
                "vector {v}: LUT score {score} vs exact {exact}, bound {bound}"
            );
        }
    }

    /// The AVX2 kernel must reproduce the scalar reference (same integer
    /// sums; the float epilogue may differ by rounding only) across group
    /// counts below, at, and above the flush interval, and across partial
    /// tail blocks.
    #[test]
    fn avx2_matches_scalar() {
        if !is_supported() {
            eprintln!("skipped: no AVX2+FMA");
            return;
        }
        let mut rng = StdRng::seed_from_u64(42);
        // (padded_dim, count): 16 groups; 192 groups; exactly U16_ACC_GROUPS
        // groups; 384 groups (two flushes); partial blocks of every shape.
        for &(padded_dim, count) in &[
            (64usize, 33usize),
            (256, 31),
            (768, 100),
            (1024, 64),
            (1536, 65),
        ] {
            let (records, _, _, stride, codes_len) = random_records(&mut rng, padded_dim, count);
            let query = random_query(&mut rng, padded_dim);
            let lut = QueryLut2bit::new(&query, 0.0);

            let blocked = Blocked2bit::pack(&records, stride, codes_len, padded_dim, count);
            let simd = blocked.score_all(&lut);
            let scalar = score_all_scalar(&records, stride, codes_len, padded_dim, count, &lut);

            assert_eq!(simd.len(), count);
            for v in 0..count {
                // `bias` and `scale · sum` may cancel, so allow rounding
                // noise relative to their magnitudes, not the result's.
                let tol = (lut.bias.abs() + scalar[v].abs()) * 1e-5 + 1e-6;
                assert!(
                    (simd[v] - scalar[v]).abs() <= tol,
                    "dim {padded_dim}, count {count}, vector {v}: \
                     SIMD {} vs scalar {} (tol {tol})",
                    simd[v],
                    scalar[v]
                );
            }
        }
    }

    /// The fused top-k scan must return exactly the k best of `score_all`.
    #[test]
    fn top_k_matches_score_all() {
        if !is_supported() {
            eprintln!("skipped: no AVX2+FMA");
            return;
        }
        let padded_dim = 768;
        let count = 1000;
        let k = 10;
        let mut rng = StdRng::seed_from_u64(3);
        let (records, _, _, stride, codes_len) = random_records(&mut rng, padded_dim, count);
        let query = random_query(&mut rng, padded_dim);
        let lut = QueryLut2bit::new(&query, 0.0);
        let blocked = Blocked2bit::pack(&records, stride, codes_len, padded_dim, count);

        let mut expected: Vec<(f32, u32)> = blocked
            .score_all(&lut)
            .into_iter()
            .enumerate()
            .map(|(id, score)| (score, id as u32))
            .collect();
        expected.sort_by(|a, b| b.partial_cmp(a).unwrap());
        expected.truncate(k);

        let got = blocked.scan_top_k(&lut, k);
        assert_eq!(got, expected);
    }

    /// End to end against the real quantizer: pack real records, build the
    /// LUT from the same rotation `precompute_query` applies, and check the
    /// LUT scores stay within the deterministic quantization bound of the
    /// exact centroid dot (scaled per vector). Catches any wiring mismatch
    /// with production encoding (bit order, rotation, extras layout).
    #[test]
    fn matches_turboquant_exact_reference() {
        if !is_supported() {
            eprintln!("skipped: no AVX2+FMA");
            return;
        }
        let dim = 256;
        let count = 512;
        let mut rng = StdRng::seed_from_u64(11);

        let tq = TurboQuantizer::new(
            dim,
            TQBits::Bits2,
            TQMode::Normal,
            DistanceType::Dot,
            TQRotation::Padded,
            None,
        );
        let padded_dim = tq.get_padded_dim();
        let stride = tq.quantized_size();
        let codes_len = stride - size_of::<f32>();

        let mut buf = vec![0.0f64; padded_dim];
        let mut records = Vec::new();
        for _ in 0..count {
            let vector: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
            records.extend_from_slice(&tq.quantize(&vector, &mut buf));
        }

        // Rotate the query exactly as `precompute_query` does.
        let query: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
        let mut rotated: Vec<f64> = query.iter().map(|&x| f64::from(x)).collect();
        rotated.resize(padded_dim, 0.0);
        HadamardRotation::new(padded_dim).apply(&mut rotated);
        let rotated_f32: Vec<f32> = rotated.iter().map(|&x| x as f32).collect();

        let lut = QueryLut2bit::new(&rotated_f32, 0.0);
        let blocked = Blocked2bit::pack(&records, stride, codes_len, padded_dim, count);
        let scores = blocked.score_all(&lut);

        let n_groups = padded_dim / 4;
        let raw_bound = 2.0 * n_groups as f32 * lut.scale / 2.0 * 1.01 + 1e-4;
        for v in 0..count {
            let record = &records[v * stride..(v + 1) * stride];
            let (centroid_values, extras) = tq.unpack_vector(record);
            let exact_raw: f64 = centroid_values
                .zip(rotated.iter())
                .map(|(c, &q)| c * q)
                .sum();
            let sf = extras.scaling_factor();
            let expected = exact_raw as f32 * sf;
            let bound = raw_bound * sf.abs();
            assert!(
                (scores[v] - expected).abs() <= bound,
                "vector {v}: LUT {} vs exact {expected}, bound {bound}",
                scores[v]
            );
        }
    }

    /// `bias_correction` must shift every score by exactly `correction · sf`
    /// — the TQ+ `ec_correction` contract.
    #[test]
    fn bias_correction_shifts_scores() {
        let padded_dim = 32;
        let count = 5;
        let correction = 1.25f32;
        let mut rng = StdRng::seed_from_u64(17);
        let (records, _, sfs, stride, codes_len) = random_records(&mut rng, padded_dim, count);
        let query = random_query(&mut rng, padded_dim);

        let plain = QueryLut2bit::new(&query, 0.0);
        let corrected = QueryLut2bit::new(&query, correction);
        let scores_plain = score_all_scalar(&records, stride, codes_len, padded_dim, count, &plain);
        let scores_corrected =
            score_all_scalar(&records, stride, codes_len, padded_dim, count, &corrected);

        for v in 0..count {
            let expected = scores_plain[v] + correction * sfs[v];
            assert!(
                (scores_corrected[v] - expected).abs() <= expected.abs() * 1e-5 + 1e-5,
                "vector {v}: corrected {} vs expected {expected}",
                scores_corrected[v]
            );
        }
    }

    /// `pack_records` over per-record fetches must produce the identical
    /// shadow as `pack` over the contiguous slice.
    #[test]
    fn pack_records_matches_pack() {
        let padded_dim = 64;
        let count = 45; // partial tail block
        let mut rng = StdRng::seed_from_u64(23);
        let (records, _, _, stride, codes_len) = random_records(&mut rng, padded_dim, count);

        let contiguous = Blocked2bit::pack(&records, stride, codes_len, padded_dim, count);
        let fetched = Blocked2bit::pack_records(count, codes_len, padded_dim, |v| {
            records[v * stride..(v + 1) * stride].to_vec()
        });

        assert_eq!(fetched.blocked, contiguous.blocked);
        assert_eq!(fetched.scales, contiguous.scales);
        assert_eq!(fetched.n_groups, contiguous.n_groups);
        assert_eq!(fetched.n_blocks, contiguous.n_blocks);
        assert_eq!(fetched.count, contiguous.count);
        assert!(fetched.heap_size_bytes() >= fetched.blocked.len() + fetched.scales.len() * 4);
    }

    /// `score_range` must reproduce the matching `score_all` slice for
    /// aligned and unaligned ranges, including block-crossing ones.
    #[test]
    fn score_range_matches_score_all() {
        if !is_supported() {
            eprintln!("skipped: no AVX2+FMA");
            return;
        }
        let padded_dim = 128;
        let count = 100;
        let mut rng = StdRng::seed_from_u64(29);
        let (records, _, _, stride, codes_len) = random_records(&mut rng, padded_dim, count);
        let query = random_query(&mut rng, padded_dim);
        let lut = QueryLut2bit::new(&query, 0.0);
        let blocked = Blocked2bit::pack(&records, stride, codes_len, padded_dim, count);
        let all = blocked.score_all(&lut);

        for &(start, len) in &[
            (0usize, 100usize),
            (0, 1),
            (31, 2),
            (32, 32),
            (40, 50),
            (99, 1),
            (5, 64),
        ] {
            let mut out = vec![0.0f32; len];
            blocked.score_range(&lut, start, &mut out);
            assert_eq!(
                out,
                all[start..start + len],
                "range ({start}, {len}) diverges from score_all"
            );
        }
    }
}
