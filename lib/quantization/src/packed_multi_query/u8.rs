//! Batched MaxSim for scalar-quantized (int8) multi-vectors.
//!
//! The query is packed so that SIMD lanes hold query tokens, four code bytes
//! per lane:
//!
//! ```text
//! data[((block * dim / 4 + k4) * BLOCK + lane) * 4 + i] = query[block * BLOCK + lane][k4 * 4 + i]
//! ```
//!
//! Each stored vector is then folded in with one broadcast of four of its code
//! bytes per step, multiplied against [`BLOCK`] query tokens at once (`maddubs`
//! for dot products, absolute differences for L1). The integer sums are exact
//! and the postprocess runs in the same operation order as
//! [`EncodedVectorsU8`](crate::EncodedVectorsU8), so scores are bit-identical
//! to the per-pair path.

use common::ambient::hw::HwMetric;

use super::BLOCK;
use crate::encoded_vectors_u8::ALIGNMENT;

/// Bytes of the `f32` offset that precedes the codes of every stored vector.
pub(crate) const OFFSET_SIZE: usize = size_of::<f32>();

pub struct PackedMultiQueryU8 {
    data: Vec<u8>,
    /// Per query token offset, padded with zeros to whole blocks.
    offsets: Vec<f32>,
    vectors_count: usize,
    /// Code bytes per vector, a multiple of 4.
    code_size: usize,
    multiplier: f32,
    /// Sum of absolute differences instead of the dot product.
    l1: bool,
    /// Hardware counter units per pushed vector (`0` to count nothing).
    cpu_per_vector: usize,
}

impl PackedMultiQueryU8 {
    /// `queries` yields `(codes, offset)` per query token; codes are at least
    /// `code_size` bytes in `0..=127`. Scores are
    /// `multiplier · Σ + offset + vector_offset`.
    pub(crate) fn new<'a>(
        queries: impl ExactSizeIterator<Item = (&'a [u8], f32)>,
        code_size: usize,
        multiplier: f32,
        l1: bool,
        cpu_per_vector: usize,
    ) -> Self {
        // Scalar codes are padded to `ALIGNMENT` (16) bytes: four steps of four codes,
        // one per accumulator chain of the AVX-512 kernel.
        assert!(code_size.is_multiple_of(ALIGNMENT));
        let vectors_count = queries.len();
        let blocks = vectors_count.div_ceil(BLOCK);
        let mut data = vec![0u8; blocks * code_size * BLOCK];
        let mut offsets = vec![0.0; blocks * BLOCK];
        for (token, (codes, offset)) in queries.enumerate() {
            let (block, lane) = (token / BLOCK, token % BLOCK);
            offsets[token] = offset;
            for (k4, chunk) in codes[..code_size].as_chunks::<4>().0.iter().enumerate() {
                let at = ((block * (code_size / 4) + k4) * BLOCK + lane) * 4;
                data[at..at + 4].copy_from_slice(chunk);
            }
        }
        Self {
            data,
            offsets,
            vectors_count,
            code_size,
            multiplier,
            l1,
            cpu_per_vector,
        }
    }

    pub fn vectors_count(&self) -> usize {
        self.vectors_count
    }

    pub(crate) fn lanes(&self) -> usize {
        self.offsets.len()
    }

    /// Fold one stored vector, its `f32` offset followed by its codes, into the
    /// running per-token maxima.
    pub(crate) fn push(&self, maxima: &mut [f32], vector: &[u8]) {
        assert!(vector.len() >= OFFSET_SIZE + self.code_size);
        assert!(maxima.len() >= self.lanes());
        if self.cpu_per_vector > 0 {
            HwMetric::Cpu.bump(self.cpu_per_vector * self.vectors_count);
        }
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vl")
            && std::is_x86_feature_detected!("avx512vnni")
        {
            // SAFETY: AVX-512 F, BW, VL and VNNI are available; the lengths are checked
            // above.
            unsafe {
                if self.l1 {
                    x86::push_avx512::<true>(self, maxima, vector);
                } else {
                    x86::push_avx512::<false>(self, maxima, vector);
                }
            }
            return;
        }
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 is available and the lengths are checked above.
            unsafe {
                if self.l1 {
                    x86::push_avx2::<true>(self, maxima, vector);
                } else {
                    x86::push_avx2::<false>(self, maxima, vector);
                }
            }
            return;
        }
        self.push_portable(maxima, vector);
    }

    fn push_portable(&self, maxima: &mut [f32], vector: &[u8]) {
        let vector_offset = f32::from_ne_bytes(vector[..OFFSET_SIZE].try_into().unwrap());
        let codes = &vector[OFFSET_SIZE..OFFSET_SIZE + self.code_size];
        let steps = self.code_size / 4;
        for block in 0..self.lanes() / BLOCK {
            let packed = &self.data[block * steps * BLOCK * 4..][..steps * BLOCK * 4];
            let mut acc = [0i32; BLOCK];
            for (k4, x) in codes.as_chunks::<4>().0.iter().enumerate() {
                let q = &packed[k4 * BLOCK * 4..][..BLOCK * 4];
                for (lane, acc) in acc.iter_mut().enumerate() {
                    for i in 0..4 {
                        let (q, x) = (i32::from(q[lane * 4 + i]), i32::from(x[i]));
                        *acc += if self.l1 { q.abs_diff(x) as i32 } else { q * x };
                    }
                }
            }
            let offsets = &self.offsets[block * BLOCK..][..BLOCK];
            let maxima = &mut maxima[block * BLOCK..][..BLOCK];
            for lane in 0..BLOCK {
                // `multiplier * score + query_offset + vector_offset`, the order of
                // `MetadataInt8::postprocess_score`.
                let score = self.multiplier * acc[lane] as f32 + offsets[lane] + vector_offset;
                if maxima[lane] < score {
                    maxima[lane] = score;
                }
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{BLOCK, OFFSET_SIZE, PackedMultiQueryU8};

    /// Independent accumulator chains per block in the AVX-512 kernels: one
    /// chain waits out `vpdpbusd`'s latency on every step.
    const CHAINS: usize = 4;

    /// [`push_avx2`] with one 16-lane register per block: `vpdpbusd` multiplies
    /// four code bytes and adds the products in one instruction. A last block of
    /// at most 8 tokens uses one 8-lane register.
    ///
    /// # Safety
    /// CPU must support AVX-512 F, BW, VL and VNNI; `vector` holds the offset and
    /// `code_size` codes, `maxima` holds `query.lanes()` values.
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,avx512vnni")]
    pub(super) unsafe fn push_avx512<const L1: bool>(
        query: &PackedMultiQueryU8,
        maxima: &mut [f32],
        vector: &[u8],
    ) {
        unsafe {
            let vector_offset = _mm512_set1_ps(vector.as_ptr().cast::<f32>().read_unaligned());
            let codes = vector.as_ptr().add(OFFSET_SIZE);
            let steps = query.code_size / 4;
            let multiplier = _mm512_set1_ps(query.multiplier);
            for block in 0..query.lanes() / BLOCK {
                let packed = query.data.as_ptr().add(block * steps * BLOCK * 4);
                if query.vectors_count - block * BLOCK <= 8 {
                    let mut sums = [_mm256_setzero_si256(); CHAINS];
                    for k4 in (0..steps).step_by(CHAINS) {
                        for (chain, sum) in sums.iter_mut().enumerate() {
                            *sum = step_avx512_half::<L1>(*sum, codes, packed, k4 + chain);
                        }
                    }
                    let acc = _mm256_add_epi32(
                        _mm256_add_epi32(sums[0], sums[1]),
                        _mm256_add_epi32(sums[2], sums[3]),
                    );
                    finish8(query, maxima, block * BLOCK, acc, vector);
                    continue;
                }
                // Integer sums, so splitting them over chains changes nothing. `steps`
                // is a multiple of `CHAINS`, as `code_size` is of 16.
                let mut sums = [_mm512_setzero_si512(); CHAINS];
                for k4 in (0..steps).step_by(CHAINS) {
                    for (chain, sum) in sums.iter_mut().enumerate() {
                        *sum = step_avx512::<L1>(*sum, codes, packed, k4 + chain);
                    }
                }
                let acc = _mm512_add_epi32(
                    _mm512_add_epi32(sums[0], sums[1]),
                    _mm512_add_epi32(sums[2], sums[3]),
                );
                let score = _mm512_add_ps(
                    _mm512_add_ps(
                        _mm512_mul_ps(multiplier, _mm512_cvtepi32_ps(acc)),
                        _mm512_loadu_ps(query.offsets.as_ptr().add(block * BLOCK)),
                    ),
                    vector_offset,
                );
                let max = maxima.as_mut_ptr().add(block * BLOCK);
                _mm512_storeu_ps(max, _mm512_max_ps(score, _mm512_loadu_ps(max)));
            }
        }
    }

    /// Four codes of the stored vector against four codes of 16 query tokens.
    ///
    /// # Safety
    /// CPU must support AVX-512 F, BW and VNNI; `codes` holds step `k4` and
    /// `packed` its 16 lanes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn step_avx512<const L1: bool>(
        acc: __m512i,
        codes: *const u8,
        packed: *const u8,
        k4: usize,
    ) -> __m512i {
        // SAFETY: by this function's contract.
        let (x, q) = unsafe {
            (
                _mm512_set1_epi32(codes.add(k4 * 4).cast::<i32>().read_unaligned()),
                _mm512_loadu_si512(packed.add(k4 * BLOCK * 4).cast()),
            )
        };
        if L1 {
            let d = _mm512_sub_epi8(_mm512_max_epu8(x, q), _mm512_min_epu8(x, q));
            _mm512_dpbusd_epi32(acc, d, _mm512_set1_epi8(1))
        } else {
            // Unsigned stored codes times signed query codes, both in 0..=127.
            _mm512_dpbusd_epi32(acc, x, q)
        }
    }

    /// [`step_avx512`] for the first 8 query tokens of a block.
    ///
    /// # Safety
    /// CPU must support AVX-512 VL and VNNI; `codes` holds step `k4` and `packed`
    /// its 16 lanes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,avx512vnni")]
    unsafe fn step_avx512_half<const L1: bool>(
        acc: __m256i,
        codes: *const u8,
        packed: *const u8,
        k4: usize,
    ) -> __m256i {
        // SAFETY: by this function's contract.
        let (x, q) = unsafe {
            (
                _mm256_set1_epi32(codes.add(k4 * 4).cast::<i32>().read_unaligned()),
                _mm256_loadu_si256(packed.add(k4 * BLOCK * 4).cast()),
            )
        };
        if L1 {
            let d = _mm256_sub_epi8(_mm256_max_epu8(x, q), _mm256_min_epu8(x, q));
            _mm256_dpbusd_epi32(acc, d, _mm256_set1_epi8(1))
        } else {
            _mm256_dpbusd_epi32(acc, x, q)
        }
    }

    /// # Safety
    /// CPU must support AVX2; `vector` holds the offset and `code_size` codes,
    /// `maxima` holds `query.lanes()` values.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn push_avx2<const L1: bool>(
        query: &PackedMultiQueryU8,
        maxima: &mut [f32],
        vector: &[u8],
    ) {
        for block in 0..query.lanes() / BLOCK {
            // SAFETY: as this function's contract.
            unsafe { push_block_avx2::<L1>(query, maxima, vector, block) };
        }
    }

    /// One block of [`push_avx2`].
    ///
    /// # Safety
    /// As [`push_avx2`]; `block` is a block of `query`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn push_block_avx2<const L1: bool>(
        query: &PackedMultiQueryU8,
        maxima: &mut [f32],
        vector: &[u8],
        block: usize,
    ) {
        // SAFETY: as this function's contract.
        unsafe {
            let codes = vector.as_ptr().add(OFFSET_SIZE);
            // A last block of at most 8 tokens uses one register.
            if query.vectors_count - block * BLOCK <= 8 {
                let [acc] = block_avx2::<L1, 1>(query, block, codes);
                finish8(query, maxima, block * BLOCK, acc, vector);
            } else {
                let [acc0, acc1] = block_avx2::<L1, 2>(query, block, codes);
                finish8(query, maxima, block * BLOCK, acc0, vector);
                finish8(query, maxima, block * BLOCK + 8, acc1, vector);
            }
        }
    }

    /// Fold the integer sums of 8 lanes from `lane` into their maxima.
    ///
    /// # Safety
    /// CPU must support AVX2; `vector` holds the offset, `lane + 8` lanes exist.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn finish8(
        query: &PackedMultiQueryU8,
        maxima: &mut [f32],
        lane: usize,
        acc: __m256i,
        vector: &[u8],
    ) {
        // SAFETY: by this function's contract.
        unsafe {
            let vector_offset = _mm256_set1_ps(vector.as_ptr().cast::<f32>().read_unaligned());
            // `multiplier * score + query_offset + vector_offset`, same order as
            // `MetadataInt8::postprocess_score`.
            let score = _mm256_add_ps(
                _mm256_add_ps(
                    _mm256_mul_ps(_mm256_set1_ps(query.multiplier), _mm256_cvtepi32_ps(acc)),
                    _mm256_loadu_ps(query.offsets.as_ptr().add(lane)),
                ),
                vector_offset,
            );
            // `max(score, maximum)` keeps the maximum unless the score is larger,
            // like `if maximum < score`.
            let max = maxima.as_mut_ptr().add(lane);
            _mm256_storeu_ps(max, _mm256_max_ps(score, _mm256_loadu_ps(max)));
        }
    }

    /// Integer sums of the first `HALVES` x 8 query tokens of one block.
    ///
    /// # Safety
    /// CPU must support AVX2; `codes` holds `code_size` bytes.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn block_avx2<const L1: bool, const HALVES: usize>(
        query: &PackedMultiQueryU8,
        block: usize,
        codes: *const u8,
    ) -> [__m256i; HALVES] {
        let steps = query.code_size / 4;
        let ones_u8 = _mm256_set1_epi8(1);
        let ones = _mm256_set1_epi16(1);
        let mut acc = [_mm256_setzero_si256(); HALVES];
        // SAFETY: `steps` steps of 16 lanes per block, and `code_size` codes.
        unsafe {
            let packed = query.data.as_ptr().add(block * steps * BLOCK * 4);
            for k4 in 0..steps {
                // Four codes of the stored vector against four codes of 8 query tokens
                // per register.
                let x = _mm256_set1_epi32(codes.add(k4 * 4).cast::<i32>().read_unaligned());
                for (h, acc) in acc.iter_mut().enumerate() {
                    let q = _mm256_loadu_si256(packed.add(k4 * BLOCK * 4 + h * 32).cast());
                    let products = if L1 {
                        // `|x - q|` per byte, then pairs of bytes summed against ones.
                        let d = _mm256_or_si256(_mm256_subs_epu8(x, q), _mm256_subs_epu8(q, x));
                        _mm256_maddubs_epi16(d, ones_u8)
                    } else {
                        // `maddubs` takes the stored codes as unsigned and the query codes as
                        // signed; both are in 0..=127, and a pair of products fits in i16.
                        _mm256_maddubs_epi16(x, q)
                    };
                    *acc = _mm256_add_epi32(*acc, _mm256_madd_epi16(products, ones));
                }
            }
        }
        acc
    }
}
