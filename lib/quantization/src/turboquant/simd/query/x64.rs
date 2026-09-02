//! x86_64 kernels for [`QuerySimd`].
//!
//! The codebook is stored unsigned (`Encoding::codebook` as `u8`), which
//! is what `maddubs` / `VPDPBUSD` expect in their `u8` operand slot; the
//! signed shift `c_signed = c_u − offset` is undone by the per-query
//! `bias_correction`.  See the module docs of [`super`] for the query
//! ranges that keep the `maddubs` pair sums inside i16.

use core::arch::x86_64::*;

use super::{Code, PLANE_BLOCK, QueryPlanes, QuerySimd, encoding, tail_block};

/// Packed data bytes per SSE block: one XMM of codes.
const BLOCK_128: usize = 16;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_128));

/// Packed data bytes per AVX2 block: one YMM of codes.
const BLOCK_256: usize = 32;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_256));

/// Packed data bytes per AVX-512 block: one ZMM of codes.
const BLOCK_512: usize = 64;
const _: () = assert!(PLANE_BLOCK.is_multiple_of(BLOCK_512));

/// The codebook as a shuffle table for the width packing `PLANES` codes per
/// byte.
const fn codebook<const PLANES: usize>() -> [Code; 16] {
    encoding(PLANES).codebook
}

/// Mask of one code in the low bits of a byte.
const fn code_mask<const PLANES: usize>() -> i8 {
    ((1u16 << (8 / PLANES)) - 1) as i8
}

/// One [`BLOCK_128`]-byte block of every query plane.
#[derive(Clone, Copy)]
struct QueryBlock128<const PLANES: usize, const QUERY_BYTES: usize> {
    bytes: [[__m128i; PLANES]; QUERY_BYTES],
}

impl<const PLANES: usize, const QUERY_BYTES: usize> QueryBlock128<PLANES, QUERY_BYTES> {
    /// # Safety
    /// CPU must support `sse2`; `offset + BLOCK_128` must not exceed the
    /// plane length.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn load(planes: &QueryPlanes<PLANES, QUERY_BYTES>, offset: usize) -> Self {
        let mut block = Self {
            bytes: [[_mm_setzero_si128(); PLANES]; QUERY_BYTES],
        };
        for (b, byte_planes) in block.bytes.iter_mut().enumerate() {
            for (k, plane) in byte_planes.iter_mut().enumerate() {
                *plane = unsafe { load_plane_128(&planes.bytes[b][k], offset) };
            }
        }
        block
    }
}

/// # Safety
/// CPU must support `sse2`; `offset + BLOCK_128 <= plane.len()`.
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn load_plane_128(plane: &[i8], offset: usize) -> __m128i {
    debug_assert!(offset + BLOCK_128 <= plane.len());
    unsafe { _mm_loadu_si128(plane.as_ptr().add(offset).cast::<__m128i>()) }
}

/// [`next_plane_512`] on XMM.
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn next_plane_128<const PLANES: usize>(codes: __m128i) -> __m128i {
    match PLANES {
        2 => _mm_srli_epi16(codes, 4),
        4 => _mm_srli_epi16(codes, 2),
        _ => _mm_srli_epi16(codes, 1),
    }
}

/// `maddubs → madd` accumulators of one vector; the 128-bit form of
/// [`Acc256`], with the same integer bounds.
#[derive(Clone, Copy)]
struct Acc128<const QUERY_BYTES: usize> {
    bytes: [[__m128i; 2]; QUERY_BYTES],
}

impl<const QUERY_BYTES: usize> Acc128<QUERY_BYTES> {
    /// # Safety
    /// CPU must support `sse2`.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn zero() -> Self {
        Self {
            bytes: [[_mm_setzero_si128(); 2]; QUERY_BYTES],
        }
    }

    /// Fold one block of packed `codes` (16 bytes) into the accumulators.
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[inline]
    #[target_feature(enable = "sse4.1,ssse3")]
    unsafe fn accumulate<const PLANES: usize>(
        &mut self,
        codes: __m128i,
        query: QueryBlock128<PLANES, QUERY_BYTES>,
    ) {
        let table = const { codebook::<PLANES>() };
        let codebook = unsafe { _mm_loadu_si128(table.as_ptr().cast::<__m128i>()) };
        let mask = _mm_set1_epi8(const { code_mask::<PLANES>() });
        let ones = _mm_set1_epi16(1);
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = _mm_shuffle_epi8(codebook, _mm_and_si128(shifted, mask));
            for (acc, planes) in self.bytes.iter_mut().zip(&query.bytes) {
                acc[k & 1] = _mm_add_epi32(
                    acc[k & 1],
                    _mm_madd_epi16(_mm_maddubs_epi16(values, planes[k]), ones),
                );
            }
            shifted = unsafe { next_plane_128::<PLANES>(shifted) };
        }
    }

    /// Per-lane totals of every query byte.
    ///
    /// # Safety
    /// CPU must support `sse2`.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn fold(self) -> [__m128i; QUERY_BYTES] {
        self.bytes.map(|[a, b]| _mm_add_epi32(a, b))
    }
}

/// One [`BLOCK_256`]-byte block of every query plane.
#[derive(Clone, Copy)]
struct QueryBlock256<const PLANES: usize, const QUERY_BYTES: usize> {
    bytes: [[__m256i; PLANES]; QUERY_BYTES],
}

impl<const PLANES: usize, const QUERY_BYTES: usize> QueryBlock256<PLANES, QUERY_BYTES> {
    /// # Safety
    /// CPU must support `avx2`; `offset + BLOCK_256` must not exceed the
    /// plane length.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn load(planes: &QueryPlanes<PLANES, QUERY_BYTES>, offset: usize) -> Self {
        let mut block = Self {
            bytes: [[_mm256_setzero_si256(); PLANES]; QUERY_BYTES],
        };
        for (b, byte_planes) in block.bytes.iter_mut().enumerate() {
            for (k, plane) in byte_planes.iter_mut().enumerate() {
                *plane = unsafe { load_plane_256(&planes.bytes[b][k], offset) };
            }
        }
        block
    }
}

/// # Safety
/// CPU must support `avx2`; `offset + BLOCK_256 <= plane.len()`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn load_plane_256(plane: &[i8], offset: usize) -> __m256i {
    debug_assert!(offset + BLOCK_256 <= plane.len());
    unsafe { _mm256_loadu_si256(plane.as_ptr().add(offset).cast::<__m256i>()) }
}

/// [`next_plane_512`] on YMM.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn next_plane_256<const PLANES: usize>(codes: __m256i) -> __m256i {
    match PLANES {
        2 => _mm256_srli_epi16(codes, 4),
        4 => _mm256_srli_epi16(codes, 2),
        _ => _mm256_srli_epi16(codes, 1),
    }
}

/// `maddubs → madd` accumulators of one vector; the same two-pair shape as
/// [`Acc512`].
///
/// The `maddubs` pair sums stay inside i16 by the module-level query bound
/// (`|pair| ≤ 2 · 255 · 64 = 32 640`, or exactly `2 · 128 · 128 = 32 768`
/// at the negative end for the 1-bit encoding); `madd` against ones then
/// adds at most 65 280 (65 536) per i32 lane per plane, the same bound as
/// VNNI.
#[derive(Clone, Copy)]
struct Acc256<const QUERY_BYTES: usize> {
    bytes: [[__m256i; 2]; QUERY_BYTES],
}

impl<const QUERY_BYTES: usize> Acc256<QUERY_BYTES> {
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn zero() -> Self {
        Self {
            bytes: [[_mm256_setzero_si256(); 2]; QUERY_BYTES],
        }
    }

    /// Fold one block of packed `codes` (32 bytes) into the accumulators.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn accumulate<const PLANES: usize>(
        &mut self,
        codes: __m256i,
        query: QueryBlock256<PLANES, QUERY_BYTES>,
    ) {
        let table = const { codebook::<PLANES>() };
        let codebook = _mm256_broadcastsi128_si256(unsafe {
            _mm_loadu_si128(table.as_ptr().cast::<__m128i>())
        });
        let mask = _mm256_set1_epi8(const { code_mask::<PLANES>() });
        let ones = _mm256_set1_epi16(1);
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = _mm256_shuffle_epi8(codebook, _mm256_and_si256(shifted, mask));
            for (acc, planes) in self.bytes.iter_mut().zip(&query.bytes) {
                acc[k & 1] = _mm256_add_epi32(
                    acc[k & 1],
                    _mm256_madd_epi16(_mm256_maddubs_epi16(values, planes[k]), ones),
                );
            }
            shifted = unsafe { next_plane_256::<PLANES>(shifted) };
        }
    }

    /// Per-lane totals of every query byte.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn fold(self) -> [__m256i; QUERY_BYTES] {
        self.bytes.map(|[a, b]| _mm256_add_epi32(a, b))
    }
}

/// One [`BLOCK_512`]-byte block of every query plane.
#[derive(Clone, Copy)]
struct QueryBlock512<const PLANES: usize, const QUERY_BYTES: usize> {
    bytes: [[__m512i; PLANES]; QUERY_BYTES],
}

impl<const PLANES: usize, const QUERY_BYTES: usize> QueryBlock512<PLANES, QUERY_BYTES> {
    /// # Safety
    /// CPU must support `avx512f`; `offset + BLOCK_512` must not exceed the
    /// plane length.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn load(planes: &QueryPlanes<PLANES, QUERY_BYTES>, offset: usize) -> Self {
        let mut block = Self {
            bytes: [[_mm512_setzero_si512(); PLANES]; QUERY_BYTES],
        };
        for (b, byte_planes) in block.bytes.iter_mut().enumerate() {
            for (k, plane) in byte_planes.iter_mut().enumerate() {
                *plane = unsafe { load_plane_512(&planes.bytes[b][k], offset) };
            }
        }
        block
    }
}

/// # Safety
/// CPU must support `avx512f`; `offset + BLOCK_512 <= plane.len()`.
#[inline]
#[target_feature(enable = "avx512f")]
unsafe fn load_plane_512(plane: &[i8], offset: usize) -> __m512i {
    debug_assert!(offset + BLOCK_512 <= plane.len());
    unsafe { _mm512_loadu_si512(plane.as_ptr().add(offset).cast::<__m512i>()) }
}

/// The next plane's codes moved into the low bits of every byte: `codes >>
/// bits` within 16-bit lanes, so the caller masks off what crosses over
/// from the neighboring byte.  The shift count is an immediate, hence the
/// match over the widths.
#[inline]
#[target_feature(enable = "avx512bw")]
unsafe fn next_plane_512<const PLANES: usize>(codes: __m512i) -> __m512i {
    match PLANES {
        2 => _mm512_srli_epi16(codes, 4),
        4 => _mm512_srli_epi16(codes, 2),
        _ => _mm512_srli_epi16(codes, 1),
    }
}

/// `VPDPBUSD` accumulators of one vector: per query byte two independent
/// chains, plane `k` feeding chain `k & 1`.  Two chains per byte are
/// enough to keep the multiply-accumulate latency off the critical path at
/// every width (`PLANES / 2` dependent steps per block against `PLANES`
/// issued per byte).
///
/// i32 lane bound: each `VPDPBUSD` adds at most `4 · 255 · 64 = 65 280`
/// (`4 · 128 · 128 = 65 536` for the 1-bit encoding) to a lane, so overflow
/// needs ~32 K blocks ≈ 2 M packed bytes — far beyond any real input.
#[derive(Clone, Copy)]
struct Acc512<const QUERY_BYTES: usize> {
    bytes: [[__m512i; 2]; QUERY_BYTES],
}

impl<const QUERY_BYTES: usize> Acc512<QUERY_BYTES> {
    /// # Safety
    /// CPU must support `avx512f`.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn zero() -> Self {
        Self {
            bytes: [[_mm512_setzero_si512(); 2]; QUERY_BYTES],
        }
    }

    /// Fold one block of packed `codes` (64 bytes) into the accumulators:
    /// plane by plane, the codes are shifted down, masked to one code per
    /// byte and looked up in the codebook.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn accumulate<const PLANES: usize>(
        &mut self,
        codes: __m512i,
        query: QueryBlock512<PLANES, QUERY_BYTES>,
    ) {
        let table = const { codebook::<PLANES>() };
        let codebook =
            _mm512_broadcast_i32x4(unsafe { _mm_loadu_si128(table.as_ptr().cast::<__m128i>()) });
        let mask = _mm512_set1_epi8(const { code_mask::<PLANES>() });
        let mut shifted = codes;
        for k in 0..PLANES {
            let values = _mm512_shuffle_epi8(codebook, _mm512_and_si512(shifted, mask));
            for (acc, planes) in self.bytes.iter_mut().zip(&query.bytes) {
                acc[k & 1] = _mm512_dpbusd_epi32(acc[k & 1], values, planes[k]);
            }
            shifted = unsafe { next_plane_512::<PLANES>(shifted) };
        }
    }

    /// Per-lane totals of every query byte.
    ///
    /// # Safety
    /// CPU must support `avx512f`.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn fold(self) -> [__m512i; QUERY_BYTES] {
        self.bytes.map(|[a, b]| _mm512_add_epi32(a, b))
    }
}

impl<const PLANES: usize, const QUERY_BYTES: usize> QuerySimd<PLANES, QUERY_BYTES> {
    /// x86_64 SSE4.1 + SSSE3 over the query planes: one XMM of packed codes
    /// per block, the 128-bit form of the AVX2 kernel.
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`.
    #[target_feature(enable = "sse4.1,ssse3")]
    pub unsafe fn dotprod_raw_sse(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}, {QUERY_BYTES}>::dotprod_raw_sse: vector length mismatch ({} vs \
             expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe {
            let totals = self.accumulate_sse(vector.as_ptr()).fold();
            Self::combine_bytes(totals.map(|total| i64::from(hsum_i32_sse(total))))
        }
    }

    /// Block loop of the SSE kernels over the vector at `data`.
    ///
    /// # Safety
    /// CPU must support `ssse3` and `sse4.1`; `data` must be readable for
    /// `self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "sse4.1,ssse3")]
    unsafe fn accumulate_sse(&self, data: *const u8) -> Acc128<QUERY_BYTES> {
        unsafe {
            let mut acc = Acc128::zero();

            let full_blocks = self.vector_bytes / BLOCK_128;
            for block in 0..full_blocks {
                let offset = block * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                let codes = _mm_loadu_si128(data.add(offset).cast::<__m128i>());
                acc.accumulate(codes, query);
            }

            let tail = self.vector_bytes % BLOCK_128;
            if tail > 0 {
                let offset = full_blocks * BLOCK_128;
                let query = QueryBlock128::load(&self.planes, offset);
                let block = tail_block::<BLOCK_128>(data.add(offset), tail);
                let codes = _mm_loadu_si128(block.as_ptr().cast::<__m128i>());
                acc.accumulate(codes, query);
            }

            acc
        }
    }

    /// x86_64 AVX2 over the query planes: one YMM of packed codes per
    /// block, one `vpshufb` codebook lookup per plane and `maddubs → madd`
    /// products into independent accumulators (see [`Acc256`]).  The last
    /// partial block runs on a zero-padded copy of the remaining bytes.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_raw_avx2(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}, {QUERY_BYTES}>::dotprod_raw_avx2: vector length mismatch ({} \
             vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe { self.reduce_avx2(self.accumulate_avx2(vector.as_ptr())) }
    }

    /// Batch counterpart of [`Self::dotprod_raw_avx2`] with the float
    /// reconstruction applied — see `dotprod_batch` for the layout contract
    /// (asserted there).
    ///
    /// Unlike the VNNI kernel this one walks the block loop one vector at a
    /// time: its loop-carried chain is a single `vpaddd` per accumulator
    /// (the `maddubs → madd` products hang off the loads), so the
    /// accumulator pairs already keep the pipeline full, and interleaving
    /// vectors only adds register pressure on the 16 YMM registers —
    /// measured 10–15 % slower with groups of two or four on Zen 4 at the
    /// 4-bit width.
    ///
    /// The horizontal reduction is shared across [`REDUCE_GROUP_256`]
    /// vectors instead: each vector's accumulators are folded to four i64
    /// lanes right after its block loop, and the group's lanes are
    /// transposed into one YMM of sums that is bias-corrected, converted and
    /// stored four-wide.  The per-vector serial chain (fold, extract, widen,
    /// scalar convert, multiply, store) retired in order behind the next
    /// vector's block loop and cost about a third of the kernel's cycles;
    /// sharing it measured 10–25 % faster on Zen 3, bit-identical.
    ///
    /// # Safety
    /// CPU must support `avx2`; `data` must hold `out.len()` vectors at
    /// `stride`.
    #[target_feature(enable = "avx2")]
    pub unsafe fn dotprod_batch_avx2(&self, data: &[u8], stride: usize, out: &mut [f32]) {
        unsafe {
            let (groups, rest) = out.as_chunks_mut::<REDUCE_GROUP_256>();
            let mut v = 0;
            for group in groups {
                let mut lanes = [_mm256_setzero_si256(); REDUCE_GROUP_256];
                for lane in &mut lanes {
                    let acc = self.accumulate_avx2(data.as_ptr().add(v * stride));
                    *lane = self.widen_avx2(acc);
                    v += 1;
                }
                let scores = self.postprocess_x4_avx2(transpose_sum_4x64(lanes));
                _mm_storeu_ps(group.as_mut_ptr(), scores);
            }
            for out in rest {
                let acc = self.accumulate_avx2(data.as_ptr().add(v * stride));
                *out = self.postprocess(self.reduce_avx2(acc));
                v += 1;
            }
        }
    }

    /// One vector's accumulators folded to four i64 lanes whose sum is the
    /// raw dot product: the fused i32 fold whenever the vector is short
    /// enough for its lane bound, otherwise each query byte widened on its
    /// own and combined in i64.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn widen_avx2(&self, acc: Acc256<QUERY_BYTES>) -> __m256i {
        unsafe { self.widen_totals_256(acc.fold()) }
    }

    /// [`Self::widen_avx2`] on the per-byte lane totals.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn widen_totals_256(&self, totals: [__m256i; QUERY_BYTES]) -> __m256i {
        unsafe {
            if self.vector_bytes <= Self::FUSED_REDUCTION_MAX_BYTES {
                return widen_i64_256(combine_fused_256::<PLANES, QUERY_BYTES>(totals));
            }
            let mut combined = widen_i64_256(totals[0]);
            if QUERY_BYTES == 2 {
                let high = widen_i64_256(totals[1]);
                let scaled = match const { encoding(PLANES).query_high_coef } {
                    128 => _mm256_slli_epi64(high, 7),
                    _ => _mm256_slli_epi64(high, 8),
                };
                combined = _mm256_add_epi64(combined, scaled);
            }
            combined
        }
    }

    /// [`Self::postprocess`] on four raw dot products at once.  The i64 →
    /// f64 step goes through the exponent trick (add `1.5 · 2^52` as an
    /// integer, subtract it as a double), exact for `|x| < 2^51`; a raw dot
    /// product is bounded by `dim · 8127 · 255`, so that holds to dims past
    /// `10^9`.  f64 → f32 then rounds once, to nearest even, exactly like
    /// the scalar `as f32`.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn postprocess_x4_avx2(&self, raw: __m256i) -> __m128 {
        let bias = _mm256_set1_epi64x(self.bias_correction);
        let diff = _mm256_sub_epi64(raw, bias);
        let magic = _mm256_set1_epi64x(0x4338_0000_0000_0000);
        let as_f64 = _mm256_sub_pd(
            _mm256_castsi256_pd(_mm256_add_epi64(diff, magic)),
            _mm256_castsi256_pd(magic),
        );
        _mm_mul_ps(_mm256_cvtpd_ps(as_f64), _mm_set1_ps(self.postprocess_scale))
    }

    /// Block loop of the AVX2 kernels over the vector at `data`.
    ///
    /// # Safety
    /// CPU must support `avx2`; `data` must be readable for
    /// `self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn accumulate_avx2(&self, data: *const u8) -> Acc256<QUERY_BYTES> {
        unsafe {
            let mut acc = Acc256::zero();

            let full_blocks = self.vector_bytes / BLOCK_256;
            for block in 0..full_blocks {
                let offset = block * BLOCK_256;
                let query = QueryBlock256::load(&self.planes, offset);
                let codes = _mm256_loadu_si256(data.add(offset).cast::<__m256i>());
                acc.accumulate(codes, query);
            }

            let tail = self.vector_bytes % BLOCK_256;
            if tail > 0 {
                let offset = full_blocks * BLOCK_256;
                let query = QueryBlock256::load(&self.planes, offset);
                let block = tail_block::<BLOCK_256>(data.add(offset), tail);
                let codes = _mm256_loadu_si256(block.as_ptr().cast::<__m256i>());
                acc.accumulate(codes, query);
            }

            acc
        }
    }

    /// Raw dot product from one vector's accumulators: the fused reduction
    /// whenever the vector is short enough for its lane bound, otherwise
    /// one reduction per query byte.
    ///
    /// # Safety
    /// CPU must support `avx2`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn reduce_avx2(&self, acc: Acc256<QUERY_BYTES>) -> i64 {
        unsafe {
            let totals = acc.fold();
            if self.vector_bytes <= Self::FUSED_REDUCTION_MAX_BYTES {
                reduce_fused_256::<PLANES, QUERY_BYTES>(totals)
            } else {
                Self::combine_bytes(totals.map(|total| i64::from(hsum_i32_avx2(total))))
            }
        }
    }

    /// AVX-512 VNNI (Ice Lake Xeon+, Zen 4+) over the query planes: one ZMM
    /// of packed codes per block, one `vpshufb` codebook lookup per plane
    /// and `VPDPBUSD` into independent accumulators (see [`Acc512`]).  The
    /// last partial block is a masked load whose dead lanes multiply
    /// against the planes' zero padding.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dotprod_raw_avx512_vnni(&self, vector: &[u8]) -> i64 {
        assert_eq!(
            vector.len(),
            self.vector_bytes,
            "QuerySimd<{PLANES}, {QUERY_BYTES}>::dotprod_raw_avx512_vnni: vector length \
             mismatch ({} vs expected {})",
            vector.len(),
            self.vector_bytes,
        );

        unsafe {
            let [acc] = self.accumulate_avx512::<1>(vector.as_ptr(), 0);
            self.reduce_avx512(acc)
        }
    }

    /// Batch counterpart of [`Self::dotprod_raw_avx512_vnni`] with the float
    /// reconstruction applied — see `dotprod_batch` for the layout contract
    /// (asserted there).
    ///
    /// Vectors up to [`INTERLEAVE_MAX_BYTES`] are scored in groups of
    /// [`GROUP_512`]: the group shares each query block load and its tail
    /// mask, and its independent accumulators keep `VPDPBUSD` saturated.
    /// Longer vectors walk the block loop one vector at a time: the group
    /// reads [`GROUP_512`] interleaved byte streams, which hardware
    /// prefetchers stream far worse than a single sequential one once each
    /// vector spans more than a couple of cache lines.
    ///
    /// Either way the reduction is shared across the group as in
    /// [`Self::dotprod_batch_avx2`]: each vector's accumulators are folded
    /// to four i64 lanes, the group's lanes are transposed into one YMM of
    /// sums, bias-corrected, converted and stored four-wide.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`; `data` must
    /// hold `out.len()` vectors at `stride`.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dotprod_batch_avx512_vnni(&self, data: &[u8], stride: usize, out: &mut [f32]) {
        unsafe {
            let (groups, rest) = out.as_chunks_mut::<GROUP_512>();
            let interleave = self.vector_bytes <= INTERLEAVE_MAX_BYTES;
            let mut v = 0;
            for group in groups {
                let accs = if interleave {
                    self.accumulate_avx512::<GROUP_512>(data.as_ptr().add(v * stride), stride)
                } else {
                    let mut accs = [Acc512::zero(); GROUP_512];
                    for (i, acc) in accs.iter_mut().enumerate() {
                        let [single] = self
                            .accumulate_avx512::<1>(data.as_ptr().add((v + i) * stride), stride);
                        *acc = single;
                    }
                    accs
                };
                // Reduce the group together only for a one-byte query. With two
                // bytes each vector carries twice the accumulator state, so
                // holding all four until the transpose spills the register file
                // and costs more than sharing the reduction saves.
                if QUERY_BYTES == 1 {
                    let lanes = accs.map(|acc| self.widen_avx512(acc));
                    let scores = self.postprocess_x4_avx2(transpose_sum_4x64(lanes));
                    _mm_storeu_ps(group.as_mut_ptr(), scores);
                } else {
                    for (out, acc) in group.iter_mut().zip(accs) {
                        *out = self.postprocess(self.reduce_avx512(acc));
                    }
                }
                v += GROUP_512;
            }
            for out in rest {
                let [acc] = self.accumulate_avx512::<1>(data.as_ptr().add(v * stride), stride);
                *out = self.postprocess(self.reduce_avx512(acc));
                v += 1;
            }
        }
    }

    /// [`Self::widen_avx2`] for ZMM accumulators: each total is folded to
    /// 256 bits first, which halves the lanes and doubles their values —
    /// the per-byte bound behind [`fused_reduction_max_bytes`] is unchanged.
    ///
    /// # Safety
    /// CPU must support `avx512f`.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn widen_avx512(&self, acc: Acc512<QUERY_BYTES>) -> __m256i {
        unsafe {
            let totals = acc.fold().map(|total| {
                _mm256_add_epi32(
                    _mm512_castsi512_si256(total),
                    _mm512_extracti64x4_epi64(total, 1),
                )
            });
            self.widen_totals_256(totals)
        }
    }

    /// Block loop of the AVX-512 kernels over `N` vectors stored `stride`
    /// bytes apart starting at `data`.  Every query block is loaded once and
    /// folded into all `N` accumulator sets.
    ///
    /// # Safety
    /// CPU must support `avx512f`, `avx512bw`, and `avx512vnni`; `data` must
    /// be readable for `(N - 1) * stride + self.vector_bytes` bytes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    unsafe fn accumulate_avx512<const N: usize>(
        &self,
        data: *const u8,
        stride: usize,
    ) -> [Acc512<QUERY_BYTES>; N] {
        unsafe {
            let mut accs = [Acc512::zero(); N];

            let full_blocks = self.vector_bytes / BLOCK_512;
            for block in 0..full_blocks {
                let offset = block * BLOCK_512;
                let query = QueryBlock512::load(&self.planes, offset);
                for (v, acc) in accs.iter_mut().enumerate() {
                    let codes = _mm512_loadu_si512(data.add(v * stride + offset).cast::<__m512i>());
                    acc.accumulate(codes, query);
                }
            }

            let tail = self.vector_bytes % BLOCK_512;
            if tail > 0 {
                let offset = full_blocks * BLOCK_512;
                let query = QueryBlock512::load(&self.planes, offset);
                let mask: __mmask64 = (1 << tail) - 1;
                for (v, acc) in accs.iter_mut().enumerate() {
                    let codes =
                        _mm512_maskz_loadu_epi8(mask, data.add(v * stride + offset).cast::<i8>());
                    acc.accumulate(codes, query);
                }
            }

            accs
        }
    }

    /// Raw dot product from one vector's accumulators: the fused reduction
    /// whenever the vector is short enough for its lane bound, otherwise
    /// one reduction per query byte.
    ///
    /// # Safety
    /// CPU must support `avx512f` and `avx512bw`.
    #[inline]
    #[target_feature(enable = "avx512f,avx512bw")]
    unsafe fn reduce_avx512(&self, acc: Acc512<QUERY_BYTES>) -> i64 {
        unsafe {
            let totals = acc.fold();
            if self.vector_bytes <= Self::FUSED_REDUCTION_MAX_BYTES {
                reduce_fused_512::<PLANES, QUERY_BYTES>(totals)
            } else {
                Self::combine_bytes(totals.map(|total| i64::from(_mm512_reduce_add_epi32(total))))
            }
        }
    }

    /// Longest encoded vector (bytes) the fused reduction accepts — see
    /// [`fused_reduction_max_bytes`].
    const FUSED_REDUCTION_MAX_BYTES: usize = fused_reduction_max_bytes::<PLANES, QUERY_BYTES>();
}

/// Vectors per interleaved group of the AVX-512 batch kernel: 4 × 4
/// accumulators plus the query block, codebook and mask fit the 32 ZMM
/// registers without spilling.  Equal to [`REDUCE_GROUP_256`] so one
/// interleaved group feeds one shared reduction.
const GROUP_512: usize = REDUCE_GROUP_256;

/// Longest encoded vector (bytes) the AVX-512 batch kernel scores in
/// interleaved groups: four cache lines per vector.  Measured streaming
/// from DRAM on Zen 4 at the 4-bit width: grouping is 10 % faster than the
/// per-vector walk at dim 512 and twice as slow at dim 1024.
const INTERLEAVE_MAX_BYTES: usize = 4 * BLOCK_512;

/// Longest encoded vector (bytes) [`reduce_fused_256`] accepts at a width.
///
/// Per packed byte, its `PLANES` codes add at most `PLANES · c_max · K/2`
/// to the folded lane of each query byte holding it, so `(1 + K)` times
/// that to the fused lane (`low + K · high`).  The reduction folds to four
/// i32 lanes before widening, each holding a quarter of the bytes:
/// `bytes / 4 · per_byte ≤ i32::MAX`.  A one-byte query has no `K · high`
/// term and cannot overflow the fold within any real dim.
const fn fused_reduction_max_bytes<const PLANES: usize, const QUERY_BYTES: usize>() -> usize {
    if QUERY_BYTES == 1 {
        return usize::MAX;
    }
    let encoding = encoding(PLANES);
    let mut c_max = 0;
    let mut k = 0;
    while k < (1 << (8 / PLANES)) {
        if encoding.codebook[k] as usize > c_max {
            c_max = encoding.codebook[k] as usize;
        }
        k += 1;
    }
    let radix = encoding.query_high_coef as usize;
    let per_byte = PLANES * c_max * (radix / 2) * (1 + radix);
    4 * (i32::MAX as usize) / per_byte
}

/// Vectors per shared reduction of the AVX2 batch kernel: four vectors'
/// i64 lanes transpose into one YMM of sums.
const REDUCE_GROUP_256: usize = 4;

/// `Σ_b K^b · Σ totals[b]` with the horizontal reductions fused into one:
/// the high lanes are shifted by `log2(K)` and added to the low lanes up
/// front, leaving a single tree reduction that widens to i64 before its
/// last two adds.
///
/// # Safety
/// CPU must support `avx2`; the totals must come from a vector within the
/// width's [`fused_reduction_max_bytes`].
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn reduce_fused_256<const PLANES: usize, const QUERY_BYTES: usize>(
    totals: [__m256i; QUERY_BYTES],
) -> i64 {
    unsafe {
        let wide = widen_i64_256(combine_fused_256::<PLANES, QUERY_BYTES>(totals));
        let pair = _mm_add_epi64(
            _mm256_castsi256_si128(wide),
            _mm256_extracti128_si256(wide, 1),
        );
        let total = _mm_add_epi64(pair, _mm_unpackhi_epi64(pair, pair));
        _mm_cvtsi128_si64(total)
    }
}

/// The i32 lanes of `Σ_b K^b · totals[b]`: the high byte's lanes shifted by
/// `log2(K)` and added to the low byte's.
///
/// # Safety
/// CPU must support `avx2`; the totals must come from a vector within the
/// width's [`fused_reduction_max_bytes`].
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn combine_fused_256<const PLANES: usize, const QUERY_BYTES: usize>(
    totals: [__m256i; QUERY_BYTES],
) -> __m256i {
    let mut combined = totals[0];
    if QUERY_BYTES == 2 {
        // The radix is a power of two (128 or 256); the shift is an immediate.
        let high = totals[1];
        let scaled = match const { encoding(PLANES).query_high_coef } {
            128 => _mm256_slli_epi32(high, 7),
            _ => _mm256_slli_epi32(high, 8),
        };
        combined = _mm256_add_epi32(combined, scaled);
    }
    combined
}

/// Eight i32 lanes folded to four and sign-extended to i64.
///
/// # Safety
/// CPU must support `avx2`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn widen_i64_256(lanes: __m256i) -> __m256i {
    let fold_128 = _mm_add_epi32(
        _mm256_castsi256_si128(lanes),
        _mm256_extracti128_si256(lanes, 1),
    );
    _mm256_cvtepi32_epi64(fold_128)
}

/// The lane sums of four i64 vectors, one per output lane: `unpack` pairs
/// the lanes of `a`/`b` and `c`/`d` within each 128-bit half, the 128-bit
/// permutes then bring the halves together.
///
/// # Safety
/// CPU must support `avx2`.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn transpose_sum_4x64([a, b, c, d]: [__m256i; 4]) -> __m256i {
    let ab = _mm256_add_epi64(_mm256_unpacklo_epi64(a, b), _mm256_unpackhi_epi64(a, b));
    let cd = _mm256_add_epi64(_mm256_unpacklo_epi64(c, d), _mm256_unpackhi_epi64(c, d));
    _mm256_add_epi64(
        _mm256_permute2x128_si256(ab, cd, 0x20),
        _mm256_permute2x128_si256(ab, cd, 0x31),
    )
}

/// [`reduce_fused_256`] for ZMM accumulators: folds each to 256 bits first,
/// which halves the lanes and doubles their values — the per-byte bound
/// behind [`fused_reduction_max_bytes`] is unchanged.
///
/// # Safety
/// CPU must support `avx512f`; the totals must come from a vector within
/// the width's [`fused_reduction_max_bytes`].
#[inline]
#[target_feature(enable = "avx512f")]
unsafe fn reduce_fused_512<const PLANES: usize, const QUERY_BYTES: usize>(
    totals: [__m512i; QUERY_BYTES],
) -> i64 {
    unsafe {
        reduce_fused_256::<PLANES, QUERY_BYTES>(totals.map(|total| {
            _mm256_add_epi32(
                _mm512_castsi512_si256(total),
                _mm512_extracti64x4_epi64(total, 1),
            )
        }))
    }
}

#[target_feature(enable = "sse2")]
unsafe fn hsum_i32_sse(v: __m128i) -> i32 {
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0x4E));
    let v = _mm_add_epi32(v, _mm_shuffle_epi32(v, 0xB1));
    _mm_cvtsi128_si32(v)
}

#[inline]
#[target_feature(enable = "avx2")]
unsafe fn hsum_i32_avx2(v: __m256i) -> i32 {
    unsafe {
        hsum_i32_sse(_mm_add_epi32(
            _mm256_castsi256_si128(v),
            _mm256_extracti128_si256(v, 1),
        ))
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use rand::prelude::StdRng;

    use super::super::super::shared::random_bytes;
    use super::super::QuerySimd;
    use super::super::shared::{parity_dims, random_inputs};

    fn has_sse() -> bool {
        std::is_x86_feature_detected!("ssse3") && std::is_x86_feature_detected!("sse4.1")
    }

    fn has_avx512_vnni() -> bool {
        std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512bw")
            && std::is_x86_feature_detected!("avx512vnni")
    }

    /// Every kernel the host supports must reproduce the scalar reference
    /// bit-exactly at every parity dim.
    fn kernels_match_scalar<const PLANES: usize, const QUERY_BYTES: usize>() {
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>() {
            let (query, vector) = random_inputs::<PLANES, QUERY_BYTES>(&mut rng, dim);
            let scalar = query.dotprod_raw(&vector);
            let tag = format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES} dim={dim}");
            unsafe {
                if has_sse() {
                    let sse = query.dotprod_raw_sse(&vector);
                    assert_eq!(scalar, sse, "{tag}: scalar {scalar} != sse {sse}");
                }
                if std::is_x86_feature_detected!("avx2") {
                    let avx2 = query.dotprod_raw_avx2(&vector);
                    assert_eq!(scalar, avx2, "{tag}: scalar {scalar} != avx2 {avx2}");
                }
                if has_avx512_vnni() {
                    let vnni512 = query.dotprod_raw_avx512_vnni(&vector);
                    assert_eq!(
                        scalar, vnni512,
                        "{tag}: scalar {scalar} != avx512_vnni {vnni512}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_kernels_match_scalar() {
        kernels_match_scalar::<2, 2>();
        kernels_match_scalar::<4, 2>();
        kernels_match_scalar::<8, 1>();
        kernels_match_scalar::<8, 2>();
    }

    /// Saturation safety at an extreme dim (64K) under the worst-case load:
    /// the query maxed out and every code at the max-magnitude codebook
    /// slot (all-ones bytes at every width).  The scalar reference is i64
    /// throughout; a SIMD mismatch proves some intermediate saturated or
    /// overflowed.
    fn saturation_safety_64k<const PLANES: usize, const QUERY_BYTES: usize>() {
        let dim = 65_536;
        let query = QuerySimd::<PLANES, QUERY_BYTES>::new(&vec![1.0_f32; dim]);
        let vector = vec![0xFF_u8; dim / PLANES];
        let scalar = query.dotprod_raw(&vector);
        let tag = format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES}");
        unsafe {
            if has_sse() {
                let sse = query.dotprod_raw_sse(&vector);
                assert_eq!(scalar, sse, "{tag}: sse disagrees");
            }
            if std::is_x86_feature_detected!("avx2") {
                let avx2 = query.dotprod_raw_avx2(&vector);
                assert_eq!(scalar, avx2, "{tag}: avx2 disagrees");
            }
            if has_avx512_vnni() {
                let vnni512 = query.dotprod_raw_avx512_vnni(&vector);
                assert_eq!(scalar, vnni512, "{tag}: avx512_vnni disagrees");
            }
        }
    }

    #[test]
    fn test_saturation_safety_64k() {
        saturation_safety_64k::<2, 2>();
        saturation_safety_64k::<4, 2>();
        saturation_safety_64k::<8, 1>();
        saturation_safety_64k::<8, 2>();
    }

    /// The derivation behind the fused-reduction bound, checked against
    /// the hand-derived 4-bit value: `2 · 255 · 64 · 129 = 4 210 560` per
    /// byte, `4 · i32::MAX / 4 210 560 = 2040`.
    #[test]
    fn test_fused_reduction_bound() {
        assert_eq!(QuerySimd::<2, 2>::FUSED_REDUCTION_MAX_BYTES, 2040);
        assert_eq!(QuerySimd::<4, 2>::FUSED_REDUCTION_MAX_BYTES, 1020);
        assert_eq!(QuerySimd::<8, 2>::FUSED_REDUCTION_MAX_BYTES, 255);
        assert_eq!(QuerySimd::<8, 1>::FUSED_REDUCTION_MAX_BYTES, usize::MAX);
    }

    /// Vector lengths (bytes) at and just past a width's fused-reduction
    /// bound, so both reduction paths are exercised; none for a one-byte
    /// query, which always fuses.
    fn fused_bound_dims<const PLANES: usize, const QUERY_BYTES: usize>() -> Vec<usize> {
        let bound = QuerySimd::<PLANES, QUERY_BYTES>::FUSED_REDUCTION_MAX_BYTES;
        if bound == usize::MAX {
            Vec::new()
        } else {
            vec![bound * PLANES, (bound + 1) * PLANES]
        }
    }

    /// The batch kernels must reproduce the scalar reference for every
    /// parity dim (interleaved groups and the per-vector remainder, both
    /// reduction paths) at a stride equal to and larger than the vector.
    fn batch_kernels_match_scalar<const PLANES: usize, const QUERY_BYTES: usize>() {
        let mut rng = StdRng::seed_from_u64(7);
        for dim in parity_dims::<PLANES>().chain(fused_bound_dims::<PLANES, QUERY_BYTES>()) {
            let (query, _) = random_inputs::<PLANES, QUERY_BYTES>(&mut rng, dim);
            let vector_bytes = dim / PLANES;
            for stride in [vector_bytes, vector_bytes + 4] {
                let count = 11;
                let data = random_bytes(&mut rng, count * stride);
                let expected: Vec<f32> = (0..count)
                    .map(|v| {
                        query.postprocess(query.dotprod_raw(&data[v * stride..][..vector_bytes]))
                    })
                    .collect();
                let tag =
                    format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES} dim={dim} stride={stride}");
                unsafe {
                    if std::is_x86_feature_detected!("avx2") {
                        let mut actual = vec![0.0; count];
                        query.dotprod_batch_avx2(&data, stride, &mut actual);
                        assert_eq!(expected, actual, "{tag}: avx2 batch");
                    }
                    if has_avx512_vnni() {
                        let mut actual = vec![0.0; count];
                        query.dotprod_batch_avx512_vnni(&data, stride, &mut actual);
                        assert_eq!(expected, actual, "{tag}: avx512_vnni batch");
                    }
                }
            }
        }
    }

    #[test]
    fn test_batch_kernels_match_scalar() {
        batch_kernels_match_scalar::<2, 2>();
        batch_kernels_match_scalar::<4, 2>();
        batch_kernels_match_scalar::<8, 1>();
        batch_kernels_match_scalar::<8, 2>();
    }

    /// The fused reduction at its exact lane bound under the heaviest load
    /// it can see — every query dim at the extreme `q_signed` (both signs),
    /// every code at the max-magnitude codebook slot — must still match the
    /// i64 scalar reference.
    fn fused_reduction_bound_worst_case<const PLANES: usize, const QUERY_BYTES: usize>() {
        let bytes = QuerySimd::<PLANES, QUERY_BYTES>::FUSED_REDUCTION_MAX_BYTES;
        let vector = vec![0xFF_u8; bytes];
        for sign in [1.0_f32, -1.0] {
            let query = QuerySimd::<PLANES, QUERY_BYTES>::new(&vec![sign; bytes * PLANES]);
            let scalar = query.dotprod_raw(&vector);
            let tag = format!("PLANES={PLANES} QUERY_BYTES={QUERY_BYTES} sign={sign}");
            unsafe {
                if std::is_x86_feature_detected!("avx2") {
                    let avx2 = query.dotprod_raw_avx2(&vector);
                    assert_eq!(scalar, avx2, "{tag}: avx2 fused reduction overflowed");
                }
                if has_avx512_vnni() {
                    let vnni512 = query.dotprod_raw_avx512_vnni(&vector);
                    assert_eq!(
                        scalar, vnni512,
                        "{tag}: avx512_vnni fused reduction overflowed"
                    );
                }
            }
        }
    }

    #[test]
    fn test_fused_reduction_bound_worst_case() {
        fused_reduction_bound_worst_case::<2, 2>();
        fused_reduction_bound_worst_case::<4, 2>();
        fused_reduction_bound_worst_case::<8, 2>();
    }
}
