//! Batched MaxSim over TurboQuant-encoded multi-vectors.
//!
//! Scoring a multi-vector token by token runs one single-vector kernel per
//! `query x document` token pair, and each call unpacks the document codes
//! through the codebook again. Here the query side is laid out once, with
//! SIMD lanes holding [`BLOCK`] query tokens, and each document vector is
//! decoded once into signed integers and multiplied against all of them:
//!
//! ```text
//! data[((block * planes + plane) * pairs + pair) * BLOCK * 2 + lane * 2 + i]
//!     = value of dim `2 * pair + i` of query token `block * BLOCK + lane`
//! ```
//!
//! A step broadcasts one pair of document values and folds it into 16 lanes
//! with `madd`. All arithmetic up to the raw dot product is integer and
//! exact, and the float postprocess repeats the single-vector code's
//! operations in the same order, so scores are bit-identical to
//! [`TurboQuantizer::score_precomputed`] and [`TurboQuantizer::score_symmetric`].
//!
//! The query side is either an encoded query (asymmetric scoring) or the
//! stored vectors of another point (symmetric scoring, used while building
//! the HNSW graph). For TQ+ symmetric scoring of 2- and 4-bit codes the
//! per-coordinate weights `w = 256 · w_hi + w_lo` are folded into two query
//! planes, `c · w_lo` and `c · w_hi`, each of which fits an `i16`.

use common::ambient::hw::HwMetric;

use crate::DistanceType;
use crate::packed_multi_query::{BLOCK, crossover};
use crate::turboquant::encoding::TqVectorExtras;
use crate::turboquant::quantization::TurboQuantizer;
use crate::turboquant::simd::query::signed_code_value;
use crate::turboquant::simd::query1bit::CENTROID_SQ;
use crate::turboquant::simd::{CODEBOOK_SCALE_SQ_2BIT, CODEBOOK_SCALE_SQ_4BIT};
use crate::turboquant::{EncodedQueryTQ, EncodedQueryTQData, TQBits};

/// How a lane's raw integer dot product becomes the dot term of its score.
#[derive(Clone, Copy, Debug)]
enum DotTerm {
    /// Asymmetric: `lane_scale · raw + lane_add` (`postprocess_scale` and
    /// the TQ+ `ec_correction` of the query token).
    Query,
    /// Symmetric, plain codebook dot: `raw / divisor`.
    Div(f32),
    /// Symmetric 1-bit: `factor · raw`.
    Mul(f32),
    /// Symmetric 8- and 16-bit: `raw`.
    Raw,
    /// Symmetric TQ+: `raw / divisor + xm_query + xm_vector − mm`, with
    /// `xm_query` in `lane_add`.
    Weighted { divisor: f32, mm: f32 },
}

/// A multi-vector query packed for batched MaxSim against TurboQuant codes.
pub struct PackedMultiQueryTQ {
    /// Query values as `i16` pairs, see the module docs.
    data: Vec<i16>,
    /// `1`, or `2` for the TQ+ weighted symmetric planes (`raw = p0 + 256 · p1`).
    planes: usize,
    /// `1`, or `2` for 16-bit codes, decoded as `256 · high + low` with a
    /// signed high and an unsigned low byte: the products of two 16-bit values
    /// would overflow `i32` sums, those of a byte and a 16-bit value don't.
    doc_planes: usize,
    /// Dims per vector rounded up to pairs.
    pairs: usize,
    vectors_count: usize,
    /// Accumulate in `i64`: some lane's `Σ |q| · max |c|` does not fit `i32`.
    wide: bool,
    bits: TQBits,
    /// Code bytes per stored vector; the extras follow them.
    codes_len: usize,
    /// Signed value of every code of a data byte, `values_per_byte` per byte.
    decode_table: Vec<i16>,
    values_per_byte: usize,
    distance: DistanceType,
    symmetric: bool,
    dot: DotTerm,
    invert: bool,
    /// Per lane, padded to whole blocks: `postprocess_scale` of the query
    /// token, or the `scaling_factor` of the query-side stored vector.
    lane_scale: Vec<f32>,
    /// Per lane: TQ+ `ec_correction` (asymmetric) or `xm` (symmetric).
    lane_add: Vec<f32>,
    /// Per lane, for L2: the query's L2 norm, or the stored vector's length.
    lane_l2: Vec<f32>,
    /// Hardware counter units per pushed vector (`0` to count nothing).
    cpu_per_vector: usize,
}

impl TurboQuantizer {
    /// Whether [`Self::pack_multi_query`] (or, `stored`,
    /// [`Self::pack_multi_stored`]) of `vectors_count` vectors scores faster
    /// than pair by pair on this CPU.
    pub fn packed_pays(&self, vectors_count: usize, stored: bool) -> bool {
        crossover::pays(
            crossover::turbo(self.bits, stored),
            self.padded_dim,
            vectors_count,
        )
    }

    /// [`Self::pack_multi_query`] when it is faster than scoring pair by pair.
    pub fn pack_multi_query_if_faster(
        &self,
        query: &[EncodedQueryTQ],
        invert: bool,
        count_cpu: bool,
    ) -> Option<PackedMultiQueryTQ> {
        if !self.packed_pays(query.len(), false) {
            return None;
        }
        self.pack_multi_query(query, invert, count_cpu)
            .filter(|packed| !packed.is_wide())
    }

    /// [`Self::pack_multi_stored`] when it is faster than scoring pair by pair.
    pub fn pack_multi_stored_if_faster<'a>(
        &self,
        vectors: impl ExactSizeIterator<Item = &'a [u8]>,
        invert: bool,
    ) -> Option<PackedMultiQueryTQ> {
        if !self.packed_pays(vectors.len(), true) {
            return None;
        }
        self.pack_multi_stored(vectors, invert)
            .filter(|packed| !packed.is_wide())
    }

    /// Pack a multi-vector query, one [`EncodedQueryTQ`] per inner vector,
    /// for batched MaxSim with [`PackedMultiQueryTQ::push`].
    ///
    /// Scores are those of [`Self::score_precomputed`], negated when
    /// `invert`. `None` for L1, which dequantizes per vector.
    pub fn pack_multi_query(
        &self,
        query: &[EncodedQueryTQ],
        invert: bool,
        count_cpu: bool,
    ) -> Option<PackedMultiQueryTQ> {
        if matches!(self.distance, DistanceType::L1) {
            return None;
        }
        let mut packed = self.packed_empty(query.len(), false, 1, DotTerm::Query, invert);
        packed.cpu_per_vector = if count_cpu {
            packed.codes_len + self.extras_len()
        } else {
            0
        };
        let max_code = packed.max_code();
        let mut values = vec![0i16; packed.pairs * 2];
        for (token, query) in query.iter().enumerate() {
            let (signed, scale) = match &query.data {
                EncodedQueryTQData::Bits1(q) => q.signed_query(),
                EncodedQueryTQData::Bits1Wide(q) => q.signed_query(),
                EncodedQueryTQData::Bits2(q) => q.signed_query(),
                EncodedQueryTQData::Bits4(q) => q.signed_query(),
                EncodedQueryTQData::Bits8(q) => q.signed_query(),
                EncodedQueryTQData::Bits16(q) => q.signed_query(),
            };
            debug_assert!(signed.len() <= values.len());
            values.fill(0);
            for (value, &q) in values.iter_mut().zip(&signed) {
                // Every width keeps `|q_signed|` within `i16`.
                *value = i16::try_from(q).ok()?;
            }
            packed.set_lane(token, 0, &values, max_code);
            packed.lane_scale[token] = scale;
            packed.lane_add[token] = query.ec_correction;
            packed.lane_l2[token] = query.l2_norm.unwrap_or(1.0);
        }
        Some(packed)
    }

    /// Pack the stored vectors of one point as the query side of a symmetric
    /// MaxSim against other stored vectors, scored as [`Self::score_symmetric`]
    /// (negated when `invert`). `None` for L1.
    pub fn pack_multi_stored<'a>(
        &self,
        vectors: impl ExactSizeIterator<Item = &'a [u8]>,
        invert: bool,
    ) -> Option<PackedMultiQueryTQ> {
        if matches!(self.distance, DistanceType::L1) {
            return None;
        }
        let weights = match (&self.error_correction, self.bits) {
            (Some(ec), TQBits::Bits2 | TQBits::Bits4) => Some(ec),
            _ => None,
        };
        let dot = match (weights, self.bits) {
            (Some(ec), TQBits::Bits2) => DotTerm::Weighted {
                divisor: ec.weight_scale * CODEBOOK_SCALE_SQ_2BIT,
                mm: ec.mm_const,
            },
            (Some(ec), _) => DotTerm::Weighted {
                divisor: ec.weight_scale * CODEBOOK_SCALE_SQ_4BIT,
                mm: ec.mm_const,
            },
            (None, TQBits::Bits1 | TQBits::Bits1_5) => DotTerm::Mul(CENTROID_SQ),
            (None, TQBits::Bits2) => DotTerm::Div(CODEBOOK_SCALE_SQ_2BIT),
            (None, TQBits::Bits4) => DotTerm::Div(CODEBOOK_SCALE_SQ_4BIT),
            (None, TQBits::Bits8 | TQBits::Bits16) => DotTerm::Raw,
        };
        let planes = if weights.is_some() { 2 } else { 1 };
        let mut packed = self.packed_empty(vectors.len(), true, planes, dot, invert);
        let max_code = packed.max_code();
        let mut values = vec![0i16; packed.pairs * 2];
        let mut weighted = vec![0i16; packed.pairs * 2];
        for (token, vector) in vectors.enumerate() {
            let (codes, extras) = vector.split_at(packed.codes_len);
            let extras = TqVectorExtras::from_bytes(extras);
            if packed.doc_planes == 2 {
                // The query side keeps whole 16-bit values; only stored vectors are split.
                for (value, &code) in values.iter_mut().zip(codes.as_chunks::<2>().0) {
                    *value = i16::from_le_bytes(code);
                }
            } else {
                packed.decode(codes, &mut values);
            }
            match weights {
                None => packed.set_lane(token, 0, &values, max_code),
                Some(ec) => {
                    // `w = 256 · w_hi + w_lo` with `0 <= w < 2^15`: `|c · w_lo| <= 128 · 255`
                    // and `|c · w_hi| <= 128 · 127` both fit an `i16`.
                    for (plane, shift) in [(0, 0), (1, 8)] {
                        for ((out, &c), &w) in
                            weighted.iter_mut().zip(&values).zip(&ec.d_prime_sq_i16)
                        {
                            *out = c * ((w >> shift) & 0xFF);
                        }
                        packed.set_lane(token, plane, &weighted, max_code);
                    }
                    packed.lane_add[token] = extras.ec_correction();
                }
            }
            packed.lane_scale[token] = extras.scaling_factor();
            if matches!(self.distance, DistanceType::L2) {
                packed.lane_l2[token] = extras.l2_length();
            }
        }
        Some(packed)
    }

    fn extras_len(&self) -> usize {
        TqVectorExtras::size_for(self.bits, self.distance, self.mode)
    }

    fn packed_empty(
        &self,
        vectors_count: usize,
        symmetric: bool,
        planes: usize,
        dot: DotTerm,
        invert: bool,
    ) -> PackedMultiQueryTQ {
        let codes_len = self.quantized_size() - self.extras_len();
        let (values_per_byte, decode_table) = decode_table(self.bits, symmetric);
        let dims = match values_per_byte {
            0 => codes_len / 2,
            n => codes_len * n,
        };
        let pairs = dims.div_ceil(2);
        let lanes = vectors_count.div_ceil(BLOCK) * BLOCK;
        PackedMultiQueryTQ {
            data: vec![0; lanes / BLOCK * planes * pairs * BLOCK * 2],
            planes,
            doc_planes: if self.bits == TQBits::Bits16 { 2 } else { 1 },
            pairs,
            vectors_count,
            wide: false,
            bits: self.bits,
            codes_len,
            decode_table,
            values_per_byte,
            distance: self.distance,
            symmetric,
            dot,
            invert,
            lane_scale: vec![0.0; lanes],
            lane_add: vec![0.0; lanes],
            lane_l2: vec![0.0; lanes],
            cpu_per_vector: 0,
        }
    }
}

/// Signed value of each code of every possible data byte, and the number of
/// codes per byte (`0` for 16-bit codes, which take two bytes).
///
/// Symmetric 1-bit scoring counts agreeing signs (`±1`); everything else
/// uses the values the asymmetric kernels multiply.
fn decode_table(bits: TQBits, symmetric: bool) -> (usize, Vec<i16>) {
    fn table<const PLANES: usize>(value: impl Fn(u8, usize) -> i16) -> (usize, Vec<i16>) {
        let table = (0..=u8::MAX)
            .flat_map(|byte| (0..PLANES).map(move |k| (byte, k)))
            .map(|(byte, k)| value(byte, k))
            .collect();
        (PLANES, table)
    }
    match bits {
        TQBits::Bits1 | TQBits::Bits1_5 if symmetric => {
            table::<8>(|byte, k| if byte >> k & 1 == 1 { 1 } else { -1 })
        }
        TQBits::Bits1 | TQBits::Bits1_5 => table::<8>(signed_code_value::<8>),
        TQBits::Bits2 => table::<4>(signed_code_value::<4>),
        TQBits::Bits4 => table::<2>(signed_code_value::<2>),
        TQBits::Bits8 => table::<1>(signed_code_value::<1>),
        TQBits::Bits16 => (0, Vec::new()),
    }
}

impl PackedMultiQueryTQ {
    pub fn vectors_count(&self) -> usize {
        self.vectors_count
    }

    /// Accumulates in `i64`, on the portable kernel.
    pub fn is_wide(&self) -> bool {
        self.wide
    }

    pub(crate) fn lanes(&self) -> usize {
        self.lane_scale.len()
    }

    /// Scratch `i16` values one decoded vector needs.
    pub(crate) fn scratch_len(&self) -> usize {
        self.pairs * 2 * self.doc_planes
    }

    /// Products summed per lane: one plane of the query or of the document has
    /// two (`raw = term 0 + 256 · term 1`), never both.
    fn terms(&self) -> usize {
        debug_assert!(self.planes == 1 || self.doc_planes == 1);
        self.planes.max(self.doc_planes)
    }

    /// Offsets of the query plane in a block and of the document values of `term`.
    fn term_offsets(&self, block: usize, term: usize) -> (usize, usize) {
        let plane = if self.planes == 2 { term } else { 0 };
        let doc = if self.doc_planes == 2 {
            term * self.pairs * 2
        } else {
            0
        };
        ((block * self.planes + plane) * self.pairs * BLOCK * 2, doc)
    }

    /// Largest `|c_signed|` of a decoded stored vector.
    fn max_code(&self) -> i64 {
        match self.bits {
            // The low byte of a split 16-bit value; the high one is at most 128.
            TQBits::Bits16 => i64::from(u8::MAX),
            TQBits::Bits1 | TQBits::Bits1_5 | TQBits::Bits2 | TQBits::Bits4 | TQBits::Bits8 => self
                .decode_table
                .iter()
                .map(|&c| i64::from(c).abs())
                .max()
                .unwrap_or(0),
        }
    }

    /// Write one token's values of one plane and widen the accumulators if
    /// they could overflow `i32`.
    fn set_lane(&mut self, token: usize, plane: usize, values: &[i16], max_code: i64) {
        let (block, lane) = (token / BLOCK, token % BLOCK);
        let base = (block * self.planes + plane) * self.pairs * BLOCK * 2;
        for (pair, chunk) in values.as_chunks::<2>().0.iter().enumerate() {
            let at = base + pair * BLOCK * 2 + lane * 2;
            self.data[at..at + 2].copy_from_slice(chunk);
        }
        // A pair of products is at most `2 · 2^15 · 2^15 = 2^31` only for
        // `-2^15` on both sides, which neither side holds.
        let bound: i64 = values.iter().map(|&q| i64::from(q).abs() * max_code).sum();
        if bound > i64::from(i32::MAX) {
            self.wide = true;
        }
    }

    /// Decode the codes of one stored vector into signed values, zero-padded
    /// to whole pairs.
    fn decode(&self, codes: &[u8], out: &mut [i16]) {
        debug_assert_eq!(codes.len(), self.codes_len);
        debug_assert_eq!(out.len(), self.scratch_len());
        if self.doc_planes == 2 {
            let (low, high) = out.split_at_mut(self.pairs * 2);
            let values = codes.as_chunks::<2>().0;
            for ((low, high), &code) in low.iter_mut().zip(high.iter_mut()).zip(values) {
                let value = i16::from_le_bytes(code);
                *low = value & 0xFF;
                *high = value >> 8;
            }
            low[values.len()..].fill(0);
            high[values.len()..].fill(0);
            return;
        }
        #[cfg(target_arch = "x86_64")]
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 is available.
            unsafe { x86::decode_avx2(self, codes, out) };
        } else {
            self.decode_by_table(codes, out);
        }
        #[cfg(not(target_arch = "x86_64"))]
        self.decode_by_table(codes, out);
        let decoded = match self.values_per_byte {
            0 => codes.len() / 2,
            n => codes.len() * n,
        };
        out[decoded..].fill(0);
    }

    fn decode_by_table(&self, codes: &[u8], out: &mut [i16]) {
        fn by_table<const PLANES: usize>(table: &[i16], codes: &[u8], out: &mut [i16]) {
            let (table, _) = table.as_chunks::<PLANES>();
            let (chunks, _) = out.as_chunks_mut::<PLANES>();
            for (out, &byte) in chunks.iter_mut().zip(codes) {
                *out = table[usize::from(byte)];
            }
        }
        match self.values_per_byte {
            8 => by_table::<8>(&self.decode_table, codes, out),
            4 => by_table::<4>(&self.decode_table, codes, out),
            2 => by_table::<2>(&self.decode_table, codes, out),
            1 => by_table::<1>(&self.decode_table, codes, out),
            _ => unreachable!("16-bit codes are split in `decode`"),
        }
    }

    /// Fold one stored vector (codes followed by extras) into the running
    /// per-token maxima. `scratch` holds [`Self::scratch_len`] values.
    pub(crate) fn push(&self, maxima: &mut [f32], scratch: &mut [i16], vector: &[u8]) {
        assert!(vector.len() >= self.codes_len);
        if self.cpu_per_vector > 0 {
            HwMetric::Cpu.bump(self.cpu_per_vector * self.vectors_count);
        }
        let (codes, extras) = vector.split_at(self.codes_len);
        let extras = TqVectorExtras::from_bytes(extras);
        self.decode(codes, scratch);
        let doc = Doc {
            scale: extras.scaling_factor(),
            l2: match self.distance {
                DistanceType::L2 => extras.l2_length(),
                DistanceType::Dot | DistanceType::Cosine | DistanceType::L1 => 0.0,
            },
            add: match self.dot {
                DotTerm::Weighted { .. } => extras.ec_correction(),
                DotTerm::Query | DotTerm::Div(_) | DotTerm::Mul(_) | DotTerm::Raw => 0.0,
            },
        };

        #[cfg(target_arch = "x86_64")]
        if !self.wide
            && std::is_x86_feature_detected!("avx512f")
            && std::is_x86_feature_detected!("avx512vl")
            && std::is_x86_feature_detected!("avx512vnni")
        {
            // SAFETY: AVX-512 F, VL and VNNI are available; see below for the buffers.
            unsafe { x86::push_avx512(self, maxima, scratch, &doc) };
            return;
        }
        #[cfg(target_arch = "x86_64")]
        if !self.wide && std::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 is available; `scratch` holds `pairs` pairs and `data`
            // holds `planes · pairs` pairs per block.
            unsafe { x86::push_avx2(self, maxima, scratch, &doc) };
            return;
        }
        self.push_portable(maxima, scratch, &doc);
    }

    fn push_portable(&self, maxima: &mut [f32], doc_values: &[i16], doc: &Doc) {
        let block_len = self.pairs * BLOCK * 2;
        for block in 0..self.lanes() / BLOCK {
            let mut raw = [0f64; BLOCK];
            for term in 0..self.terms() {
                let (base, doc_base) = self.term_offsets(block, term);
                let query = &self.data[base..base + block_len];
                let doc_values = &doc_values[doc_base..doc_base + self.pairs * 2];
                let mut acc = [0i64; BLOCK];
                for (pair, d) in doc_values.as_chunks::<2>().0.iter().enumerate() {
                    let q = &query[pair * BLOCK * 2..][..BLOCK * 2];
                    for (lane, acc) in acc.iter_mut().enumerate() {
                        *acc += i64::from(q[lane * 2]) * i64::from(d[0])
                            + i64::from(q[lane * 2 + 1]) * i64::from(d[1]);
                    }
                }
                let factor = if term == 0 { 1.0 } else { 256.0 };
                for (raw, &acc) in raw.iter_mut().zip(&acc) {
                    *raw += acc as f64 * factor;
                }
            }
            // `i64 → f64` is exact below `2^53`, so `f64 → f32` rounds once,
            // exactly as `i64 as f32` does.
            let raw = raw.map(|raw| raw as f32);
            self.finish_block(block, &raw, doc, maxima);
        }
    }

    /// Scores of one block from its raw dot products, in the operation order
    /// of the single-vector scorers, and the running maxima. Each step is a
    /// plain loop over the lanes, so it vectorizes.
    #[inline(always)]
    fn finish_block(&self, block: usize, raw: &[f32; BLOCK], doc: &Doc, maxima: &mut [f32]) {
        let lanes = block * BLOCK..(block + 1) * BLOCK;
        let scale: &[f32; BLOCK] = self.lane_scale[lanes.clone()].try_into().unwrap();
        let add: &[f32; BLOCK] = self.lane_add[lanes.clone()].try_into().unwrap();
        let l2: &[f32; BLOCK] = self.lane_l2[lanes.clone()].try_into().unwrap();
        let maxima: &mut [f32; BLOCK] = (&mut maxima[lanes]).try_into().unwrap();

        let mut dot = [0f32; BLOCK];
        match self.dot {
            DotTerm::Query => {
                for lane in 0..BLOCK {
                    dot[lane] = scale[lane] * raw[lane] + add[lane];
                }
            }
            DotTerm::Div(divisor) => {
                for lane in 0..BLOCK {
                    dot[lane] = raw[lane] / divisor;
                }
            }
            DotTerm::Mul(factor) => {
                for lane in 0..BLOCK {
                    dot[lane] = factor * raw[lane];
                }
            }
            DotTerm::Raw => dot = *raw,
            DotTerm::Weighted { divisor, mm } => {
                for lane in 0..BLOCK {
                    dot[lane] = raw[lane] / divisor + add[lane] + doc.add - mm;
                }
            }
        }

        let mut score = [0f32; BLOCK];
        match (self.symmetric, self.distance) {
            (false, DistanceType::L2) => {
                for lane in 0..BLOCK {
                    score[lane] =
                        l2[lane] * l2[lane] + doc.l2 * doc.l2 - 2.0 * dot[lane] * doc.scale;
                }
            }
            (true, DistanceType::L2) => {
                for lane in 0..BLOCK {
                    score[lane] = l2[lane] * l2[lane] + doc.l2 * doc.l2
                        - 2.0 * scale[lane] * doc.scale * dot[lane];
                }
            }
            // L1 is never packed.
            (false, DistanceType::Dot | DistanceType::Cosine | DistanceType::L1) => {
                for lane in 0..BLOCK {
                    score[lane] = dot[lane] * doc.scale;
                }
            }
            (true, DistanceType::Dot | DistanceType::Cosine | DistanceType::L1) => {
                for lane in 0..BLOCK {
                    score[lane] = dot[lane] * scale[lane] * doc.scale;
                }
            }
        }
        if self.invert {
            score = score.map(|score| -score);
        }
        for lane in 0..BLOCK {
            if maxima[lane] < score[lane] {
                maxima[lane] = score[lane];
            }
        }
    }
}

/// Per stored vector terms of the score.
struct Doc {
    /// `scaling_factor`.
    scale: f32,
    /// `l2_length`, for L2.
    l2: f32,
    /// TQ+ `xm`, for the weighted symmetric score.
    add: f32,
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{BLOCK, Doc, DotTerm, PackedMultiQueryTQ};
    use crate::DistanceType;

    /// [`PackedMultiQueryTQ::decode`] for 4- and 8-bit codes: 16 code bytes per
    /// step, looked up with a byte shuffle (4-bit) or shifted to the signed grid
    /// (8-bit). Other widths decode by table.
    ///
    /// # Safety
    /// CPU must support AVX2.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn decode_avx2(query: &PackedMultiQueryTQ, codes: &[u8], out: &mut [i16]) {
        let (chunks, tail) = codes.as_chunks::<16>();
        debug_assert!(
            query.values_per_byte == 0 || out.len() >= codes.len() * query.values_per_byte
        );
        // SAFETY: every chunk of 16 code bytes writes `16 · values_per_byte` values,
        // within `out` by the assert above; the table holds 256 entries per code.
        unsafe {
            match query.values_per_byte {
                2 => {
                    // The 16 values of a 4-bit code fit `i8`.
                    let table: [i8; 16] =
                        std::array::from_fn(|code| query.decode_table[code * 2] as i8);
                    let table = _mm_loadu_si128(table.as_ptr().cast());
                    let mask = _mm_set1_epi8(0x0F);
                    for (i, chunk) in chunks.iter().enumerate() {
                        let bytes = _mm_loadu_si128(chunk.as_ptr().cast());
                        let low = _mm_shuffle_epi8(table, _mm_and_si128(bytes, mask));
                        let high =
                            _mm_shuffle_epi8(table, _mm_and_si128(_mm_srli_epi16(bytes, 4), mask));
                        // Low nibble first: dims `2j`, `2j + 1`.
                        let out = out.as_mut_ptr().add(i * 32);
                        _mm256_storeu_si256(
                            out.cast(),
                            _mm256_cvtepi8_epi16(_mm_unpacklo_epi8(low, high)),
                        );
                        _mm256_storeu_si256(
                            out.add(16).cast(),
                            _mm256_cvtepi8_epi16(_mm_unpackhi_epi8(low, high)),
                        );
                    }
                }
                1 => {
                    // `code - 128`, as the table holds.
                    debug_assert!(query.decode_table[0] == -128 && query.decode_table[255] == 127);
                    let offset = _mm256_set1_epi16(128);
                    for (i, chunk) in chunks.iter().enumerate() {
                        let bytes = _mm_loadu_si128(chunk.as_ptr().cast());
                        let values = _mm256_sub_epi16(_mm256_cvtepu8_epi16(bytes), offset);
                        _mm256_storeu_si256(out.as_mut_ptr().add(i * 16).cast(), values);
                    }
                }
                _ => return query.decode_by_table(codes, out),
            }
        }
        let done = chunks.len() * 16;
        query.decode_by_table(tail, &mut out[done * query.values_per_byte..]);
    }

    /// Scores of 8 lanes from their raw dot products, in the operation order of
    /// [`PackedMultiQueryTQ::finish_block`].
    ///
    /// # Safety
    /// CPU must support AVX2; `lane + 8` lanes exist.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn score8(query: &PackedMultiQueryTQ, lane: usize, raw: __m256, doc: &Doc) -> __m256 {
        // SAFETY: the lane arrays are padded to whole blocks.
        let (scale, add, l2) = unsafe {
            (
                _mm256_loadu_ps(query.lane_scale.as_ptr().add(lane)),
                _mm256_loadu_ps(query.lane_add.as_ptr().add(lane)),
                _mm256_loadu_ps(query.lane_l2.as_ptr().add(lane)),
            )
        };
        let dot = match query.dot {
            DotTerm::Query => _mm256_add_ps(_mm256_mul_ps(scale, raw), add),
            DotTerm::Div(divisor) => _mm256_div_ps(raw, _mm256_set1_ps(divisor)),
            DotTerm::Mul(factor) => _mm256_mul_ps(_mm256_set1_ps(factor), raw),
            DotTerm::Raw => raw,
            DotTerm::Weighted { divisor, mm } => _mm256_sub_ps(
                _mm256_add_ps(
                    _mm256_add_ps(_mm256_div_ps(raw, _mm256_set1_ps(divisor)), add),
                    _mm256_set1_ps(doc.add),
                ),
                _mm256_set1_ps(mm),
            ),
        };
        let doc_scale = _mm256_set1_ps(doc.scale);
        let two = _mm256_set1_ps(2.0);
        let score = match (query.symmetric, query.distance) {
            (false, DistanceType::L2) => _mm256_sub_ps(
                _mm256_add_ps(_mm256_mul_ps(l2, l2), _mm256_set1_ps(doc.l2 * doc.l2)),
                _mm256_mul_ps(_mm256_mul_ps(two, dot), doc_scale),
            ),
            (true, DistanceType::L2) => _mm256_sub_ps(
                _mm256_add_ps(_mm256_mul_ps(l2, l2), _mm256_set1_ps(doc.l2 * doc.l2)),
                _mm256_mul_ps(_mm256_mul_ps(_mm256_mul_ps(two, scale), doc_scale), dot),
            ),
            (false, DistanceType::Dot | DistanceType::Cosine | DistanceType::L1) => {
                _mm256_mul_ps(dot, doc_scale)
            }
            (true, DistanceType::Dot | DistanceType::Cosine | DistanceType::L1) => {
                _mm256_mul_ps(_mm256_mul_ps(dot, scale), doc_scale)
            }
        };
        if query.invert {
            _mm256_xor_ps(score, _mm256_set1_ps(-0.0))
        } else {
            score
        }
    }

    /// Raw dot products of 8 lanes as `f32`: `plane 0 + 256 · plane 1`, exact in
    /// `f64` below `2^53`, then rounded once as `i64 as f32` does.
    #[inline]
    #[target_feature(enable = "avx2")]
    fn combine8(terms: usize, acc: [__m256i; 2]) -> __m256 {
        if terms == 1 {
            // `i32 → f32` rounds once, like `i64 as f32` of the same value.
            return _mm256_cvtepi32_ps(acc[0]);
        }
        let half = |acc: [__m128i; 2]| {
            let low = _mm256_cvtepi32_pd(acc[0]);
            let high = _mm256_mul_pd(_mm256_cvtepi32_pd(acc[1]), _mm256_set1_pd(256.0));
            _mm256_cvtpd_ps(_mm256_add_pd(low, high))
        };
        let first = half([
            _mm256_castsi256_si128(acc[0]),
            _mm256_castsi256_si128(acc[1]),
        ]);
        let second = half([
            _mm256_extracti128_si256(acc[0], 1),
            _mm256_extracti128_si256(acc[1], 1),
        ]);
        _mm256_set_m128(second, first)
    }

    /// # Safety
    /// CPU must support AVX2; the accumulators of `query` must fit `i32`
    /// (`!query.wide`); `doc_values` holds `pairs` pairs.
    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn push_avx2(
        query: &PackedMultiQueryTQ,
        maxima: &mut [f32],
        doc_values: &[i16],
        doc: &Doc,
    ) {
        debug_assert!(doc_values.len() >= query.scratch_len());
        for block in 0..query.lanes() / BLOCK {
            // SAFETY: as this function's contract.
            unsafe { push_block_avx2(query, maxima, doc_values, doc, block) };
        }
    }

    /// One block of [`push_avx2`].
    ///
    /// # Safety
    /// As [`push_avx2`]; `block` is a block of `query`.
    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn push_block_avx2(
        query: &PackedMultiQueryTQ,
        maxima: &mut [f32],
        doc_values: &[i16],
        doc: &Doc,
        block: usize,
    ) {
        let pairs = query.pairs;
        let doc_ptr = doc_values.as_ptr().cast::<i32>();
        // A last block of at most 8 tokens uses one register.
        let half = query.vectors_count - block * BLOCK <= 8;
        // Sums per term and half block.
        let mut by_term = [[_mm256_setzero_si256(); 2]; 2];
        for (term, sums) in by_term.iter_mut().enumerate().take(query.terms()) {
            let (base, doc_base) = query.term_offsets(block, term);
            // SAFETY: `data` holds `planes · pairs` pairs of 16 lanes per block,
            // `doc_values` `doc_planes · pairs` pairs, and `pair < pairs`.
            unsafe {
                let packed = query.data.as_ptr().add(base);
                let doc_ptr = doc_ptr.add(doc_base / 2);
                let (mut acc0, mut acc1) = (_mm256_setzero_si256(), _mm256_setzero_si256());
                if half {
                    for pair in 0..pairs {
                        let x = _mm256_set1_epi32(doc_ptr.add(pair).read_unaligned());
                        let q0 = _mm256_loadu_si256(packed.add(pair * BLOCK * 2).cast());
                        acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(x, q0));
                    }
                } else {
                    for pair in 0..pairs {
                        // One pair of document values against the same pair of dims of
                        // 8 + 8 query tokens.
                        let x = _mm256_set1_epi32(doc_ptr.add(pair).read_unaligned());
                        let q = packed.add(pair * BLOCK * 2);
                        let q0 = _mm256_loadu_si256(q.cast());
                        let q1 = _mm256_loadu_si256(q.add(BLOCK).cast());
                        acc0 = _mm256_add_epi32(acc0, _mm256_madd_epi16(x, q0));
                        acc1 = _mm256_add_epi32(acc1, _mm256_madd_epi16(x, q1));
                    }
                }
                *sums = [acc0, acc1];
            }
        }
        for half_block in 0..if half { 1 } else { 2 } {
            let lane = block * BLOCK + half_block * 8;
            let acc = by_term.map(|sums| sums[half_block]);
            // SAFETY: lanes are padded to whole blocks.
            unsafe {
                let score = score8(query, lane, combine8(query.terms(), acc), doc);
                let max = maxima.as_mut_ptr().add(lane);
                // Keeps the maximum unless the score is larger, like `maximum < score`.
                _mm256_storeu_ps(max, _mm256_max_ps(score, _mm256_loadu_ps(max)));
            }
        }
    }

    /// Independent accumulator chains per block in [`push_avx512`]: one chain
    /// waits out `vpdpwssd`'s latency on every pair.
    const CHAINS: usize = 4;

    /// [`push_avx2`] with one 16-lane register per block and `vpdpwssd`, which
    /// multiplies a pair and adds it in one instruction. A last block of at most
    /// 8 tokens uses one 8-lane register.
    ///
    /// # Safety
    /// CPU must support AVX-512 F, VL and VNNI; the accumulators of `query` must
    /// fit `i32` (`!query.wide`); `doc_values` holds `pairs` pairs.
    #[target_feature(enable = "avx512f,avx512vl,avx512vnni")]
    pub(super) unsafe fn push_avx512(
        query: &PackedMultiQueryTQ,
        maxima: &mut [f32],
        doc_values: &[i16],
        doc: &Doc,
    ) {
        debug_assert!(doc_values.len() >= query.scratch_len());
        let pairs = query.pairs;
        let whole = pairs - pairs % CHAINS;
        let doc_ptr = doc_values.as_ptr().cast::<i32>();
        for block in 0..query.lanes() / BLOCK {
            if query.vectors_count - block * BLOCK <= 8 {
                // A last block of at most 8 tokens: the same chains in one 8-lane
                // register.
                let mut acc = [_mm256_setzero_si256(); 2];
                for (term, acc) in acc.iter_mut().enumerate().take(query.terms()) {
                    let (base, doc_base) = query.term_offsets(block, term);
                    // SAFETY: as in `push_block_avx2`.
                    unsafe {
                        let packed = query.data.as_ptr().add(base);
                        let doc_ptr = doc_ptr.add(doc_base / 2);
                        let mut sums = [_mm256_setzero_si256(); CHAINS];
                        for first in (0..whole).step_by(CHAINS) {
                            for (chain, sum) in sums.iter_mut().enumerate() {
                                *sum = pair_avx512_half(*sum, doc_ptr, packed, first + chain);
                            }
                        }
                        for pair in whole..pairs {
                            sums[0] = pair_avx512_half(sums[0], doc_ptr, packed, pair);
                        }
                        *acc = _mm256_add_epi32(
                            _mm256_add_epi32(sums[0], sums[1]),
                            _mm256_add_epi32(sums[2], sums[3]),
                        );
                    }
                }
                let lane = block * BLOCK;
                // SAFETY: AVX-512 implies AVX2; lanes are padded to whole blocks.
                unsafe {
                    let score = score8(query, lane, combine8(query.terms(), acc), doc);
                    let max = maxima.as_mut_ptr().add(lane);
                    _mm256_storeu_ps(max, _mm256_max_ps(score, _mm256_loadu_ps(max)));
                }
                continue;
            }
            let mut acc = [_mm512_setzero_si512(); 2];
            for (term, acc) in acc.iter_mut().enumerate().take(query.terms()) {
                let (base, doc_base) = query.term_offsets(block, term);
                // SAFETY: as in `push_block_avx2`.
                unsafe {
                    let packed = query.data.as_ptr().add(base);
                    let doc_ptr = doc_ptr.add(doc_base / 2);
                    // Integer sums, so splitting them over chains changes nothing.
                    let mut sums = [_mm512_setzero_si512(); CHAINS];
                    for first in (0..whole).step_by(CHAINS) {
                        for (chain, sum) in sums.iter_mut().enumerate() {
                            *sum = pair_avx512(*sum, doc_ptr, packed, first + chain);
                        }
                    }
                    for pair in whole..pairs {
                        sums[0] = pair_avx512(sums[0], doc_ptr, packed, pair);
                    }
                    *acc = _mm512_add_epi32(
                        _mm512_add_epi32(sums[0], sums[1]),
                        _mm512_add_epi32(sums[2], sums[3]),
                    );
                }
            }
            // The two halves of the block through the 8-lane score.
            for half_block in 0..2 {
                let lane = block * BLOCK + half_block * 8;
                let halves = acc.map(|acc| {
                    if half_block == 0 {
                        _mm512_castsi512_si256(acc)
                    } else {
                        _mm512_extracti64x4_epi64(acc, 1)
                    }
                });
                // SAFETY: AVX-512 implies AVX2; lanes are padded to whole blocks.
                unsafe {
                    let score = score8(query, lane, combine8(query.terms(), halves), doc);
                    let max = maxima.as_mut_ptr().add(lane);
                    _mm256_storeu_ps(max, _mm256_max_ps(score, _mm256_loadu_ps(max)));
                }
            }
        }
    }

    /// One pair of document values against the same pair of dims of 16 query
    /// tokens.
    ///
    /// # Safety
    /// CPU must support AVX-512 F and VNNI; `doc_ptr` holds pair `pair` and
    /// `packed` its 16 lanes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512vnni")]
    unsafe fn pair_avx512(
        acc: __m512i,
        doc_ptr: *const i32,
        packed: *const i16,
        pair: usize,
    ) -> __m512i {
        // SAFETY: by this function's contract.
        unsafe {
            let x = _mm512_set1_epi32(doc_ptr.add(pair).read_unaligned());
            let q = _mm512_loadu_si512(packed.add(pair * BLOCK * 2).cast());
            _mm512_dpwssd_epi32(acc, x, q)
        }
    }

    /// [`pair_avx512`] for the first 8 query tokens of a block.
    ///
    /// # Safety
    /// CPU must support AVX-512 VL and VNNI; `doc_ptr` holds pair `pair` and
    /// `packed` its 16 lanes.
    #[inline]
    #[target_feature(enable = "avx512f,avx512vl,avx512vnni")]
    unsafe fn pair_avx512_half(
        acc: __m256i,
        doc_ptr: *const i32,
        packed: *const i16,
        pair: usize,
    ) -> __m256i {
        // SAFETY: by this function's contract.
        unsafe {
            let x = _mm256_set1_epi32(doc_ptr.add(pair).read_unaligned());
            let q = _mm256_loadu_si256(packed.add(pair * BLOCK * 2).cast());
            _mm256_dpwssd_epi32(acc, x, q)
        }
    }
}
