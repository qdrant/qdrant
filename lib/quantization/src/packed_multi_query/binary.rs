//! Batched MaxSim for binary-quantized multi-vectors.
//!
//! Binary scores are an XOR popcount between a stored vector and the query,
//! which costs a few instructions per vector; scoring pair by pair spends most
//! of its time on the call, the dispatch and the score conversion around it.
//! Here the query bits are laid out once per search as 64-bit words with SIMD
//! lanes holding query tokens:
//!
//! ```text
//! words[((block * planes + plane) * vector_words + word) * BLOCK + lane]
//! ```
//!
//! A scalar query keeps bit `b` of each of its values in plane `b`; a binary
//! query has one plane. Each word of a stored vector is XORed against
//! [`BLOCK`] query tokens at once and counted per lane, which vectorizes
//! (`vpopcntq` on AVX-512, a nibble lookup on AVX2). Counts are exact integers
//! and the conversion repeats `EncodedVectorsBin::calculate_metric`, so scores
//! are bit-identical.

use common::ambient::hw::HwMetric;

use super::BLOCK;
use crate::DistanceType;

pub struct PackedMultiQueryBinary {
    words: Vec<u64>,
    /// Bytes of a stored vector.
    vector_len: usize,
    /// Stored vector bytes rounded up to words.
    vector_words: usize,
    bits_count: usize,
    vectors_count: usize,
    lanes: usize,
    dim: f32,
    distance: DistanceType,
    invert: bool,
    /// Hardware counter units per pushed vector (`0` to count nothing).
    cpu_per_vector: usize,
}

impl PackedMultiQueryBinary {
    /// `queries` yields every query token's encoded bytes: `vector_len` bytes
    /// of a binary query, or `vector_len · bits_count` bytes of a scalar query
    /// whose storage elements are `element_size` bytes, with the `bits_count`
    /// bit-elements of each storage element stored next to each other.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new<'a>(
        queries: impl ExactSizeIterator<Item = &'a [u8]>,
        vector_len: usize,
        element_size: usize,
        bits_count: usize,
        dim: usize,
        distance: DistanceType,
        invert: bool,
        cpu_per_vector: usize,
    ) -> Self {
        let vectors_count = queries.len();
        let lanes = vectors_count.div_ceil(BLOCK) * BLOCK;
        let vector_words = vector_len.div_ceil(8);
        let mut words = vec![0u64; lanes * bits_count * vector_words];
        let mut plane = vec![0u8; vector_words * 8];
        for (token, query) in queries.enumerate() {
            debug_assert_eq!(query.len(), vector_len * bits_count);
            let (block, lane) = (token / BLOCK, token % BLOCK);
            for bit in 0..bits_count {
                // Gather plane `bit` of the token: the `bit`-th element of every group.
                for (element, group) in query.chunks_exact(element_size * bits_count).enumerate() {
                    let value = &group[bit * element_size..][..element_size];
                    plane[element * element_size..][..element_size].copy_from_slice(value);
                }
                for (word, bytes) in plane.as_chunks::<8>().0.iter().enumerate() {
                    let at = ((block * bits_count + bit) * vector_words + word) * BLOCK + lane;
                    words[at] = u64::from_le_bytes(*bytes);
                }
            }
        }
        Self {
            words,
            vector_len,
            vector_words,
            bits_count,
            vectors_count,
            lanes,
            dim: dim as f32,
            distance,
            invert,
            cpu_per_vector,
        }
    }

    pub fn vectors_count(&self) -> usize {
        self.vectors_count
    }

    pub(crate) fn lanes(&self) -> usize {
        self.lanes
    }

    /// Fold one stored vector into the running per-token maxima.
    pub(crate) fn push(&self, maxima: &mut [f32], vector: &[u8]) {
        assert!(vector.len() >= self.vector_len);
        assert!(maxima.len() >= self.lanes);
        if self.cpu_per_vector > 0 {
            HwMetric::Cpu.bump(self.cpu_per_vector * self.vectors_count);
        }
        // The vector as little-endian words, zero-padded like the query.
        let mut doc = [0u64; 32];
        let mut doc_words = Vec::new();
        let doc: &mut [u64] = if self.vector_words <= doc.len() {
            &mut doc[..self.vector_words]
        } else {
            doc_words.resize(self.vector_words, 0);
            &mut doc_words
        };
        let (whole, tail) = vector[..self.vector_len].as_chunks::<8>();
        for (word, bytes) in doc.iter_mut().zip(whole) {
            *word = u64::from_le_bytes(*bytes);
        }
        if !tail.is_empty() {
            let mut bytes = [0u8; 8];
            bytes[..tail.len()].copy_from_slice(tail);
            doc[whole.len()] = u64::from_le_bytes(bytes);
        }

        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512f")
                && std::is_x86_feature_detected!("avx512vpopcntdq")
            {
                // SAFETY: the features are available.
                unsafe { self.push_avx512(maxima, doc) };
                return;
            }
            if std::is_x86_feature_detected!("avx2") {
                // SAFETY: the feature is available.
                unsafe { self.push_avx2(maxima, doc) };
                return;
            }
        }
        self.push_any(maxima, doc);
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f,avx512vpopcntdq")]
    unsafe fn push_avx512(&self, maxima: &mut [f32], doc: &[u64]) {
        self.push_any(maxima, doc);
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2,popcnt")]
    unsafe fn push_avx2(&self, maxima: &mut [f32], doc: &[u64]) {
        self.push_any(maxima, doc);
    }

    /// Lane loops for the compiler to vectorize under the caller's features.
    #[inline(always)]
    fn push_any(&self, maxima: &mut [f32], doc: &[u64]) {
        let plane_len = self.vector_words * BLOCK;
        for block in 0..self.lanes / BLOCK {
            let mut counts = [0u32; BLOCK];
            for bit in 0..self.bits_count {
                let plane = &self.words[(block * self.bits_count + bit) * plane_len..][..plane_len];
                let mut plane_counts = [0u32; BLOCK];
                for (&doc_word, query) in doc.iter().zip(plane.as_chunks::<BLOCK>().0) {
                    for lane in 0..BLOCK {
                        plane_counts[lane] += (query[lane] ^ doc_word).count_ones();
                    }
                }
                for lane in 0..BLOCK {
                    counts[lane] += plane_counts[lane] << bit;
                }
            }
            self.finish_block(block, &counts, maxima);
        }
    }

    /// `EncodedVectorsBin::calculate_metric` on a block of XOR counts.
    #[inline(always)]
    fn finish_block(&self, block: usize, counts: &[u32; BLOCK], maxima: &mut [f32]) {
        let mut xor = [0f32; BLOCK];
        if self.bits_count == 1 {
            for lane in 0..BLOCK {
                xor[lane] = counts[lane] as f32;
            }
        } else {
            let levels = ((1 << self.bits_count) - 1) as f32;
            for lane in 0..BLOCK {
                xor[lane] = (counts[lane] as f32) / levels;
            }
        }
        let mut score = [0f32; BLOCK];
        // `invert` swaps the sign of Dot/Cosine; L1 and L2 already have it swapped.
        let xor_minus_zeros = match self.distance {
            DistanceType::Dot | DistanceType::Cosine => self.invert,
            DistanceType::L1 | DistanceType::L2 => !self.invert,
        };
        for lane in 0..BLOCK {
            let zeros = self.dim - xor[lane];
            score[lane] = if xor_minus_zeros {
                xor[lane] - zeros
            } else {
                zeros - xor[lane]
            };
        }
        let maxima = &mut maxima[block * BLOCK..][..BLOCK];
        for lane in 0..BLOCK {
            if maxima[lane] < score[lane] {
                maxima[lane] = score[lane];
            }
        }
    }
}
