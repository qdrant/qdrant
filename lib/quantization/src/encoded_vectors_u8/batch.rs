//! SQ8 query batches. A stored block is read once, then each vector load feeds four queries.

use common::ambient::hw::HwMetric;
use common::types::PointOffsetType;

use super::{EncodedQueryU8, EncodedVectorsU8};
use crate::{DistanceType, EncodedStorage, EncodedVectors};

/// Encoded queries and reusable scratch space for scoring one block of stored vectors.
///
/// The score matrix is query-major. Scratch grows to the largest point block passed to
/// [`EncodedVectorsU8::score_query_batch`], independently of the collection size.
pub struct EncodedQueryBatchU8 {
    queries: Vec<EncodedQueryU8>,
    dim: usize,
    codes: Vec<u8>,
    offsets: Vec<f32>,
    dots: Vec<i32>,
    kernel: Kernel,
}

#[derive(Clone, Copy)]
enum Kernel {
    #[cfg(target_arch = "x86_64")]
    Avx512Vnni,
    #[cfg(target_arch = "x86_64")]
    AvxVnni,
    #[cfg(target_arch = "aarch64")]
    NeonDot,
    #[cfg(target_arch = "aarch64")]
    NeonI8mm,
    Scalar,
}

impl Kernel {
    fn best() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512vnni")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512f")
            {
                return Self::Avx512Vnni;
            }
            if is_x86_feature_detected!("avxvnni") && is_x86_feature_detected!("avx2") {
                return Self::AvxVnni;
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("i8mm") {
                return Self::NeonI8mm;
            }
            if std::arch::is_aarch64_feature_detected!("dotprod") {
                return Self::NeonDot;
            }
        }
        Self::Scalar
    }

    fn score(self, queries: &[EncodedQueryU8], vectors: &[u8], dim: usize, scores: &mut [i32]) {
        match self {
            // SAFETY: these variants are only constructed after runtime feature detection.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512Vnni => unsafe { score_avx512(queries, vectors, dim, scores) },
            #[cfg(target_arch = "x86_64")]
            Self::AvxVnni => unsafe { score_avx(queries, vectors, dim, scores) },
            #[cfg(target_arch = "aarch64")]
            Self::NeonDot => unsafe { score_neon(queries, vectors, dim, scores) },
            #[cfg(target_arch = "aarch64")]
            Self::NeonI8mm => unsafe { score_i8mm(queries, vectors, dim, scores) },
            Self::Scalar => score_scalar(queries, vectors, dim, scores),
        }
    }
}

impl<TStorage: EncodedStorage> EncodedVectorsU8<TStorage> {
    /// Prepare a batch only when its distance and CPU have a blocked dot-product kernel.
    /// Single-query, Manhattan, and unsupported CPUs keep their existing scorer.
    pub fn query_batch<I>(&self, queries: I) -> Option<EncodedQueryBatchU8>
    where
        I: IntoIterator<Item = EncodedQueryU8>,
        I::IntoIter: ExactSizeIterator,
    {
        let queries = queries.into_iter();
        if queries.len() < 2
            || self.metadata.actual_dim() == 0
            || self.metadata.vector_parameters().distance_type == DistanceType::L1
        {
            return None;
        }
        let kernel = Kernel::best();
        if matches!(kernel, Kernel::Scalar) {
            return None;
        }
        // Keep query preprocessing lazy: falling back must not encode the queries twice.
        let queries: Vec<_> = queries.collect();
        assert!(
            queries
                .iter()
                .all(|query| query.encoded_query.len() == self.metadata.actual_dim())
        );
        Some(EncodedQueryBatchU8 {
            queries,
            dim: self.metadata.actual_dim(),
            codes: Vec::new(),
            offsets: Vec::new(),
            dots: Vec::new(),
            kernel,
        })
    }

    /// Score the prepared queries against `ids`, preserving their order in each score row.
    /// Storage callbacks may arrive out of order; their position determines the matrix column.
    pub fn score_query_batch(
        &self,
        batch: &mut EncodedQueryBatchU8,
        ids: &[PointOffsetType],
        scores: &mut [f32],
    ) -> std::io::Result<()> {
        let dim = self.metadata.actual_dim();
        assert_eq!(batch.dim, dim, "query batch dimension must match storage");
        assert_eq!(scores.len(), batch.queries.len() * ids.len());
        if ids.is_empty() {
            return Ok(());
        }
        batch.codes.resize(ids.len() * dim, 0);
        batch.offsets.resize(ids.len(), 0.0);
        batch.dots.resize(scores.len(), 0);
        // Keep the storage's pipelined read/error path. The same bytes serve every query.
        self.for_each_batch(ids, |i, bytes| {
            let (offset, codes) = Self::parse_vec_data(&bytes);
            batch.codes[i * dim..(i + 1) * dim].copy_from_slice(&codes[..dim]);
            batch.offsets[i] = offset;
        })?;
        HwMetric::Cpu.bump(self.metadata.vector_parameters().dim * scores.len());
        batch
            .kernel
            .score(&batch.queries, &batch.codes, dim, &mut batch.dots);
        for (q, query) in batch.queries.iter().enumerate() {
            for (v, &offset) in batch.offsets.iter().enumerate() {
                let i = q * ids.len() + v;
                // Integer reduction and f32 conversion happen once, in the same order as score().
                scores[i] =
                    self.metadata
                        .postprocess_score(batch.dots[i] as f32, query.offset, offset);
            }
        }
        Ok(())
    }
}

fn score_scalar(queries: &[EncodedQueryU8], vectors: &[u8], dim: usize, scores: &mut [i32]) {
    let count = vectors.len() / dim;
    for (q, query) in queries.iter().enumerate() {
        for (v, vector) in vectors.chunks_exact(dim).enumerate() {
            scores[q * count + v] = query
                .encoded_query
                .iter()
                .zip(vector)
                .map(|(&a, &b)| i32::from(a) * i32::from(b))
                .sum();
        }
    }
}

/// Codes are 0..=127, so unsigned-by-signed byte products and i32 sums are exact
/// up to Qdrant's maximum dimension. The last 16/32/48 bytes use masked loads.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
unsafe fn score_avx512(queries: &[EncodedQueryU8], vectors: &[u8], dim: usize, scores: &mut [i32]) {
    use std::arch::x86_64::*;
    let count = vectors.len() / dim;
    unsafe {
        for (block, queries) in queries.chunks(4).enumerate() {
            for (v, vector) in vectors.chunks_exact(dim).enumerate() {
                let mut accum = [_mm512_setzero_si512(); 4];
                for d in (0..dim).step_by(64) {
                    let remaining = (dim - d).min(64);
                    let mask = u64::MAX >> (64 - remaining);
                    let code = _mm512_maskz_loadu_epi8(mask, vector.as_ptr().add(d).cast());
                    for (q, query) in queries.iter().enumerate() {
                        let query = _mm512_maskz_loadu_epi8(
                            mask,
                            query.encoded_query.as_ptr().add(d).cast(),
                        );
                        accum[q] = _mm512_dpbusd_epi32(accum[q], code, query);
                    }
                }
                for q in 0..queries.len() {
                    scores[(block * 4 + q) * count + v] = _mm512_reduce_add_epi32(accum[q]);
                }
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avxvnni")]
unsafe fn score_avx(queries: &[EncodedQueryU8], vectors: &[u8], dim: usize, scores: &mut [i32]) {
    use std::arch::x86_64::*;
    let count = vectors.len() / dim;
    unsafe {
        for (block, queries) in queries.chunks(4).enumerate() {
            for (v, vector) in vectors.chunks_exact(dim).enumerate() {
                let mut accum = [_mm256_setzero_si256(); 4];
                let full_dim = dim / 32 * 32;
                for d in (0..full_dim).step_by(32) {
                    let code = _mm256_loadu_si256(vector.as_ptr().add(d).cast());
                    for (q, query) in queries.iter().enumerate() {
                        let query = _mm256_loadu_si256(query.encoded_query.as_ptr().add(d).cast());
                        accum[q] = _mm256_dpbusd_avx_epi32(accum[q], code, query);
                    }
                }
                if full_dim < dim {
                    let code = _mm256_zextsi128_si256(_mm_loadu_si128(
                        vector.as_ptr().add(full_dim).cast(),
                    ));
                    for (q, query) in queries.iter().enumerate() {
                        let query = _mm256_zextsi128_si256(_mm_loadu_si128(
                            query.encoded_query.as_ptr().add(full_dim).cast(),
                        ));
                        accum[q] = _mm256_dpbusd_avx_epi32(accum[q], code, query);
                    }
                }
                for q in 0..queries.len() {
                    let mut sum = _mm_add_epi32(
                        _mm256_castsi256_si128(accum[q]),
                        _mm256_extracti128_si256::<1>(accum[q]),
                    );
                    sum = _mm_hadd_epi32(sum, sum);
                    sum = _mm_hadd_epi32(sum, sum);
                    scores[(block * 4 + q) * count + v] = _mm_cvtsi128_si32(sum);
                }
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon,dotprod")]
unsafe fn score_neon(queries: &[EncodedQueryU8], vectors: &[u8], dim: usize, scores: &mut [i32]) {
    use std::arch::aarch64::*;
    let count = vectors.len() / dim;
    unsafe {
        for (block, queries) in queries.chunks(4).enumerate() {
            for (v, vector) in vectors.chunks_exact(dim).enumerate() {
                let mut accum = [vdupq_n_s32(0); 4];
                for d in (0..dim).step_by(16) {
                    let code = vld1q_s8(vector.as_ptr().add(d).cast());
                    for (q, query) in queries.iter().enumerate() {
                        let query = vld1q_s8(query.encoded_query.as_ptr().add(d).cast());
                        accum[q] = vdotq_s32(accum[q], code, query);
                    }
                }
                for q in 0..queries.len() {
                    scores[(block * 4 + q) * count + v] = vaddvq_s32(accum[q]);
                }
            }
        }
    }
}

/// Four queries by four stored vectors. SMMLA multiplies two contiguous 8-byte query
/// rows by two contiguous 8-byte vector columns into [q0v0, q0v1, q1v0, q1v1].
/// Each loaded vector pair is shared by both query pairs; incomplete pairs are zero-filled.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon,i8mm")]
unsafe fn score_i8mm(queries: &[EncodedQueryU8], vectors: &[u8], dim: usize, scores: &mut [i32]) {
    use std::arch::aarch64::*;
    let count = vectors.len() / dim;
    unsafe {
        for (block, queries) in queries.chunks(4).enumerate() {
            for first in (0..count).step_by(4) {
                let width = (count - first).min(4);
                let mut accum = [[vdupq_n_s32(0); 2]; 2];
                for d in (0..dim).step_by(8) {
                    let mut codes = [vdupq_n_s8(0); 2];
                    for (pair, code) in codes.iter_mut().enumerate().take(width.div_ceil(2)) {
                        let v = first + pair * 2;
                        let low = vld1_s8(vectors.as_ptr().add(v * dim + d).cast());
                        let high = if pair * 2 + 1 < width {
                            vld1_s8(vectors.as_ptr().add((v + 1) * dim + d).cast())
                        } else {
                            vdup_n_s8(0)
                        };
                        *code = vcombine_s8(low, high);
                    }
                    for (pair, sums) in accum.iter_mut().enumerate().take(queries.len().div_ceil(2))
                    {
                        let q = pair * 2;
                        let low = vld1_s8(queries[q].encoded_query.as_ptr().add(d).cast());
                        let high = if q + 1 < queries.len() {
                            vld1_s8(queries[q + 1].encoded_query.as_ptr().add(d).cast())
                        } else {
                            vdup_n_s8(0)
                        };
                        let query = vcombine_s8(low, high);
                        for (sum, &code) in sums.iter_mut().zip(&codes).take(width.div_ceil(2)) {
                            // vmmlaq_s32 is still unstable in Rust. This is its single
                            // instruction equivalent, enabled only after detecting i8mm.
                            std::arch::asm!(
                                "smmla {sum:v}.4s, {query:v}.16b, {code:v}.16b",
                                sum = inout(vreg) *sum,
                                query = in(vreg) query,
                                code = in(vreg) code,
                                options(pure, nomem, nostack),
                            );
                        }
                    }
                }
                for (pair, sums) in accum.iter().enumerate().take(queries.len().div_ceil(2)) {
                    for (vpair, &sum) in sums.iter().enumerate().take(width.div_ceil(2)) {
                        let mut lanes = [0; 4];
                        vst1q_s32(lanes.as_mut_ptr(), sum);
                        for q in 0..(queries.len() - pair * 2).min(2) {
                            for v in 0..(width - vpair * 2).min(2) {
                                scores
                                    [(block * 4 + pair * 2 + q) * count + first + vpair * 2 + v] =
                                    lanes[q * 2 + v];
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;

    use common::ambient;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::VectorParameters;
    use crate::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use crate::encoded_vectors_u8::{ScalarQuantizationMethod, get_quantized_vector_size};

    fn kernels() -> Vec<Kernel> {
        let mut kernels = vec![Kernel::Scalar];
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx512vnni")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512f")
            {
                kernels.push(Kernel::Avx512Vnni);
            }
            if is_x86_feature_detected!("avxvnni") && is_x86_feature_detected!("avx2") {
                kernels.push(Kernel::AvxVnni);
            }
        }
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("dotprod") {
            kernels.push(Kernel::NeonDot);
        }
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("i8mm") {
            kernels.push(Kernel::NeonI8mm);
        }
        kernels
    }

    fn encoded(
        dim: usize,
        distance_type: DistanceType,
        invert: bool,
    ) -> EncodedVectorsU8<TestEncodedStorage> {
        let mut rng = StdRng::seed_from_u64(42);
        let vectors: Vec<Vec<f32>> = (0..67)
            .map(|_| (0..dim).map(|_| rng.random_range(-5.0..10.0)).collect())
            .collect();
        let parameters = VectorParameters {
            dim,
            deprecated_count: None,
            distance_type,
            invert,
        };
        EncodedVectorsU8::encode(
            vectors.iter(),
            std::iter::repeat_n(true, vectors.len()),
            TestEncodedStorageBuilder::new(None, get_quantized_vector_size(&parameters)),
            &parameters,
            None,
            ScalarQuantizationMethod::Int8,
            None,
            &AtomicBool::new(false),
        )
        .unwrap()
    }

    #[test]
    fn query_batch_matches_independent_scores_bitwise() {
        let _scope = ambient::test_guard();
        for dim in [1, 16, 17, 31, 32, 48, 63, 64, 65, 127, 512, 1537] {
            for distance in [DistanceType::Dot, DistanceType::Cosine, DistanceType::L2] {
                for invert in [false, true] {
                    let vectors = encoded(dim, distance, invert);
                    for count in [2, 3, 4, 5, 8, 17, 64] {
                        let queries: Vec<_> = (0..count)
                            .map(|q| {
                                vectors.encode_query(
                                    &(0..dim)
                                        .map(|d| ((q * 17 + d * 13) % 29) as f32 - 10.0)
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .collect();
                        let ids = [66, 0, 3, 3, 21, 1, 65];
                        let expected: Vec<_> = queries
                            .iter()
                            .flat_map(|query| {
                                let mut scores = vec![0.0; ids.len()];
                                vectors.score_points(query, &ids, &mut scores).unwrap();
                                scores
                            })
                            .map(f32::to_bits)
                            .collect();
                        let mut batch = EncodedQueryBatchU8 {
                            queries,
                            dim: vectors.metadata.actual_dim(),
                            codes: vec![],
                            offsets: vec![],
                            dots: vec![],
                            kernel: Kernel::Scalar,
                        };
                        for kernel in kernels() {
                            batch.kernel = kernel;
                            let mut scores = vec![0.0; expected.len()];
                            vectors
                                .score_query_batch(&mut batch, &ids, &mut scores)
                                .unwrap();
                            assert_eq!(
                                scores.into_iter().map(f32::to_bits).collect::<Vec<_>>(),
                                expected,
                                "dim {dim}, queries {count}, distance {distance:?}, invert {invert}"
                            );
                            vectors.score_query_batch(&mut batch, &[], &mut []).unwrap();
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn maximum_codes_do_not_saturate() {
        let dim = 65_536;
        let queries: Vec<_> = (0..5)
            .map(|_| EncodedQueryU8 {
                offset: 0.0,
                encoded_query: vec![127; dim],
            })
            .collect();
        let vectors = vec![127; dim * 3];
        for kernel in kernels() {
            let mut scores = vec![0; 15];
            kernel.score(&queries, &vectors, dim, &mut scores);
            assert_eq!(scores, vec![127 * 127 * dim as i32; 15]);
        }
    }

    #[test]
    fn rejects_batch_from_different_dimension_before_kernel() {
        let _scope = ambient::test_guard();
        let vectors = encoded(16, DistanceType::Dot, false);
        let other = encoded(512, DistanceType::Dot, false);
        let queries = vec![
            vectors.encode_query(&[1.0; 16]),
            vectors.encode_query(&[2.0; 16]),
        ];
        let mut batch = EncodedQueryBatchU8 {
            queries,
            dim: 16,
            codes: vec![],
            offsets: vec![],
            dots: vec![],
            kernel: Kernel::best(),
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            other.score_query_batch(&mut batch, &[0], &mut [0.0; 2])
        }));
        assert!(result.is_err());
    }

    #[test]
    fn fallback_does_not_preprocess_queries() {
        let _scope = ambient::test_guard();
        let vectors = encoded(17, DistanceType::L1, false);
        let queries = (0..8).map(|_| -> EncodedQueryU8 { panic!("fallback must stay lazy") });
        assert!(vectors.query_batch(queries).is_none());
        let vectors = encoded(17, DistanceType::Dot, false);
        let queries =
            (0..1).map(|_| -> EncodedQueryU8 { panic!("single-query fallback must stay lazy") });
        assert!(vectors.query_batch(queries).is_none());
    }

    #[test]
    fn single_query_and_l1_use_independent_scorers() {
        let _scope = ambient::test_guard();
        let vectors = encoded(17, DistanceType::Dot, false);
        assert!(vectors.query_batch(vec![]).is_none());
        assert!(
            vectors
                .query_batch(vec![vectors.encode_query(&[1.0; 17])])
                .is_none()
        );
        let vectors = encoded(17, DistanceType::L1, false);
        assert!(
            vectors
                .query_batch(vec![
                    vectors.encode_query(&[1.0; 17]),
                    vectors.encode_query(&[2.0; 17])
                ])
                .is_none()
        );
    }

    /// The batch must use the fallible pipelined interface, not get_vector_data.
    struct CallbackStorage {
        inner: TestEncodedStorage,
        fail: bool,
    }
    impl EncodedStorage for CallbackStorage {
        fn is_in_ram_or_mmap() -> bool {
            false
        }
        fn is_cold(&self) -> bool {
            true
        }
        fn vectors_count(&self) -> usize {
            self.inner.vectors_count()
        }
        fn heap_size_bytes(&self) -> usize {
            self.inner.heap_size_bytes()
        }
        fn get_vector_data(&self, _: PointOffsetType) -> Cow<'_, [u8]> {
            panic!("must use for_each_batch")
        }
        fn get_vector_data_opt(&self, _: PointOffsetType) -> Option<Cow<'_, [u8]>> {
            panic!("must use for_each_batch")
        }
        fn for_each_batch(
            &self,
            ids: &[PointOffsetType],
            mut callback: impl FnMut(usize, Cow<'_, [u8]>),
        ) -> std::io::Result<()> {
            if self.fail {
                return Err(std::io::Error::other("injected read error"));
            }
            for (i, &id) in ids.iter().enumerate().rev() {
                callback(i, self.inner.get_vector_data(id));
            }
            Ok(())
        }
        fn files(&self) -> Vec<PathBuf> {
            vec![]
        }
        fn immutable_files(&self) -> Vec<PathBuf> {
            vec![]
        }
    }

    #[test]
    fn query_batch_preserves_callback_order_and_read_errors() {
        let _scope = ambient::test_guard();
        let vectors = encoded(49, DistanceType::L2, true);
        let queries = vec![
            vectors.encode_query(&[1.0; 49]),
            vectors.encode_query(&[2.0; 49]),
        ];
        let ids = [65, 0, 1, 4, 8];
        let expected: Vec<_> = queries
            .iter()
            .flat_map(|query| {
                let mut scores = vec![0.0; ids.len()];
                vectors.score_points(query, &ids, &mut scores).unwrap();
                scores
            })
            .collect();
        let mut batch = EncodedQueryBatchU8 {
            queries,
            dim: vectors.metadata.actual_dim(),
            codes: vec![],
            offsets: vec![],
            dots: vec![],
            kernel: Kernel::best(),
        };
        let mut vectors = EncodedVectorsU8 {
            encoded_vectors: CallbackStorage {
                inner: vectors.encoded_vectors,
                fail: false,
            },
            metadata: vectors.metadata,
            metadata_path: None,
        };
        let mut scores = vec![0.0; expected.len()];
        vectors
            .score_query_batch(&mut batch, &ids, &mut scores)
            .unwrap();
        assert_eq!(scores, expected);
        vectors.encoded_vectors.fail = true;
        assert_eq!(
            vectors
                .score_query_batch(&mut batch, &ids, &mut scores)
                .unwrap_err()
                .to_string(),
            "injected read error"
        );
    }
}
