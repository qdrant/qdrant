//! Batched MaxSim for product-quantized multi-vectors.
//!
//! A PQ score is a sum of lookups, one per sub-vector, into the query's
//! distance table. Here the tables of all query tokens are interleaved so that
//! one table row holds the entry of [`BLOCK`] query tokens:
//!
//! ```text
//! lut[((block * subvectors + sub) * centroids + centroid) * BLOCK + lane]
//! ```
//!
//! Each stored vector then costs one row load per sub-vector and block instead
//! of one scattered lookup per sub-vector and query token. The additions are
//! grouped like the single-vector kernel of the running CPU (four running sums
//! on SSE and NEON, folded the same way), so scores are bit-identical.

use common::ambient::hw::HwMetric;

use super::BLOCK;

pub struct PackedMultiQueryPQ {
    lut: Vec<f32>,
    subvectors: usize,
    centroids: usize,
    vectors_count: usize,
    lanes: usize,
    summation: Summation,
    /// Hardware counter units per pushed vector (`0` to count nothing).
    cpu_per_vector: usize,
}

/// How the single-vector kernel of this CPU adds up the lookups.
#[derive(Clone, Copy)]
enum Summation {
    /// One running sum.
    Sequential,
    /// Four running sums over groups of four sub-vectors, folded as
    /// `(s0 + s2) + (s1 + s3)` (SSE) or `(s0 + s1) + (s2 + s3)` (NEON), then the
    /// remaining sub-vectors added in order.
    Four { pairwise: bool },
}

impl Summation {
    fn detect() -> Self {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if std::is_x86_feature_detected!("sse4.1") {
            return Summation::Four { pairwise: false };
        }
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return Summation::Four { pairwise: true };
        }
        Summation::Sequential
    }
}

impl PackedMultiQueryPQ {
    /// `queries` yields every query token's lookup table, `subvectors · centroids`
    /// values, sub-vector major.
    pub(crate) fn new<'a>(
        queries: impl ExactSizeIterator<Item = &'a [f32]>,
        subvectors: usize,
        centroids: usize,
        cpu_per_vector: usize,
    ) -> Self {
        let vectors_count = queries.len();
        let lanes = vectors_count.div_ceil(BLOCK) * BLOCK;
        let mut lut = vec![0.0; lanes * subvectors * centroids];
        for (token, table) in queries.enumerate() {
            debug_assert_eq!(table.len(), subvectors * centroids);
            let (block, lane) = (token / BLOCK, token % BLOCK);
            for (row, &value) in table.iter().enumerate() {
                lut[(block * subvectors * centroids + row) * BLOCK + lane] = value;
            }
        }
        Self {
            lut,
            subvectors,
            centroids,
            vectors_count,
            lanes,
            summation: Summation::detect(),
            cpu_per_vector,
        }
    }

    pub fn vectors_count(&self) -> usize {
        self.vectors_count
    }

    pub(crate) fn lanes(&self) -> usize {
        self.lanes
    }

    /// Fold one stored vector, one centroid index per sub-vector, into the running
    /// per-token maxima.
    pub(crate) fn push(&self, maxima: &mut [f32], vector: &[u8]) {
        assert!(vector.len() >= self.subvectors);
        assert!(maxima.len() >= self.lanes);
        if self.cpu_per_vector > 0 {
            HwMetric::Cpu.bump(self.cpu_per_vector * self.vectors_count);
        }
        let codes = &vector[..self.subvectors];
        let block_len = self.subvectors * self.centroids * BLOCK;
        for block in 0..self.lanes / BLOCK {
            let lut = &self.lut[block * block_len..][..block_len];
            let row = |sub: usize, code: u8| -> &[f32; BLOCK] {
                lut[(sub * self.centroids + usize::from(code)) * BLOCK..][..BLOCK]
                    .try_into()
                    .unwrap()
            };
            let mut sum = [0f32; BLOCK];
            let mut rest = 0;
            if let Summation::Four { pairwise } = self.summation {
                let mut sums = [[0f32; BLOCK]; 4];
                let (groups, _) = codes.as_chunks::<4>();
                for (group, codes) in groups.iter().enumerate() {
                    for (k, &code) in codes.iter().enumerate() {
                        let row = row(group * 4 + k, code);
                        for lane in 0..BLOCK {
                            sums[k][lane] += row[lane];
                        }
                    }
                }
                for (lane, sum) in sum.iter_mut().enumerate() {
                    let [s0, s1, s2, s3] = sums.map(|sum| sum[lane]);
                    *sum = if pairwise {
                        (s0 + s1) + (s2 + s3)
                    } else {
                        (s0 + s2) + (s1 + s3)
                    };
                }
                rest = groups.len() * 4;
            }
            for (sub, &code) in codes.iter().enumerate().skip(rest) {
                let row = row(sub, code);
                for lane in 0..BLOCK {
                    sum[lane] += row[lane];
                }
            }
            let maxima = &mut maxima[block * BLOCK..][..BLOCK];
            for lane in 0..BLOCK {
                if maxima[lane] < sum[lane] {
                    maxima[lane] = sum[lane];
                }
            }
        }
    }
}
