//! MaxSim over TurboQuant multivector records, shared by the writable and the
//! read-only storage.

use common::types::ScoreType;
use quantization::PackedMultiQuery;
use quantization::turboquant::EncodedQueryTQ;
use quantization::turboquant::quantization::TurboQuantizer;
use smallvec::{SmallVec, smallvec};

/// A multi-vector query precomputed for a TurboQuant multivector storage.
pub struct TurboMultiQuery {
    /// One precomputed query per inner vector.
    vectors: Vec<EncodedQueryTQ>,
    /// `vectors` packed for the batched MaxSim kernel; `None` for Manhattan,
    /// which has none.
    packed: Option<PackedMultiQuery>,
}

impl TurboMultiQuery {
    /// `invert` negates scores, as the storage does for distances.
    pub(crate) fn new(
        quantizer: &TurboQuantizer,
        invert: bool,
        vectors: Vec<EncodedQueryTQ>,
    ) -> Self {
        let packed = quantizer
            .pack_multi_query_if_faster(&vectors, invert, false)
            .map(PackedMultiQuery::TQ);
        Self { vectors, packed }
    }

    /// Number of inner vectors.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// MaxSim against one point's concatenated encoded records.
    pub(crate) fn score_records(
        &self,
        quantizer: &TurboQuantizer,
        invert: bool,
        records: &[u8],
    ) -> ScoreType {
        let records = records.chunks_exact(quantizer.quantized_size());
        if let Some(packed) = &self.packed {
            let mut max_sim = packed.max_sim();
            for record in records {
                max_sim.push(record);
            }
            return max_sim.finish();
        }

        let mut max_sim: SmallVec<[_; 8]> = smallvec![ScoreType::NEG_INFINITY; self.vectors.len()];
        for bytes in records {
            for (qi, inner_query) in self.vectors.iter().enumerate() {
                let sim = signed(invert, quantizer.score_precomputed(inner_query, bytes));
                if max_sim[qi] < sim {
                    max_sim[qi] = sim;
                }
            }
        }
        max_sim.into_iter().sum()
    }
}

/// Symmetric MaxSim between two points' concatenated encoded records.
pub(crate) fn score_records_symmetric(
    quantizer: &TurboQuantizer,
    invert: bool,
    records_a: &[u8],
    records_b: &[u8],
) -> ScoreType {
    let quantized_size = quantizer.quantized_size();
    if let Some(packed) =
        quantizer.pack_multi_stored_if_faster(records_a.chunks_exact(quantized_size), invert)
    {
        let packed = PackedMultiQuery::TQ(packed);
        let mut max_sim = packed.max_sim();
        for bytes_b in records_b.chunks_exact(quantized_size) {
            max_sim.push(bytes_b);
        }
        return max_sim.finish();
    }

    let mut sum = 0.0;
    for bytes_a in records_a.chunks_exact(quantized_size) {
        let mut max_sim = ScoreType::NEG_INFINITY;
        for bytes_b in records_b.chunks_exact(quantized_size) {
            let sim = signed(invert, quantizer.score_symmetric(bytes_a, bytes_b));
            if sim > max_sim {
                max_sim = sim;
            }
        }
        sum += max_sim;
    }
    sum
}

fn signed(invert: bool, score: f32) -> ScoreType {
    if invert { -score } else { score }
}

#[cfg(test)]
mod tests {
    use quantization::DistanceType;
    use quantization::turboquant::TQBits;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;
    use crate::vector_storage::turbo::shared::{TQDT_MODE, TQDT_ROTATION};

    /// The batched paths against the per-pair ones, for the datatype widths.
    #[test]
    fn packed_matches_pairwise() {
        let _scope = common::ambient::test_guard();
        let dim = 48;
        let mut rng = StdRng::seed_from_u64(42);
        let mut random = |count: usize| -> Vec<Vec<f32>> {
            (0..count)
                .map(|_| (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect())
                .collect()
        };
        for bits in [TQBits::Bits4, TQBits::Bits8, TQBits::Bits16] {
            for (distance, invert) in [
                (DistanceType::Cosine, false),
                (DistanceType::Dot, false),
                (DistanceType::L2, true),
            ] {
                let quantizer =
                    TurboQuantizer::new(dim, bits, TQDT_MODE, distance, TQDT_ROTATION, None);
                let mut buf = vec![0.0; quantizer.get_padded_dim()];
                let mut encode = |vectors: Vec<Vec<f32>>| -> Vec<u8> {
                    vectors
                        .iter()
                        .flat_map(|vector| quantizer.quantize(vector, &mut buf))
                        .collect()
                };
                let records_a = encode(random(19));
                let records_b = encode(random(33));
                let label = format!("{bits:?} {distance:?}");
                // 16-bit single-vector kernels sum in `f32`, the batched one exactly.
                let close = |got: f32, want: f32| {
                    if bits == TQBits::Bits16 {
                        assert!(
                            (got - want).abs() <= 1e-5 * want.abs().max(dim as f32),
                            "{label}: {got} vs {want}"
                        );
                    } else {
                        assert_eq!(got, want, "{label}");
                    }
                };

                let query = random(21);
                let precompute = || -> Vec<_> {
                    query
                        .iter()
                        .map(|vector| quantizer.precompute_query(vector))
                        .collect()
                };
                let packed = TurboMultiQuery::new(&quantizer, invert, precompute());
                assert_eq!(
                    packed.packed.is_some(),
                    quantizer.packed_pays(query.len(), false),
                    "{label}"
                );
                let pairwise = TurboMultiQuery {
                    vectors: precompute(),
                    packed: None,
                };
                close(
                    packed.score_records(&quantizer, invert, &records_b),
                    pairwise.score_records(&quantizer, invert, &records_b),
                );

                let size = quantizer.quantized_size();
                let mut want = 0.0;
                for a in records_a.chunks_exact(size) {
                    let mut maximum = ScoreType::NEG_INFINITY;
                    for b in records_b.chunks_exact(size) {
                        maximum = maximum.max(signed(invert, quantizer.score_symmetric(a, b)));
                    }
                    want += maximum;
                }
                close(
                    score_records_symmetric(&quantizer, invert, &records_a, &records_b),
                    want,
                );
            }
        }
    }

    /// The storage packs only where the crossover says packing is faster.
    #[test]
    fn packs_past_the_crossover_only() {
        let _scope = common::ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        for (dim, bits) in [(48, TQBits::Bits4), (1024, TQBits::Bits1)] {
            let quantizer =
                TurboQuantizer::new(dim, bits, TQDT_MODE, DistanceType::Dot, TQDT_ROTATION, None);
            let vectors: Vec<Vec<f32>> = (0..40)
                .map(|_| (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect())
                .collect();
            let mut buf = vec![0.0; quantizer.get_padded_dim()];
            let records: Vec<Vec<u8>> = vectors
                .iter()
                .map(|vector| quantizer.quantize(vector, &mut buf))
                .collect();
            let records = || records.iter().map(Vec::as_slice);
            let queries = || -> Vec<_> {
                vectors
                    .iter()
                    .map(|v| quantizer.precompute_query(v))
                    .collect()
            };
            assert!(
                quantizer
                    .pack_multi_stored_if_faster(records().take(1), false)
                    .is_none()
            );
            assert!(
                quantizer
                    .pack_multi_query_if_faster(&queries()[..1], false, false)
                    .is_none()
            );
            // 40 vectors pass the crossover of 4-bit codes at dim 48 on the
            // measured CPUs; 1-bit codes at dim 1024 never do.
            let pays = dim == 48 && quantizer.packed_pays(40, true);
            assert_eq!(
                quantizer
                    .pack_multi_stored_if_faster(records(), false)
                    .is_some(),
                pays
            );
            assert!(quantizer.pack_multi_stored(records(), false).is_some());
            assert_eq!(
                quantizer
                    .pack_multi_query_if_faster(&queries(), false, false)
                    .is_some(),
                dim == 48 && quantizer.packed_pays(40, false)
            );
        }
    }
}
