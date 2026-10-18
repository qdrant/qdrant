//! MaxSim over TurboQuant multivector records, shared by the writable and the
//! read-only storage: both score through [`TurboQuantizer::encode_multi_query`],
//! [`TurboQuantizer::score_multi_query`] and
//! [`TurboQuantizer::score_symmetric_max_sim`].
//!
//! [`TurboQuantizer::encode_multi_query`]: quantization::turboquant::quantization::TurboQuantizer::encode_multi_query
//! [`TurboQuantizer::score_multi_query`]: quantization::turboquant::quantization::TurboQuantizer::score_multi_query
//! [`TurboQuantizer::score_symmetric_max_sim`]: quantization::turboquant::quantization::TurboQuantizer::score_symmetric_max_sim

use quantization::MultiQuery;
use quantization::turboquant::EncodedQueryTQ;

/// A multi-vector query prepared for a TurboQuant multivector storage.
pub type TurboMultiQuery = MultiQuery<EncodedQueryTQ>;

#[cfg(test)]
mod tests {
    use common::types::ScoreType;
    use quantization::DistanceType;
    use quantization::turboquant::TQBits;
    use quantization::turboquant::quantization::TurboQuantizer;
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
                let packed = quantizer.encode_multi_query(precompute(), invert);
                assert_eq!(
                    matches!(packed, MultiQuery::Packed(_)),
                    quantizer.packed_pays(query.len(), false),
                    "{label}"
                );
                let pairwise = TurboMultiQuery::PerPair(precompute());
                close(
                    quantizer.score_multi_query(&packed, invert, &records_b),
                    quantizer.score_multi_query(&pairwise, invert, &records_b),
                );

                let size = quantizer.quantized_size();
                let mut want = 0.0;
                for a in records_a.chunks_exact(size) {
                    let mut maximum = ScoreType::NEG_INFINITY;
                    for b in records_b.chunks_exact(size) {
                        let sim = quantizer.score_symmetric(a, b);
                        maximum = maximum.max(if invert { -sim } else { sim });
                    }
                    want += maximum;
                }
                close(
                    quantizer.score_symmetric_max_sim(&records_a, &records_b, invert),
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
