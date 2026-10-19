#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::AtomicBool;

    use common::ambient;
    use common::types::PointOffsetType;
    use quantization::MultiQuery;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_tq::{self, EncodedVectorsTQ};
    use quantization::encoded_vectors_u8::{self, EncodedVectorsU8, ScalarQuantizationMethod};
    use quantization::turboquant::{TQBits, TQMode, TQRotation};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use rstest::rstest;

    const VECTORS: usize = 300;

    /// Multi-vectors of the storage as `(first vector, vector count)`.
    const MULTI_VECTORS: &[(PointOffsetType, PointOffsetType)] = &[(0, 1), (10, 7), (100, 50)];

    /// Query token counts around the 16-lane block boundary.
    const QUERY_TOKENS: &[usize] = &[1, 15, 16, 17, 40];

    fn random_vectors(rng: &mut StdRng, count: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..count)
            .map(|_| (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect())
            .collect()
    }

    fn parameters(dim: usize, distance_type: DistanceType, invert: bool) -> VectorParameters {
        VectorParameters {
            dim,
            deprecated_count: None,
            distance_type,
            invert,
        }
    }

    /// How close batched scores must be to the per-pair ones.
    #[derive(Clone, Copy)]
    enum Match {
        /// The same value.
        Exact,
        /// Within `1e-6` of the magnitude of the summed terms (`dim` per token
        /// pair, as L2 cancels): the per-pair kernel sums in `f32`, the batched
        /// one exactly.
        Close,
    }

    fn assert_match(matching: Match, got: f32, want: f32, magnitude: f32, what: &str) {
        match matching {
            Match::Exact => assert!(got == want, "{what}: {got} vs {want}"),
            Match::Close => assert!(
                (got - want).abs() <= 1e-6 * magnitude,
                "{what}: {got} vs {want}"
            ),
        }
    }

    fn stored_multi_vectors(stored: impl Fn(PointOffsetType) -> Vec<u8>) -> Vec<Vec<Vec<u8>>> {
        MULTI_VECTORS
            .iter()
            .map(|&(start, count)| (start..start + count).map(&stored).collect())
            .collect()
    }

    /// Batched MaxSim of queries must equal the per-pair `score` loop it replaces.
    fn check_query<E: EncodedVectors>(
        encoded: &E,
        multi_vectors: &[Vec<Vec<u8>>],
        rng: &mut StdRng,
        dim: usize,
        matching: Match,
        label: &str,
    ) {
        for &tokens in QUERY_TOKENS {
            let query: Vec<_> = random_vectors(rng, tokens, dim)
                .iter()
                .map(|v| encoded.encode_query(v))
                .collect();
            let packed = encoded
                .pack_multi_query(&query)
                .unwrap_or_else(|| panic!("{label}: no packed query"));
            let packed = MultiQuery::<E::EncodedQuery>::Packed(packed);
            assert_eq!(packed.vectors_count(), tokens);
            for multi_vector in multi_vectors {
                let mut max_sim = packed.max_sim(|inner, vector| encoded.score(inner, vector));
                let mut maxima = vec![f32::NEG_INFINITY; tokens];
                for vector in multi_vector {
                    for (maximum, query) in maxima.iter_mut().zip(&query) {
                        let score = encoded.score(query, vector);
                        if *maximum < score {
                            *maximum = score;
                        }
                    }
                }
                let magnitude = (tokens * multi_vector.len() * dim) as f32;
                let want: f32 = maxima.into_iter().sum();
                for vector in multi_vector {
                    max_sim.push(vector);
                }
                let got = max_sim.finish();
                let what = format!("{label} query nq={tokens} doc_len={}", multi_vector.len());
                assert_match(matching, got, want, magnitude, &what);
            }
        }
    }

    /// Batched MaxSim of stored multi-vectors against each other must equal the
    /// per-pair `score_internal` loop it replaces.
    fn check_stored<E: EncodedVectors>(
        encoded: &E,
        multi_vectors: &[Vec<Vec<u8>>],
        dim: usize,
        matching: Match,
        label: &str,
    ) {
        for (&(a_start, a_count), a_vectors) in MULTI_VECTORS.iter().zip(multi_vectors) {
            let packed = encoded
                .pack_multi_stored(a_vectors.iter().map(Vec::as_slice))
                .unwrap_or_else(|| panic!("{label}: no packed stored vectors"));
            let packed = MultiQuery::<Infallible>::Packed(packed);
            for (&(b_start, b_count), b_vectors) in MULTI_VECTORS.iter().zip(multi_vectors) {
                let mut max_sim = packed.max_sim(|never, _| match *never {});
                let mut want = 0.0;
                let magnitude = (a_count * b_count) as f32 * dim as f32;
                for a in a_start..a_start + a_count {
                    let mut maximum = f32::NEG_INFINITY;
                    for b in b_start..b_start + b_count {
                        let score = encoded.score_internal(a, b);
                        if score > maximum {
                            maximum = score;
                        }
                    }
                    want += maximum;
                }
                for vector in b_vectors {
                    max_sim.push(vector);
                }
                let got = max_sim.finish();
                let what = format!("{label} internal na={a_count} nb={b_count}");
                assert_match(matching, got, want, magnitude, &what);
            }
        }
    }

    fn simd_cpu() -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            std::is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    }

    /// `pack_*_if_faster` and `encode_multi_query` of one and of 40 vectors: one is never worth packing,
    /// 40 are at dim 128 on the CPUs the crossovers were measured on.
    fn check_gate<E: EncodedVectors>(
        encoded: &E,
        record: impl Fn(PointOffsetType) -> Vec<u8>,
        rng: &mut StdRng,
        dim: usize,
        stored: bool,
    ) {
        let vectors = random_vectors(rng, 40, dim);
        let query: Vec<_> = vectors.iter().map(|v| encoded.encode_query(v)).collect();
        assert!(encoded.pack_multi_query_if_faster(&query[..1]).is_none());
        assert_eq!(
            encoded.pack_multi_query_if_faster(&query).is_some(),
            simd_cpu()
        );
        // The entry point the storages use picks the same kernel.
        let prepared = |count: usize| {
            let query = vectors[..count]
                .iter()
                .map(|v| encoded.encode_query(v))
                .collect();
            encoded.encode_multi_query(query)
        };
        assert!(matches!(prepared(1), MultiQuery::PerPair(_)));
        assert_eq!(matches!(prepared(40), MultiQuery::Packed(_)), simd_cpu());
        let records: Vec<Vec<u8>> = (0..40).map(record).collect();
        let records = || records.iter().map(Vec::as_slice);
        assert!(
            encoded
                .pack_multi_stored_if_faster(records().take(1))
                .is_none()
        );
        assert_eq!(
            encoded.pack_multi_stored_if_faster(records()).is_some(),
            stored && simd_cpu()
        );
    }

    // Scalar int8.

    #[rstest]
    fn packed_u8_matches_pairwise(
        #[values(
            DistanceType::Dot,
            DistanceType::Cosine,
            DistanceType::L2,
            DistanceType::L1
        )]
        distance_type: DistanceType,
        #[values(false, true)] invert: bool,
        #[values(1, 13, 64, 65, 128)] dim: usize,
    ) {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        let vectors = random_vectors(&mut rng, VECTORS, dim);
        let vector_parameters = parameters(dim, distance_type, invert);
        let encoded = EncodedVectorsU8::encode(
            vectors.iter(),
            std::iter::repeat_n(true, VECTORS),
            TestEncodedStorageBuilder::new(
                None,
                encoded_vectors_u8::get_quantized_vector_size(&vector_parameters),
            ),
            &vector_parameters,
            None,
            ScalarQuantizationMethod::Int8,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let label = format!("u8 {distance_type:?} invert={invert} dim={dim}");
        let multi_vectors = stored_multi_vectors(|i| encoded.get_quantized_vector(i).to_vec());
        check_query(
            &encoded,
            &multi_vectors,
            &mut rng,
            dim,
            Match::Exact,
            &label,
        );
        check_stored(&encoded, &multi_vectors, dim, Match::Exact, &label);
    }

    /// Scalar int8 packs past the crossover only.
    #[test]
    fn scalar_packs_past_the_crossover() {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 128;
        let vectors = random_vectors(&mut rng, VECTORS, dim);

        for distance_type in [DistanceType::Dot, DistanceType::L1] {
            let vector_parameters = parameters(dim, distance_type, false);
            let encoded = EncodedVectorsU8::encode(
                vectors.iter(),
                std::iter::repeat_n(true, VECTORS),
                TestEncodedStorageBuilder::new(
                    None,
                    encoded_vectors_u8::get_quantized_vector_size(&vector_parameters),
                ),
                &vector_parameters,
                None,
                ScalarQuantizationMethod::Int8,
                None,
                &AtomicBool::new(false),
            )
            .unwrap();
            check_gate(
                &encoded,
                |i| encoded.get_quantized_vector(i).to_vec(),
                &mut rng,
                dim,
                true,
            );
        }
    }

    // TurboQuant.

    fn encode_tq(
        vectors: &[Vec<f32>],
        vector_parameters: &VectorParameters,
        bits: TQBits,
        mode: TQMode,
    ) -> EncodedVectorsTQ<TestEncodedStorage> {
        EncodedVectorsTQ::encode(
            vectors.iter(),
            std::iter::repeat_n(true, vectors.len()),
            TestEncodedStorageBuilder::new(
                None,
                encoded_vectors_tq::get_quantized_vector_size(vector_parameters, bits, mode),
            ),
            vector_parameters,
            bits,
            mode,
            TQRotation::Padded,
            false,
            1,
            None,
            &AtomicBool::new(false),
        )
        .unwrap()
    }

    #[rstest]
    fn packed_tq_matches_pairwise(
        #[values(
            TQBits::Bits1,
            TQBits::Bits1_5,
            TQBits::Bits2,
            TQBits::Bits4,
            TQBits::Bits8,
            TQBits::Bits16
        )]
        bits: TQBits,
        #[values(TQMode::Normal, TQMode::Plus)] mode: TQMode,
        #[values(DistanceType::Dot, DistanceType::Cosine, DistanceType::L2)]
        distance_type: DistanceType,
        #[values(false, true)] invert: bool,
        #[values(16, 65, 128)] dim: usize,
    ) {
        // The grid widths have no TQ+.
        if matches!(bits, TQBits::Bits8 | TQBits::Bits16) && mode == TQMode::Plus {
            return;
        }
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        let vectors = random_vectors(&mut rng, VECTORS, dim);
        let vector_parameters = parameters(dim, distance_type, invert);
        let encoded = encode_tq(&vectors, &vector_parameters, bits, mode);
        let label = format!("tq {bits:?} {mode:?} {distance_type:?} invert={invert} dim={dim}");
        let multi_vectors = stored_multi_vectors(|i| encoded.get_quantized_vector(i).to_vec());
        // The single-vector 16-bit kernels sum their products in `f32`.
        let matching = match bits {
            TQBits::Bits16 => Match::Close,
            TQBits::Bits1 | TQBits::Bits1_5 | TQBits::Bits2 | TQBits::Bits4 | TQBits::Bits8 => {
                Match::Exact
            }
        };
        check_query(&encoded, &multi_vectors, &mut rng, dim, matching, &label);
        check_stored(&encoded, &multi_vectors, dim, matching, &label);
    }

    /// L1 dequantizes every vector; it keeps the per-pair path.
    #[test]
    fn packed_tq_l1_is_not_packed() {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        let vectors = random_vectors(&mut rng, 20, 16);
        let vector_parameters = parameters(16, DistanceType::L1, false);
        let encoded = encode_tq(&vectors, &vector_parameters, TQBits::Bits4, TQMode::Normal);
        let query = vec![encoded.encode_query(&vectors[0])];
        assert!(encoded.pack_multi_query(&query).is_none());
        let stored = encoded.get_quantized_vector(0).to_vec();
        assert!(
            encoded
                .pack_multi_stored(std::iter::once(stored.as_slice()))
                .is_none()
        );
    }

    /// TurboQuant packs past the crossover only.
    #[test]
    fn turbo_packs_past_the_crossover() {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 128;
        let vectors = random_vectors(&mut rng, VECTORS, dim);
        let vector_parameters = parameters(dim, DistanceType::Dot, false);
        for bits in [
            TQBits::Bits1,
            TQBits::Bits1_5,
            TQBits::Bits2,
            TQBits::Bits4,
            TQBits::Bits8,
            TQBits::Bits16,
        ] {
            let mode = match bits {
                TQBits::Bits8 | TQBits::Bits16 => TQMode::Normal,
                TQBits::Bits1 | TQBits::Bits1_5 | TQBits::Bits2 | TQBits::Bits4 => TQMode::Plus,
            };
            let encoded = encode_tq(&vectors, &vector_parameters, bits, mode);
            check_gate(
                &encoded,
                |i| encoded.get_quantized_vector(i).to_vec(),
                &mut rng,
                dim,
                true,
            );
        }
    }

    /// Narrow TurboQuant codes at a large dim stay pair by pair however many
    /// vectors there are.
    #[test]
    fn packed_if_faster_never_for_wide_tq1() {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(42);
        let dim = 1024;
        let vectors = random_vectors(&mut rng, 64, dim);
        let encoded = encode_tq(
            &vectors,
            &parameters(dim, DistanceType::Dot, false),
            TQBits::Bits1,
            TQMode::Plus,
        );
        let records: Vec<Vec<u8>> = (0..64)
            .map(|i| encoded.get_quantized_vector(i).to_vec())
            .collect();
        assert!(
            encoded
                .pack_multi_stored(records.iter().map(Vec::as_slice))
                .is_some()
        );
        assert!(
            encoded
                .pack_multi_stored_if_faster(records.iter().map(Vec::as_slice))
                .is_none()
        );
    }
}
