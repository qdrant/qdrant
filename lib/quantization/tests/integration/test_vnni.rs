#[cfg(test)]
#[cfg(target_arch = "x86_64")]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8::{
        self, EncodedQueryU8, EncodedVectorsU8, ScalarQuantizationMethod,
    };
    use rand::{RngExt, SeedableRng};
    use rstest::rstest;

    type Encoded = EncodedVectorsU8<TestEncodedStorage>;

    /// The VNNI dot kernels produce the same integer as the scalar one, so the
    /// scores must match `score_point_simple` exactly, for every distance.
    fn check(
        distance_type: DistanceType,
        score: fn(&Encoded, &EncodedQueryU8, &[u8]) -> f32,
        score_internal: fn(&Encoded, u32, u32) -> f32,
    ) {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        // Each tail length of both kernels, plus a dimension needing padding.
        for vector_dim in [16, 32, 48, 64, 65, 112, 128, 1536] {
            let vectors_count = 17;
            let vector_data: Vec<Vec<f32>> = (0..vectors_count)
                .map(|_| (0..vector_dim).map(|_| rng.random()).collect())
                .collect();
            let query: Vec<f32> = (0..vector_dim).map(|_| rng.random()).collect();

            let vector_parameters = VectorParameters {
                dim: vector_dim,
                deprecated_count: None,
                distance_type,
                invert: false,
            };
            let quantized_vector_size =
                encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
            let encoded = EncodedVectorsU8::encode(
                vector_data.iter(),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters,
                vectors_count,
                None,
                ScalarQuantizationMethod::Int8,
                None,
                &AtomicBool::new(false),
            )
            .unwrap();
            let query_u8 = encoded.encode_query(&query);

            for i in 0..vectors_count as u32 {
                let quantized_vector = encoded.get_quantized_vector(i);
                let j = (i + 1) % vectors_count as u32;
                let got = score(&encoded, &query_u8, &quantized_vector);
                let got_internal = score_internal(&encoded, i, j);
                let want = encoded.score_point_simple(&query_u8, &quantized_vector);
                let want_internal = encoded.score_point_simple_internal(i, j);
                assert_eq!(got, want, "dim={vector_dim}, i={i}");
                assert_eq!(
                    got_internal, want_internal,
                    "dim={vector_dim}, i={i}, j={j}"
                );
            }
        }
    }

    #[rstest]
    fn test_avx512_vnni_matches_simple(
        #[values(
            DistanceType::Dot,
            DistanceType::Cosine,
            DistanceType::L2,
            DistanceType::L1
        )]
        distance_type: DistanceType,
    ) {
        if !(is_x86_feature_detected!("avx512vnni") && is_x86_feature_detected!("avx512bw")) {
            println!("avx512vnni test skipped");
            return;
        }
        check(
            distance_type,
            Encoded::score_point_avx512_vnni,
            Encoded::score_point_avx512_vnni_internal,
        );
    }

    #[rstest]
    fn test_avx_vnni_matches_simple(
        #[values(
            DistanceType::Dot,
            DistanceType::Cosine,
            DistanceType::L2,
            DistanceType::L1
        )]
        distance_type: DistanceType,
    ) {
        if !is_x86_feature_detected!("avxvnni") {
            println!("avxvnni test skipped");
            return;
        }
        check(
            distance_type,
            Encoded::score_point_avx_vnni,
            Encoded::score_point_avx_vnni_internal,
        );
    }
}
