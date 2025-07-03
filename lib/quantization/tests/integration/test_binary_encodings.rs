#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_binary::{
        BitsStoreType, EncodedVectorsBin, Encoding, QueryEncoding,
    };
    use rand::{Rng, SeedableRng};
    use strum::IntoEnumIterator;

    use crate::metrics::dot_similarity;

    fn generate_number(rng: &mut rand::rngs::StdRng) -> f32 {
        rng.random_range(-1.0..1.0)
    }

    fn generate_vector(dim: usize, rng: &mut rand::rngs::StdRng) -> Vec<f32> {
        (0..dim)
            .map(|_| generate_number(rng) / (dim as f32).sqrt())
            .collect()
    }

    fn get_top(scores: &[f32], count: usize, invert: bool) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..scores.len()).collect();
        indices.sort_by(|&a, &b| scores[b].partial_cmp(&scores[a]).unwrap());
        if invert {
            indices.reverse();
        }
        indices.into_iter().take(count).collect()
    }

    fn match_count(ids1: &[usize], ids2: &[usize]) -> usize {
        ids1.iter().filter(|&&id| ids2.contains(&id)).count()
    }

    #[test]
    fn test_binary_dot() {
        test_binary_dot_impl::<u128>(0, false);
        test_binary_dot_impl::<u128>(600, false);
        test_binary_dot_impl::<u128>(601, false);
        test_binary_dot_impl::<u8>(600, false);
    }

    #[test]
    fn test_binary_dot_inverted() {
        test_binary_dot_impl::<u128>(700, true);
        test_binary_dot_impl::<u8>(700, true);
    }

    fn test_binary_dot_impl<TBitsStoreType: BitsStoreType>(vector_dim: usize, invert: bool) {
        let vectors_count = 1000;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encodings = [
            Encoding::OneBit,
            Encoding::OneAndHalfBits,
            Encoding::TwoBits,
        ];

        let encoded: Vec<_> = encodings
            .iter()
            .map(|&encoding| {
                EncodedVectorsBin::<TBitsStoreType, _>::encode(
                    vector_data.iter(),
                    Vec::<u8>::new(),
                    &VectorParameters {
                        dim: vector_dim,
                        count: vectors_count,
                        distance_type: DistanceType::Dot,
                        invert,
                    },
                    encoding,
                    QueryEncoding::SameAsStorage,
                    &AtomicBool::new(false),
                )
                .unwrap()
            })
            .collect();

        let top = 10;
        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);

        let orig_scores: Vec<f32> = vector_data
            .iter()
            .map(|vector| dot_similarity(&query, vector))
            .collect();
        let original_top = get_top(&orig_scores, top, invert);

        let tops = encoded
            .iter()
            .map(|encoded| {
                let query_encoded = encoded.encode_query(&query);
                let scores: Vec<f32> = (0..vector_data.len())
                    .map(|index| {
                        encoded.score_point(
                            &query_encoded,
                            index as u32,
                            &HardwareCounterCell::new(),
                        )
                    })
                    .collect();
                let tops = get_top(&scores, top, false);
                match_count(&original_top, &tops)
            })
            .collect::<Vec<_>>();

        // Check if encoding has more accuracy than previous one
        for i in 1..tops.len() {
            assert!(
                tops[i] >= tops[i - 1],
                "Encoding {} has less accuracy than encoding {}",
                i,
                i - 1
            );
        }
    }

    #[test]
    fn test_binary_dot_asymetric() {
        test_binary_dot_asymentric_impl::<u128>(0, Encoding::OneBit, false);
        test_binary_dot_asymentric_impl::<u8>(0, Encoding::OneBit, false);
        test_binary_dot_asymentric_impl::<u128>(1024, Encoding::OneBit, false);
        test_binary_dot_asymentric_impl::<u128>(601, Encoding::OneBit, false);
        test_binary_dot_asymentric_impl::<u8>(600, Encoding::OneBit, false);

        test_binary_dot_asymentric_impl::<u128>(1024, Encoding::OneAndHalfBits, false);
        test_binary_dot_asymentric_impl::<u128>(601, Encoding::OneAndHalfBits, false);
        test_binary_dot_asymentric_impl::<u8>(600, Encoding::OneAndHalfBits, false);

        test_binary_dot_asymentric_impl::<u128>(1024, Encoding::TwoBits, false);
        test_binary_dot_asymentric_impl::<u128>(701, Encoding::TwoBits, false);
        test_binary_dot_asymentric_impl::<u8>(700, Encoding::TwoBits, false);
    }

    #[test]
    fn test_binary_dot_inverted_asymetric() {
        test_binary_dot_asymentric_impl::<u128>(0, Encoding::OneBit, true);
        test_binary_dot_asymentric_impl::<u8>(0, Encoding::OneBit, true);
        test_binary_dot_asymentric_impl::<u128>(1024, Encoding::OneBit, true);
        test_binary_dot_asymentric_impl::<u128>(601, Encoding::OneBit, true);
        test_binary_dot_asymentric_impl::<u8>(600, Encoding::OneBit, true);
    }

    fn test_binary_dot_asymentric_impl<TBitsStoreType: BitsStoreType>(
        vector_dim: usize,
        encoding: Encoding,
        invert: bool,
    ) {
        let vectors_count = 1000;

        let mut rng = rand::rngs::StdRng::seed_from_u64(43);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng));
        }

        let encoded: Vec<_> = QueryEncoding::iter()
            .map(|query_encoding| {
                EncodedVectorsBin::<TBitsStoreType, _>::encode(
                    vector_data.iter(),
                    Vec::<u8>::new(),
                    &VectorParameters {
                        dim: vector_dim,
                        count: vectors_count,
                        distance_type: DistanceType::Dot,
                        invert,
                    },
                    encoding,
                    query_encoding,
                    &AtomicBool::new(false),
                )
                .unwrap()
            })
            .collect();

        let top = 10;
        let query: Vec<f32> = generate_vector(vector_dim, &mut rng);

        let orig_scores: Vec<f32> = vector_data
            .iter()
            .map(|vector| dot_similarity(&query, vector))
            .collect();
        let original_top = get_top(&orig_scores, top, invert);

        let tops = encoded
            .iter()
            .map(|encoded| {
                let query_encoded = encoded.encode_query(&query);
                let scores: Vec<f32> = (0..vector_data.len())
                    .map(|index| {
                        encoded.score_point(
                            &query_encoded,
                            index as u32,
                            &HardwareCounterCell::new(),
                        )
                    })
                    .collect();
                let tops = get_top(&scores, top, false);
                match_count(&original_top, &tops)
            })
            .collect::<Vec<_>>();

        // Check if encoding has more accuracy than previous one
        for i in 1..tops.len() {
            assert!(
                tops[i] >= tops[i - 1],
                "Encoding {} has less accuracy than encoding {}",
                i,
                i - 1
            );
        }
    }
}
