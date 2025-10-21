#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::types::PointOffsetType;
    use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_flex::EncodedVectorsFlex;
    use rand::{Rng, SeedableRng};
    use rand_distr::StandardNormal;

    use crate::metrics::dot_similarity;

    fn generate_number(rng: &mut rand::rngs::StdRng, (mean, stddev): (f32, f32)) -> f32 {
        rng.sample::<f32, _>(StandardNormal) * stddev + mean
    }

    fn generate_vector(dim: usize, rng: &mut rand::rngs::StdRng, distr: &[(f32, f32)]) -> Vec<f32> {
        (0..dim).map(|i| generate_number(rng, distr[i])).collect()
    }

    // "mean": -0.0076678228,
    // "stddev": 0.028891206,
    fn generate_distribution(dim: usize, rng: &mut rand::rngs::StdRng) -> Vec<(f32, f32)> {
        (0..dim)
            .map(|_| {
                (
                    generate_number(rng, (0.0, 0.02)),
                    generate_number(rng, (0.05, 0.04)).max(0.02),
                )
            })
            .collect()
    }

    #[test]
    fn test_binary_dot() {
        test_binary_dot_impl(1024, 4);
    }

    fn test_binary_dot_impl(vector_dim: usize, bits_count: usize) {
        let vectors_count = 10_000;
        let error = vector_dim as f32 * 1.5;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let distr = generate_distribution(vector_dim, &mut rng);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng, &distr));
        }

        if std::fs::exists("/root/qdrant/target/test_data/").unwrap_or(false) {
            // std::fs::remove_dir_all("/root/qdrant/target/test_data/").ok();
        }

        let vector_parameters = &VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };

        let quantized_vector_size =
            EncodedVectorsFlex::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                bits_count,
            );
        let encoded = EncodedVectorsFlex::<TestEncodedStorage>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            Some(1),
            Some(2.0),
            bits_count,
            None,
            None, //Some(&Path::new("/root/qdrant/target/test_data/")),
            &AtomicBool::new(false),
        )
        .unwrap();

        let query: Vec<f32> = generate_vector(vector_dim, &mut rng, &distr);
        let query_encoded = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        let mut orig_scores: Vec<(f32, usize)> = vector_data
            .iter()
            .enumerate()
            .map(|(i, v)| (dot_similarity(&query, v), i))
            .collect();
        let mut encoded_scores: Vec<(f32, usize)> = (0..vectors_count as usize)
            .map(|i| {
                (
                    encoded.score_point(&query_encoded, i as PointOffsetType, &counter),
                    i,
                )
            })
            .collect();
        orig_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        encoded_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());

        let orig_top10 = orig_scores.iter().take(10);
        let enc_top10 = encoded_scores.iter().take(10);
        let mut matches = 0usize;
        for orig_result in orig_top10 {
            for enc_result in enc_top10.clone() {
                if orig_result.1 == enc_result.1 {
                    matches += 1;
                }
            }
        }
        println!("Top 10 matches: {}/10", matches);

        //for (original_score, score) in orig_scores.iter().zip(encoded_scores.iter()).take(100) {
        //println!("TOP N RESULT: Score: {:?}, original: {:?}, diff: {}, error: {}", score, original_score, (score.0 - original_score.0).abs(), error);
        //assert!((score - original_score).abs() <= error);
        //}

        let counter = HardwareCounterCell::new();
        let mut err_sum: f64 = 0.0;
        for (index, vector) in vector_data.iter().enumerate() {
            let orginal_score = dot_similarity(&query, vector);
            let score = encoded.score_point(&query_encoded, index as u32, &counter);

            if index < 20 {
                println!(
                    "Score: {}, original: {}, diff: {}, error: {}",
                    score,
                    orginal_score,
                    (score - orginal_score).abs(),
                    error
                );
            }

            let absolute_diff = (score - orginal_score).abs() as f64;
            err_sum += absolute_diff / orginal_score.abs() as f64;
            //assert!((score - orginal_score).abs() <= error);
        }
        println!("Avg error: {}", err_sum / vector_data.len() as f64);
    }

    #[test]
    fn test_binary_dot_internal() {
        test_binary_dot_internal_impl(256, 8);
    }

    fn test_binary_dot_internal_impl(vector_dim: usize, bits_count: usize) {
        let vectors_count = 1024;
        let error = vector_dim as f32 * 0.01;

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let distr = generate_distribution(vector_dim, &mut rng);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            vector_data.push(generate_vector(vector_dim, &mut rng, &distr));
        }

        let vector_parameters = &VectorParameters {
            dim: vector_dim,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        };

        let quantized_vector_size =
            EncodedVectorsFlex::<TestEncodedStorage>::get_quantized_vector_size(
                &vector_parameters,
                bits_count,
            );
        let encoded = EncodedVectorsFlex::<TestEncodedStorage>::encode(
            vector_data.iter(),
            TestEncodedStorageBuilder::new(None, quantized_vector_size),
            &vector_parameters,
            vectors_count,
            None,
            None,
            None,
            bits_count,
            None,
            None, //Some(&Path::new("/root/qdrant/target/test_data/")),
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..vectors_count {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            println!(
                "Score: {}, original: {}, diff: {}, error: {}",
                score,
                orginal_score,
                (score - orginal_score).abs(),
                error
            );
            assert!((score - orginal_score).abs() <= error);
        }
    }
}
