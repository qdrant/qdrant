#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_rq::EncodedVectorsRQ;
    use rand::{Rng, SeedableRng};

    use crate::metrics::dot_similarity;

    const VECTORS_COUNT: usize = 513;
    const VECTOR_DIM: usize = 65;
    const ERROR: f32 = VECTOR_DIM as f32 * 0.05;

    /*
    fn generate_number(rng: &mut rand::rngs::StdRng) -> f32 {
        let n = f32::signum(rng.gen_range(-1.0..1.0));
        if n == 0.0 {
            1.0
        } else {
            n
        }
    }

    fn generate_vector(dim: usize, rng: &mut rand::rngs::StdRng) -> Vec<f32> {
        (0..dim).map(|_| generate_number(rng)).collect()
    }
     */

    #[test]
    fn test_rq_cosine() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            let mut vector: Vec<_> = (0..VECTOR_DIM)
                .map(|_| 2.0 * rng.gen::<f32>() - 1.0)
                .collect();
            //let mut vector: Vec<_> = generate_vector(VECTOR_DIM, &mut rng);
            let norm = dot_similarity(&vector, &vector).sqrt();
            vector.iter_mut().for_each(|x| *x /= norm);
            vector_data.push(vector);
        }
        let query = vector_data[0].clone();

        let encoded = EncodedVectorsRQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        let mut max_diff = 0.0;
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = dot_similarity(&query, vector);
            let diff = (score - orginal_score).abs();
            max_diff = if diff > max_diff { diff } else { max_diff };
            println!("{}: {} vs {}", diff, score, orginal_score);
            assert!(diff < ERROR);
        }
        println!("Max diff: {}", max_diff);
    }

    #[test]
    fn test_rq_cosine_internal() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            let mut vector: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen::<f32>() - 0.5).collect();
            let norm = dot_similarity(&vector, &vector).sqrt();
            vector.iter_mut().for_each(|x| *x /= norm);
            vector_data.push(vector);
        }

        let encoded = EncodedVectorsRQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 0..VECTORS_COUNT {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            let diff = (score - orginal_score).abs();
            println!("{}: {} vs {}", diff, score, orginal_score);
            assert!(diff < ERROR);
        }
    }
}
