#[cfg(test)]
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod tests {
    use std::sync::atomic::AtomicBool;

    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_u8::EncodedVectorsU8;
    use rand::{Rng, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    #[test]
    fn test_dot_sse() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_sse(&query_u8, index as u32);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_l2_sse() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L2,
                invert: false,
            },
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_sse(&query_u8, index as u32);
            let orginal_score = l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }

    #[test]
    fn test_l1_sse() {
        let vectors_count = 129;
        let vector_dim = 65;
        let error = vector_dim as f32 * 0.1;

        //let mut rng = rand::thread_rng();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<f32>> = Vec::new();
        for _ in 0..vectors_count {
            let vector: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();
            vector_data.push(vector);
        }
        let query: Vec<f32> = (0..vector_dim).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsU8::encode(
            vector_data.iter(),
            Vec::<u8>::new(),
            &VectorParameters {
                dim: vector_dim,
                count: vectors_count,
                distance_type: DistanceType::L1,
                invert: false,
            },
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point_sse(&query_u8, index as u32);
            let orginal_score = l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < error);
        }
    }
}
