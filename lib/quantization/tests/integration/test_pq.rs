#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Duration;

    use common::counter::hardware_counter::HardwareCounterCell;
    use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
    use quantization::encoded_vectors_pq::EncodedVectorsPQ;
    use rand::{Rng, SeedableRng};

    use crate::metrics::{dot_similarity, l1_similarity, l2_similarity};

    const VECTORS_COUNT: usize = 513;
    const VECTOR_DIM: usize = 65;
    const ERROR: f32 = VECTOR_DIM as f32 * 0.05;

    #[test]
    fn test_pq_dot() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_l2() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::L2,
                invert: false,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_l1() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::L1,
                invert: false,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_dot_inverted() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -dot_similarity(&query, vector);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_l2_inverted() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::L2,
                invert: true,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -l2_similarity(&query, vector);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_l1_inverted() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }
        let query: Vec<_> = (0..VECTOR_DIM).map(|_| rng.gen()).collect();

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::L1,
                invert: true,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        let query_u8 = encoded.encode_query(&query);

        let counter = HardwareCounterCell::new();
        for (index, vector) in vector_data.iter().enumerate() {
            let score = encoded.score_point(&query_u8, index as u32, &counter);
            let orginal_score = -l1_similarity(&query, vector);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_dot_internal() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::Dot,
                invert: false,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..VECTORS_COUNT {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    #[test]
    fn test_pq_dot_inverted_internal() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }

        let encoded = EncodedVectorsPQ::encode(
            vector_data.iter(),
            vec![],
            &VectorParameters {
                dim: VECTOR_DIM,
                count: VECTORS_COUNT,
                distance_type: DistanceType::Dot,
                invert: true,
            },
            1,
            1,
            &AtomicBool::new(false),
        )
        .unwrap();

        let counter = HardwareCounterCell::new();
        for i in 1..VECTORS_COUNT {
            let score = encoded.score_internal(0, i as u32, &counter);
            let orginal_score = -dot_similarity(&vector_data[0], &vector_data[i]);
            assert!((score - orginal_score).abs() < ERROR);
        }
        counter.discard_results();
    }

    // ignore this test because it requires long time
    // this test should be started separately of with `--test-threads=1` flag
    // because `num_threads::num_threads()` is used to check that all encode threads finished
    #[ignore]
    #[test]
    fn test_encode_panic() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let mut vector_data: Vec<Vec<_>> = vec![];
        for _ in 0..VECTORS_COUNT {
            vector_data.push((0..VECTOR_DIM).map(|_| rng.gen()).collect());
        }

        for i in 0.. {
            let counter = AtomicUsize::new(0);
            let panic_index = i * VECTORS_COUNT / 3;

            let start_num_threads = num_threads::num_threads();
            let vector_data = vector_data.clone();
            let result = std::thread::spawn(move || {
                EncodedVectorsPQ::encode(
                    vector_data.iter().inspect(|_| {
                        let cnt = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if cnt == panic_index {
                            panic!("test panic")
                        }
                        if cnt > panic_index {
                            // after panic add start sleeping to simulate large amount of data
                            std::thread::sleep(Duration::from_micros(100));
                        }
                    }),
                    vec![],
                    &VectorParameters {
                        dim: VECTOR_DIM,
                        count: VECTORS_COUNT,
                        distance_type: DistanceType::Dot,
                        invert: false,
                    },
                    1,
                    5,
                    &AtomicBool::new(false),
                )
                .unwrap()
            })
            .join();

            if result.is_ok() {
                // no panic, panic_index is too big, all panic cases are handled
                return;
            }

            // some time required to finish encoding threads
            std::thread::sleep(Duration::from_millis(50));

            // check that all threads are finished
            assert!(num_threads::num_threads() == start_num_threads);

            println!("Finished iteration {i}");
        }
    }
}
