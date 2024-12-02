#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use quantization::encoded_vectors::{DistanceType, VectorParameters};
    use quantization::encoded_vectors_u8::EncodedVectorsU8;
    use quantization::{EncodedVectorsPQ, EncodingError};

    #[test]
    fn stop_condition_u8() {
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();
        let stopped_ref = stopped.as_ref();

        let stop_thread = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            stopped_clone.store(true, Ordering::Relaxed);
        });

        let vectors_count = 1_000_000;
        let vector_dim = 8;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let zero_vector = vec![0.0; vector_dim];

        assert!(
            EncodedVectorsU8::encode(
                (0..vector_parameters.count).map(|_| &zero_vector),
                Vec::<u8>::new(),
                &vector_parameters,
                None,
                stopped_ref,
            )
            .err()
                == Some(EncodingError::Stopped)
        );

        stop_thread.join().unwrap();
    }

    #[test]
    fn stop_condition_pq() {
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();
        let stopped_ref = stopped.as_ref();

        let stop_thread = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            stopped_clone.store(true, Ordering::Relaxed);
        });

        let vectors_count = 1_000_000;
        let vector_dim = 8;
        let vector_parameters = VectorParameters {
            dim: vector_dim,
            count: vectors_count,
            distance_type: DistanceType::Dot,
            invert: false,
        };
        let zero_vector = vec![0.0; vector_dim];

        assert!(
            EncodedVectorsPQ::encode(
                (0..vector_parameters.count).map(|_| &zero_vector),
                Vec::<u8>::new(),
                &vector_parameters,
                2,
                1,
                stopped_ref,
            )
            .err()
                == Some(EncodingError::Stopped)
        );

        stop_thread.join().unwrap();
    }
}
