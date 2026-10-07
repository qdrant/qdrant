//! The standalone encoders must produce byte-for-byte what the storage-backed encode path
//! stores, so a writer that persists the rows itself stays readable by the regular storage.

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use common::types::PointOffsetType;
    use common::universal_io::MmapFs;
    use quantization::encoded_storage::{EncodedStorage, TestEncodedStorageBuilder};
    use quantization::encoded_vectors::{DistanceType, VectorParameters};
    use quantization::encoded_vectors_binary::{
        self, EncodedVectorsBin, EncoderBin, Encoding, QueryEncoding,
    };
    use quantization::encoded_vectors_tq::{self, EncodedVectorsTQ, EncoderTQ};
    use quantization::turboquant::{TQBits, TQMode, TQRotation};
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    const DIM: usize = 129;
    const COUNT: usize = 32;

    fn vector_parameters() -> VectorParameters {
        VectorParameters {
            dim: DIM,
            deprecated_count: None,
            distance_type: DistanceType::Dot,
            invert: false,
        }
    }

    fn random_vectors() -> Vec<Vec<f32>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        (0..COUNT)
            .map(|_| (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect())
            .collect()
    }

    #[test]
    fn encoder_bin_matches_stored_rows() {
        let vectors = random_vectors();
        for encoding in [
            Encoding::OneBit,
            Encoding::TwoBits,
            Encoding::OneAndHalfBits,
        ] {
            let dir = Builder::new().prefix("encoder_bin").tempdir().unwrap();
            let meta_path = dir.path().join("meta.json");
            let quantized_vector_size =
                encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                    DIM, encoding,
                );
            let encoded = EncodedVectorsBin::<u128, _>::encode(
                vectors.iter(),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters(),
                encoding,
                QueryEncoding::SameAsStorage,
                Some(meta_path.as_path()),
                &AtomicBool::new(false),
            )
            .unwrap();

            let encoder = EncoderBin::<u128>::load(&MmapFs, &meta_path).unwrap();
            for (id, vector) in vectors.iter().enumerate() {
                assert_eq!(
                    encoder.encode(vector).as_bytes(),
                    encoded
                        .storage()
                        .get_vector_data(id as PointOffsetType)
                        .as_ref(),
                    "{encoding:?}, vector {id}",
                );
            }
        }
    }

    #[test]
    fn encoder_tq_matches_stored_rows() {
        let vectors = random_vectors();
        for (bits, mode) in [
            (TQBits::Bits4, TQMode::Normal),
            (TQBits::Bits2, TQMode::Plus),
            (TQBits::Bits8, TQMode::Normal),
        ] {
            let dir = Builder::new().prefix("encoder_tq").tempdir().unwrap();
            let meta_path = dir.path().join("meta.json");
            let quantized_vector_size =
                encoded_vectors_tq::get_quantized_vector_size(&vector_parameters(), bits, mode);
            let encoded = EncodedVectorsTQ::encode(
                vectors.iter(),
                TestEncodedStorageBuilder::new(None, quantized_vector_size),
                &vector_parameters(),
                COUNT,
                bits,
                mode,
                TQRotation::Padded,
                false,
                1,
                Some(meta_path.as_path()),
                &AtomicBool::new(false),
            )
            .unwrap();

            let mut encoder = EncoderTQ::load(&MmapFs, &meta_path).unwrap();
            for (id, vector) in vectors.iter().enumerate() {
                assert_eq!(
                    encoder.encode(vector),
                    encoded
                        .storage()
                        .get_vector_data(id as PointOffsetType)
                        .as_ref(),
                    "{bits:?} {mode:?}, vector {id}",
                );
            }
        }
    }
}
