//! The constants of scalar quantization's `ScalarCombine`, as code that scores elsewhere reads
//! them.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::types::PointOffsetType;
use pairwise::{Combine as _, MatrixOperands as _};
use quantization::encoded_storage::TestEncodedStorageBuilder;
use quantization::encoded_vectors::{DistanceType, VectorParameters};
use quantization::encoded_vectors_u8::{
    self, EncodedVectorsU8, ScalarOperands, ScalarQuantizationMethod,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rstest::rstest;

const POINTS: usize = 60;
const DIM: usize = 100;

/// `((multiplier · dot) + (offset_a − shift)) + offset_b`, from the getters and the stored
/// records, is `ScalarCombine::score` bitwise.
#[rstest]
fn getters_rebuild_the_score(
    #[values(DistanceType::Dot, DistanceType::Cosine, DistanceType::L2)]
    distance_type: DistanceType,
    #[values(false, true)] invert: bool,
) {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let vectors: Vec<Vec<f32>> = (0..POINTS)
        .map(|_| (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect())
        .collect();
    let vector_parameters = VectorParameters {
        dim: DIM,
        distance_type,
        invert,
        deprecated_count: None,
    };
    let size = encoded_vectors_u8::get_quantized_vector_size(&vector_parameters);
    let encoded = EncodedVectorsU8::encode(
        vectors.iter(),
        std::iter::repeat_n(true, POINTS),
        TestEncodedStorageBuilder::new(None, size),
        &vector_parameters,
        None,
        ScalarQuantizationMethod::Int8,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();
    let operands = ScalarOperands::new(&encoded).unwrap();
    let combine = operands.combine();
    let (multiplier, shift) = (combine.multiplier(), combine.shift());
    match distance_type {
        DistanceType::Dot | DistanceType::Cosine => assert_ne!(shift, 0.0),
        DistanceType::L2 => assert_eq!(shift, 0.0),
        DistanceType::L1 => unreachable!(),
    }

    let (offset_at, codes_at, depth) =
        (operands.offset_at(), operands.codes_at(), operands.depth());
    let records: Vec<Vec<u8>> = (0..POINTS as PointOffsetType)
        .map(|id| encoded.get_quantized_vector(id).to_vec())
        .collect();
    let offset =
        |record: &[u8]| f32::from_ne_bytes(record[offset_at..offset_at + 4].try_into().unwrap());
    for a in &records {
        for b in &records {
            let dot: i32 = a[codes_at..codes_at + depth]
                .iter()
                .zip(&b[codes_at..codes_at + depth])
                .map(|(&x, &y)| i32::from(x) * i32::from(y))
                .sum();
            let dot = dot as f32;
            let (offset_a, offset_b) = (offset(a), offset(b));
            let rebuilt = ((multiplier * dot) + (offset_a - shift)) + offset_b;
            let score = combine.score(dot, &[offset_a, 0.0, 0.0], &[offset_b, 0.0, 0.0]);
            assert_eq!(rebuilt.to_bits(), score.to_bits(), "{rebuilt} vs {score}");
        }
    }
}
