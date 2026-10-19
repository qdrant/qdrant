//! The batched MaxSim paths of [`QuantizedMultivectorStorage`] against its
//! per-pair paths, for every quantization.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::types::{PointOffsetType, ScoreType};
use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
use quantization::encoded_vectors_binary::{EncodedVectorsBin, Encoding, QueryEncoding};
use quantization::encoded_vectors_pq::EncodedVectorsPQ;
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::{EncodedVectorsU8, ScalarQuantizationMethod};
use quantization::turboquant::{TQBits, TQMode, TQRotation};
use quantization::{
    MultiQuery, encoded_vectors_binary, encoded_vectors_pq, encoded_vectors_tq, encoded_vectors_u8,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use super::{
    MultivectorOffset, MultivectorOffsetsStorage, MultivectorOffsetsStorageRam,
    QuantizedMultivectorStorage,
};
use crate::types::MultiVectorConfig;

const DIM: usize = 32;
const VECTORS: usize = 600;

fn random_vectors(rng: &mut StdRng, count: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|_| (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect())
        .collect()
}

fn parameters(distance_type: DistanceType) -> VectorParameters {
    VectorParameters {
        dim: DIM,
        deprecated_count: None,
        distance_type,
        invert: false,
    }
}

/// Points of 1 to 20 stored vectors each, around the 16-lane block boundary.
fn offsets(rng: &mut StdRng) -> Vec<MultivectorOffset> {
    let mut offsets = Vec::new();
    let mut start = 0;
    loop {
        let count = rng.random_range(1..=20);
        if start + count > VECTORS as PointOffsetType {
            return offsets;
        }
        offsets.push(MultivectorOffset { start, count });
        start += count;
    }
}

fn check<E: EncodedVectors>(encoded: E, internal: bool, label: &str) {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(7);
    let dir = tempfile::Builder::new().prefix("packed").tempdir().unwrap();
    let offsets = offsets(&mut rng);
    let points = offsets.len() as PointOffsetType;
    let offsets_storage =
        MultivectorOffsetsStorageRam::create(&dir.path().join("offsets"), offsets.into_iter())
            .unwrap();
    let storage = QuantizedMultivectorStorage::new(
        DIM,
        encoded,
        offsets_storage,
        MultiVectorConfig::default(),
    );
    let ids: Vec<PointOffsetType> = (0..points).collect();
    let mut want = vec![0.0; ids.len()];
    let mut got = vec![0.0; ids.len()];

    // The same queries scored pair by pair and packed.
    let encode = |vectors: &[Vec<f32>]| -> Vec<_> {
        vectors
            .iter()
            .map(|vector| storage.inner_storage().encode_query(vector))
            .collect()
    };
    let queries: Vec<_> = [1, 17, 40]
        .into_iter()
        .map(|tokens| {
            let vectors = random_vectors(&mut rng, tokens);
            let packed = storage
                .inner_storage()
                .pack_multi_query(&encode(&vectors))
                .unwrap_or_else(|| panic!("{label}: no packed query"));
            (
                MultiQuery::PerPair(encode(&vectors)),
                MultiQuery::Packed(packed),
            )
        })
        .collect();

    for (per_pair, packed) in &queries {
        storage
            .score_points_batch(&ids, |score| score(per_pair), &mut want)
            .unwrap();
        storage
            .score_points_batch(&ids, |score| score(packed), &mut got)
            .unwrap();
        assert_eq!(
            got,
            want,
            "{label}: query of {} vectors",
            per_pair.vectors_count()
        );
    }

    // A query of several multi-vectors, as custom queries score them.
    let combine = |scores: &[ScoreType]| scores[0] - 0.5 * scores[1] + scores[2];
    storage
        .score_points_batch(
            &ids,
            |score| {
                combine(
                    &queries
                        .iter()
                        .map(|(query, _)| score(query))
                        .collect::<Vec<_>>(),
                )
            },
            &mut want,
        )
        .unwrap();
    storage
        .score_points_batch(
            &ids,
            |score| {
                combine(
                    &queries
                        .iter()
                        .map(|(_, query)| score(query))
                        .collect::<Vec<_>>(),
                )
            },
            &mut got,
        )
        .unwrap();
    assert_eq!(got, want, "{label}: several multi-vectors");

    if internal {
        for a in 0..points {
            let b = (a * 7 + 3) % points;
            let (offset_a, offset_b) =
                (storage.offsets.get_offset(a), storage.offsets.get_offset(b));
            let mut want = 0.0;
            for i in offset_a.start..offset_a.start + offset_a.count {
                let mut maximum = ScoreType::NEG_INFINITY;
                for j in offset_b.start..offset_b.start + offset_b.count {
                    let score = storage.inner_storage().score_internal(i, j);
                    if score > maximum {
                        maximum = score;
                    }
                }
                want += maximum;
            }
            let got = storage.score_internal(a, b);
            assert_eq!(got, want, "{label}: internal {a} vs {b}");
        }
    }
}

#[test]
fn packed_scalar_matches_pairwise() {
    let mut rng = StdRng::seed_from_u64(42);
    let vectors = random_vectors(&mut rng, VECTORS);
    for distance in [DistanceType::Dot, DistanceType::L1] {
        let params = parameters(distance);
        let encoded = EncodedVectorsU8::encode(
            vectors.iter(),
            std::iter::repeat_n(true, VECTORS),
            TestEncodedStorageBuilder::new(
                None,
                encoded_vectors_u8::get_quantized_vector_size(&params),
            ),
            &params,
            None,
            ScalarQuantizationMethod::Int8,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        check(encoded, true, &format!("scalar {distance:?}"));
    }
}

#[test]
fn packed_turbo_matches_pairwise() {
    let mut rng = StdRng::seed_from_u64(42);
    let vectors = random_vectors(&mut rng, VECTORS);
    for (bits, mode) in [
        (TQBits::Bits4, TQMode::Plus),
        (TQBits::Bits2, TQMode::Plus),
        (TQBits::Bits1, TQMode::Plus),
        (TQBits::Bits8, TQMode::Normal),
    ] {
        let params = parameters(DistanceType::Cosine);
        let encoded: EncodedVectorsTQ<TestEncodedStorage> = EncodedVectorsTQ::encode(
            vectors.iter(),
            std::iter::repeat_n(true, VECTORS),
            TestEncodedStorageBuilder::new(
                None,
                encoded_vectors_tq::get_quantized_vector_size(&params, bits, mode),
            ),
            &params,
            bits,
            mode,
            TQRotation::Padded,
            false,
            1,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        check(encoded, true, &format!("turbo {bits:?} {mode:?}"));
    }
}

#[test]
fn packed_binary_matches_pairwise() {
    let mut rng = StdRng::seed_from_u64(42);
    let vectors = random_vectors(&mut rng, VECTORS);
    for (encoding, query_encoding) in [
        (Encoding::OneBit, QueryEncoding::SameAsStorage),
        (Encoding::TwoBits, QueryEncoding::Scalar8bits),
    ] {
        let encoded = EncodedVectorsBin::<u128, _>::encode(
            vectors.iter(),
            std::iter::repeat_n(true, VECTORS),
            TestEncodedStorageBuilder::new(
                None,
                encoded_vectors_binary::get_quantized_vector_size_from_params::<u128>(
                    DIM, encoding,
                ),
            ),
            &parameters(DistanceType::Dot),
            encoding,
            query_encoding,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        check(
            encoded,
            true,
            &format!("binary {encoding:?} {query_encoding:?}"),
        );
    }
}

#[test]
fn packed_product_matches_pairwise() {
    let mut rng = StdRng::seed_from_u64(42);
    let vectors = random_vectors(&mut rng, VECTORS);
    let params = parameters(DistanceType::Dot);
    let encoded = EncodedVectorsPQ::encode(
        vectors.iter(),
        std::iter::repeat_n(true, VECTORS),
        TestEncodedStorageBuilder::new(
            None,
            encoded_vectors_pq::get_quantized_vector_size(&params, 2),
        ),
        &params,
        2,
        1,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();
    // No batched symmetric kernel: `score_internal` stays per pair.
    check(encoded, true, "product");
}
