//! Quantized codes as the vectors to sketch: each decoded by `symmetric_vector`, so that the
//! hash splits candidates by the geometry the scores see.
//!
//! Fingerprints hash the stored code, or the decoded vector where the code also stores a
//! correction the score ignores (`stores_ignored_correction`). So codes that score alike against
//! every point, up to rounding, are copies. A code that scores NaN against itself gets none.

use common::types::PointOffsetType;
use quantization::encoded_vectors_binary::{BitsStoreType, EncodedVectorsBin};
use quantization::encoded_vectors_tq::EncodedVectorsTQ;
use quantization::encoded_vectors_u8::EncodedVectorsU8;
use quantization::{EncodedStorage, EncodedVectors};

use super::{FingerprintSource, Vectors};
use crate::common::operation_error::OperationResult;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};

/// The codes of `quantized` as [`Vectors`], `None` for those PiPNN doesn't sketch.
pub(super) fn codes(quantized: &QuantizedVectors) -> Option<Box<dyn Vectors + '_>> {
    let codes = match quantized.get_storage() {
        QuantizedVectorStorage::ScalarRam(q) => codes_of(q),
        QuantizedVectorStorage::ScalarMmap(q) => codes_of(q),
        QuantizedVectorStorage::ScalarChunkedMmap(q) => codes_of(q),
        QuantizedVectorStorage::BinaryRam(q) => codes_of(q),
        QuantizedVectorStorage::BinaryMmap(q) => codes_of(q),
        QuantizedVectorStorage::BinaryChunkedMmap(q) => codes_of(q),
        QuantizedVectorStorage::TQRam(q) => codes_of(q),
        QuantizedVectorStorage::TQMmap(q) => codes_of(q),
        QuantizedVectorStorage::TQChunkedMmap(q) => codes_of(q),
        // No PiPNN with product quantization (no matrix scorer), and no sketches of multivectors
        QuantizedVectorStorage::PQRam(_)
        | QuantizedVectorStorage::PQMmap(_)
        | QuantizedVectorStorage::PQChunkedMmap(_)
        | QuantizedVectorStorage::ScalarRamMulti(_)
        | QuantizedVectorStorage::ScalarMmapMulti(_)
        | QuantizedVectorStorage::ScalarChunkedMmapMulti(_)
        | QuantizedVectorStorage::PQRamMulti(_)
        | QuantizedVectorStorage::PQMmapMulti(_)
        | QuantizedVectorStorage::PQChunkedMmapMulti(_)
        | QuantizedVectorStorage::BinaryRamMulti(_)
        | QuantizedVectorStorage::BinaryMmapMulti(_)
        | QuantizedVectorStorage::BinaryChunkedMmapMulti(_)
        | QuantizedVectorStorage::TQRamMulti(_)
        | QuantizedVectorStorage::TQMmapMulti(_)
        | QuantizedVectorStorage::TQChunkedMmapMulti(_) => return None,
    };
    Some(codes)
}

/// A quantization whose codes decode to vectors to sketch.
trait Decode: EncodedVectors + Sync {
    /// Length of the decoded vectors.
    fn decoded_dim(&self) -> usize;

    /// Appends `code` decoded to `vector`, and tells what its fingerprint hashes.
    fn decode<'c>(&self, code: &'c [u8], vector: &mut Vec<f32>) -> FingerprintSource<'c>;
}

/// The codes of one quantization.
struct Codes<'a, E>(&'a E);

fn codes_of<E: Decode>(encoded: &E) -> Box<dyn Vectors + '_> {
    Box::new(Codes(encoded))
}

impl<E: Decode> Vectors for Codes<'_, E> {
    fn dim(&self) -> usize {
        self.0.decoded_dim()
    }

    fn for_each_in_batch(
        &self,
        ids: &[PointOffsetType],
        f: &mut dyn FnMut(usize, &[f32], FingerprintSource<'_>),
    ) -> OperationResult<()> {
        let mut vector = Vec::with_capacity(self.dim());
        self.0.for_each_batch(ids, |i, code| {
            vector.clear();
            let fingerprint = self.0.decode(&code, &mut vector);
            f(i, &vector, fingerprint);
        })?;
        Ok(())
    }
}

impl<S: EncodedStorage + Sync> Decode for EncodedVectorsU8<S> {
    fn decoded_dim(&self) -> usize {
        self.symmetric_dim()
    }

    fn decode<'c>(&self, code: &'c [u8], vector: &mut Vec<f32>) -> FingerprintSource<'c> {
        vector.extend(self.symmetric_vector(code));
        FingerprintSource::Code(code)
    }
}

impl<T: BitsStoreType + Sync, S: EncodedStorage + Sync> Decode for EncodedVectorsBin<T, S> {
    fn decoded_dim(&self) -> usize {
        self.symmetric_dim()
    }

    fn decode<'c>(&self, code: &'c [u8], vector: &mut Vec<f32>) -> FingerprintSource<'c> {
        vector.extend(self.symmetric_vector(code));
        FingerprintSource::Code(code)
    }
}

impl<S: EncodedStorage + Sync> Decode for EncodedVectorsTQ<S> {
    fn decoded_dim(&self) -> usize {
        self.quantizer().get_padded_dim()
    }

    fn decode<'c>(&self, code: &'c [u8], vector: &mut Vec<f32>) -> FingerprintSource<'c> {
        let quantizer = self.quantizer();
        vector.extend(quantizer.symmetric_vector(code));
        if quantizer.score_symmetric(code, code).is_nan() {
            FingerprintSource::None
        } else if quantizer.stores_ignored_correction() {
            FingerprintSource::Vector
        } else {
            FingerprintSource::Code(code)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    use common::ambient;
    use common::bitvec::BitVec;
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;
    use rstest::rstest;
    use tempfile::TempDir;

    use super::super::{Fingerprints, HASH_BITS, Sketches, hyperplanes, project, standard_normal};
    use super::*;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::index::hnsw_index::pipnn::duplicates;
    use crate::index::hnsw_index::pipnn::orchestration_tests::{binary, scalar};
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
    use crate::types::{
        BinaryQuantizationEncoding as Encoding, Distance, QuantizationConfig,
        TurboQuantBitSize as Bits, TurboQuantQuantizationConfig, TurboQuantization,
    };
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;
    use crate::vector_storage::{VectorStorage as _, VectorStorageEnum};

    const POINTS: usize = 200;
    /// Exact copies of earlier points, and as many near-copies, whose codes differ at most in what
    /// some scores ignore.
    const COPIES: usize = 20;
    /// Not a multiple of the words, chunks, or padding of any code.
    const DIM: usize = 37;
    const SEED: u64 = 5;

    #[expect(deprecated, reason = "always_ram")]
    fn turbo(bits: Bits) -> QuantizationConfig {
        QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: None,
                memory: None,
                bits: Some(bits),
            },
        })
    }

    struct Fixture {
        vectors: Vec<Vec<f32>>,
        storage: VectorStorageEnum,
        quantized: QuantizedVectors,
        points: Vec<PointOffsetType>,
        deleted: BitVec,
        _dir: TempDir,
    }

    impl Fixture {
        /// Gaussian vectors, then [`COPIES`] copies of the first ones and as many near-copies of
        /// the next ones, with a NaN in the vector of point 3 if `nan`, quantized with `config`.
        fn new(config: &QuantizationConfig, distance: Distance, nan: bool) -> Self {
            let mut rng = StdRng::seed_from_u64(1);
            let mut vectors: Vec<Vec<f32>> = (0..POINTS - 2 * COPIES)
                .map(|_| (0..DIM).map(|_| standard_normal(&mut rng) as f32).collect())
                .collect();
            vectors.extend_from_within(..COPIES);
            for i in COPIES..2 * COPIES {
                let near = vectors[i]
                    .iter()
                    .map(|x| x + 1e-3 * standard_normal(&mut rng) as f32);
                vectors.push(near.collect());
            }
            let mut vectors: Vec<_> = (vectors.into_iter())
                .map(|vector| distance.preprocess_vector::<VectorElementType>(vector))
                .collect();
            if nan {
                vectors[3][5] = f32::NAN;
            }
            let mut storage = new_volatile_dense_vector_storage(DIM, distance);
            for (id, vector) in vectors.iter().enumerate() {
                let id = id as PointOffsetType;
                storage.insert_vector(id, VectorRef::from(vector)).unwrap();
            }
            let dir = tempfile::tempdir().unwrap();
            let quantized = QuantizedVectors::create(
                &storage,
                config,
                QuantizedVectorsStorageType::Immutable,
                dir.path(),
                1,
                &AtomicBool::new(false),
            )
            .unwrap();
            Self {
                vectors,
                storage,
                quantized,
                points: (0..POINTS as PointOffsetType).collect(),
                deleted: BitVec::repeat(false, POINTS),
                _dir: dir,
            }
        }

        /// The scorer of the final prune.
        fn scorer(&self) -> FilteredScorer<'_> {
            let quantized = Some(&self.quantized);
            FilteredScorer::new_internal(0, &self.storage, quantized, None, &self.deleted).unwrap()
        }

        fn sketches(&self) -> (Sketches, Fingerprints) {
            let stopped = AtomicBool::new(false);
            Sketches::new(
                &self.storage,
                Some(&self.quantized),
                &self.points,
                SEED,
                &stopped,
            )
            .unwrap()
            .unwrap()
        }

        /// The codes of all points, decoded.
        fn decoded(&self) -> Vec<Vec<f32>> {
            let mut decoded = vec![Vec::new(); POINTS];
            codes(&self.quantized)
                .unwrap()
                .for_each_in_batch(&self.points, &mut |i, vector, _| {
                    decoded[i] = vector.to_vec();
                })
                .unwrap();
            decoded
        }
    }

    /// A quantized build projects the decoded codes as they are, and points whose codes score
    /// alike against every point are copies: the exact copies, and also the near-copies where
    /// their codes differ only in a correction the score ignores. None of it comes from the
    /// originals, which are NaN by then.
    #[rstest]
    fn sketches_and_fingerprints_of_the_codes(
        #[values(
            scalar(),
            binary(Encoding::OneBit),
            binary(Encoding::OneAndHalfBits),
            binary(Encoding::TwoBits),
            turbo(Bits::Bits1),
            turbo(Bits::Bits1_5),
            turbo(Bits::Bits2),
            turbo(Bits::Bits4),
            turbo(Bits::Bits8)
        )]
        config: QuantizationConfig,
    ) {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(&config, Distance::Cosine, false);
        let scorer = fixture.scorer();
        let mut alike: HashMap<Vec<u32>, Vec<_>> = HashMap::new();
        for &point in &fixture.points {
            let scores = fixture.points.iter();
            let scores = scores.map(|&other| scorer.score_internal(point, other).to_bits());
            alike.entry(scores.collect()).or_default().push(point);
        }
        drop(scorer);
        let mut copies: Vec<_> = alike.into_values().filter(|g| g.len() > 1).collect();
        copies.sort();
        assert!(copies.len() >= COPIES);

        for &point in &fixture.points {
            let nan = VectorRef::from(&[f32::NAN; DIM][..]);
            fixture.storage.insert_vector(point, nan).unwrap();
        }
        let (sketches, fingerprints) = fixture.sketches();

        let decoded = fixture.decoded();
        let hyperplanes = hyperplanes(decoded[0].len(), SEED);
        let bits = |projections: [f32; HASH_BITS]| projections.map(f32::to_bits);
        let projected = decoded
            .iter()
            .map(|v| bits(project(v, &hyperplanes, false).0));
        let got = sketches.projections.iter().map(|&p| bits(p));
        assert!(got.eq(projected));
        let groups = duplicates::find(&fixture.points, &sketches, &fingerprints);
        assert_eq!(groups, copies);
    }

    /// Pair scores are an affine map of the dot product of the decoded codes, or of their negative
    /// squared distance for Euclid. Only approximately at 2 and 4 bits of TurboQuant, whose score
    /// adds a stored TQ+ correction: there the decoded codes still explain the scores ten times
    /// better than the original vectors do.
    #[rstest]
    fn decoded_codes_score_as_the_scorer(
        #[values(
            scalar(),
            binary(Encoding::OneBit),
            binary(Encoding::OneAndHalfBits),
            binary(Encoding::TwoBits),
            turbo(Bits::Bits1),
            turbo(Bits::Bits1_5),
            turbo(Bits::Bits2),
            turbo(Bits::Bits4),
            turbo(Bits::Bits8)
        )]
        config: QuantizationConfig,
        #[values(Distance::Cosine, Distance::Dot, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(&config, distance, false);
        let decoded = fixture.decoded();
        let scorer = fixture.scorer();
        let score = |a: usize, b: usize| {
            f64::from(scorer.score_internal(a as PointOffsetType, b as PointOffsetType))
        };
        let geometry = |u: &[f32], v: &[f32]| {
            let pairs = u.iter().zip(v).map(|(&x, &y)| (f64::from(x), f64::from(y)));
            match distance {
                Distance::Euclid => -pairs.map(|(x, y)| (x - y).powi(2)).sum::<f64>(),
                Distance::Cosine | Distance::Dot => pairs.map(|(x, y)| x * y).sum(),
                Distance::Manhattan => unreachable!("no Manhattan case"),
            }
        };
        let (mut scores, mut of_decoded, mut of_originals) = (Vec::new(), Vec::new(), Vec::new());
        for a in 0..POINTS {
            for b in a + 1..POINTS {
                // The scores of each vector with itself cancel the lengths L2 stores
                scores.push(if distance == Distance::Euclid {
                    score(a, b) - (score(a, a) + score(b, b)) / 2.0
                } else {
                    score(a, b)
                });
                of_decoded.push(geometry(&decoded[a], &decoded[b]));
                of_originals.push(geometry(&fixture.vectors[a], &fixture.vectors[b]));
            }
        }

        let approximate = match &config {
            QuantizationConfig::Turbo(TurboQuantization { turbo }) => {
                match turbo.bits.unwrap_or_default() {
                    Bits::Bits2 | Bits::Bits4 => true,
                    Bits::Bits1 | Bits::Bits1_5 | Bits::Bits8 => false,
                }
            }
            QuantizationConfig::Scalar(_)
            | QuantizationConfig::Product(_)
            | QuantizationConfig::Binary(_) => false,
        };
        let agreement = correlation(&scores, &of_decoded);
        let least = if approximate {
            1.0 - (1.0 - correlation(&scores, &of_originals)) / 10.0
        } else {
            1.0 - 1e-5
        };
        assert!(agreement >= least, "{agreement} below {least}");
    }

    /// Pearson's correlation coefficient.
    fn correlation(a: &[f64], b: &[f64]) -> f64 {
        let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
        let (mean_a, mean_b) = (mean(a), mean(b));
        let centered = |v: &[f64], m: f64| v.iter().map(move |x| x - m).collect::<Vec<_>>();
        let (a, b) = (centered(a, mean_a), centered(b, mean_b));
        let dot = |x: &[f64], y: &[f64]| x.iter().zip(y).map(|(x, y)| x * y).sum::<f64>();
        dot(&a, &b) / (dot(&a, &a) * dot(&b, &b)).sqrt()
    }

    /// A NaN in an original only counts where its code scores NaN: binary codes and TurboQuant's
    /// Cosine codes of 1 bit stay finite, a TurboQuant scale (Dot) or TQ+ correction (2 bits)
    /// turns NaN.
    #[rstest]
    #[case::binary(binary(Encoding::OneBit), Distance::Dot, true)]
    #[case::turbo_finite(turbo(Bits::Bits1), Distance::Cosine, true)]
    #[case::turbo_scale(turbo(Bits::Bits1), Distance::Dot, false)]
    #[case::turbo_correction(turbo(Bits::Bits2), Distance::Cosine, false)]
    fn nan_original(
        #[case] config: QuantizationConfig,
        #[case] distance: Distance,
        #[case] fingerprinted: bool,
    ) {
        let _scope = ambient::test_guard();
        let (_, fingerprints) = Fixture::new(&config, distance, true).sketches();
        assert_eq!(fingerprints.get(3).is_some(), fingerprinted);
    }
}
