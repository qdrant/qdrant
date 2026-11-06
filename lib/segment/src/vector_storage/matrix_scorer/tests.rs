//! The matrix scorer against brute force over `FilteredScorer::new_internal`, which picks
//! quantized or original vectors the way the HNSW build does. Checks both that the operands
//! reproduce `score_internal` and that the matrix scorer chooses the same vectors.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::bitvec::BitVec;
use common::types::PointOffsetType;
use quantization::turboquant::TQBits;
use quantization::turboquant::matrix::CODES_IN_F32;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _};
use rstest::rstest;
use tempfile::TempDir;

use super::*;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
    QuantizationConfig, ScalarQuantizationConfig, TurboQuantBitSize, TurboQuantQuantizationConfig,
    TurboQuantization,
};
use crate::vector_storage::dense::volatile_dense_vector_storage::{
    new_volatile_dense_byte_vector_storage, new_volatile_dense_half_vector_storage,
    new_volatile_dense_vector_storage,
};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectorsStorageType,
};
use crate::vector_storage::turbo::appendable_turbo_vector_storage::open_appendable_turbo_vector_storage;
use crate::vector_storage::{VectorStorage as _, VectorStorageRead as _};

const POINTS: usize = 300;
/// Not a multiple of 16, so padded codes are covered.
const DIM: usize = 100;
const K: usize = 5;

#[derive(Clone, Copy, Debug)]
enum Element {
    F32,
    F16,
    U8,
    /// A TurboQuant datatype storage
    Turbo(TQBits),
}

/// How close the matrix scores must be to `score_internal`.
#[derive(Clone, Copy, Debug)]
enum Tolerance {
    /// The same scores and picks.
    Exact,
    /// Relative to the score, at least 1.
    Score(f32),
    /// Relative to the magnitude of the inputs, `‖a‖·‖b‖`, or `‖a‖² + ‖b‖²` for Euclid: the
    /// error of a sum in f32 scales with it, and Euclid cancels it for close pairs.
    Input(f32),
}

struct Fixture {
    vectors: Vec<Vec<VectorElementType>>,
    distance: Distance,
    storage: VectorStorageEnum,
    quantized: Option<QuantizedVectors>,
    _dir: TempDir,
}

impl Fixture {
    fn new(element: Element, distance: Distance, quantization: Option<QuantizationConfig>) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        let dir = tempfile::tempdir().unwrap();
        let mut storage = match element {
            Element::F32 => new_volatile_dense_vector_storage(DIM, distance),
            Element::F16 => new_volatile_dense_half_vector_storage(DIM, distance),
            Element::U8 => new_volatile_dense_byte_vector_storage(DIM, distance),
            Element::Turbo(bits) => VectorStorageEnum::DenseTurboAppendableMemmap(Box::new(
                open_appendable_turbo_vector_storage(dir.path(), DIM, distance, bits, true)
                    .unwrap(),
            )),
        };
        let mut vectors = Vec::with_capacity(POINTS);
        for id in 0..POINTS as PointOffsetType {
            let vector: Vec<VectorElementType> = match element {
                Element::F32 | Element::F16 | Element::Turbo(_) => {
                    let v = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
                    distance.preprocess_vector::<VectorElementType>(v)
                }
                Element::U8 => (0..DIM).map(|_| f32::from(rng.random::<u8>())).collect(),
            };
            storage.insert_vector(id, VectorRef::from(&vector)).unwrap();
            vectors.push(vector);
        }
        let quantized = quantization.map(|config| {
            QuantizedVectors::create(
                &storage,
                &config,
                QuantizedVectorsStorageType::Immutable,
                &dir.path().join("quantized"),
                1,
                &AtomicBool::new(false),
            )
            .unwrap()
        });
        Self {
            vectors,
            distance,
            storage,
            quantized,
            _dir: dir,
        }
    }

    /// The best `K` columns of each row by brute force, as `(column position, score)`.
    fn expected(&self, rows: &[PointOffsetType], cols: MatrixCols) -> Vec<Vec<(u32, f32)>> {
        let deleted = BitVec::repeat(false, POINTS);
        let scorer =
            FilteredScorer::new_internal(0, &self.storage, self.quantized.as_ref(), None, &deleted)
                .unwrap();
        let col_ids = match cols {
            MatrixCols::Rows => rows,
            MatrixCols::Other(cols) => cols,
        };
        rows.iter()
            .enumerate()
            .map(|(r, &row)| {
                let mut scores: Vec<(u32, f32)> = col_ids
                    .iter()
                    .enumerate()
                    .filter(|&(c, _)| !matches!(cols, MatrixCols::Rows) || c != r)
                    .map(|(c, &col)| (c as u32, scorer.score_internal(row, col)))
                    .collect();
                scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                scores.truncate(K);
                scores
            })
            .collect()
    }

    /// See [`Tolerance::Input`].
    fn magnitude(&self, row: PointOffsetType) -> f32 {
        let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
        let max = self.vectors.iter().map(|v| norm(v)).fold(0.0, f32::max);
        let own = norm(&self.vectors[row as usize]);
        match self.distance {
            Distance::Euclid => own * own + max * max,
            Distance::Dot | Distance::Cosine | Distance::Manhattan => own * max,
        }
    }

    fn check(&self, tolerance: Tolerance) {
        let scorer = new_matrix_scorer(&self.storage, self.quantized.as_ref())
            .expect("storage has a matrix operands");
        self.check_scorer(&scorer, tolerance);
    }

    /// [`Self::check`] with TurboQuant operands that decode the Normal 2, 4, and 8 bits to f32,
    /// as on aarch64.
    #[expect(clippy::wildcard_enum_match_arm, reason = "test code")]
    fn check_codes_in_f32(&self, tolerance: Tolerance) {
        let scorer = match (&self.storage, &self.quantized) {
            (_, Some(quantized)) => match quantized.get_storage() {
                QuantizedVectorStorage::TQRam(q) => {
                    turbo_matrix_scorer(q.matrix_operands_with(true))
                }
                QuantizedVectorStorage::TQMmap(q) => {
                    turbo_matrix_scorer(q.matrix_operands_with(true))
                }
                QuantizedVectorStorage::TQChunkedMmap(q) => {
                    turbo_matrix_scorer(q.matrix_operands_with(true))
                }
                _ => panic!("not TurboQuant quantization"),
            },
            (VectorStorageEnum::DenseTurboAppendableMemmap(storage), None) => {
                turbo_matrix_scorer(TurboMatrixOperands::new_with(
                    storage.quantizer(),
                    TurboStorageSource(storage.as_ref()),
                    invert_score(storage.distance()),
                    true,
                ))
            }
            _ => panic!("not a TurboQuant storage"),
        };
        self.check_scorer(&scorer.expect("storage has matrix operands"), tolerance);
    }

    fn check_scorer(&self, scorer: &MatrixScorer<'_>, tolerance: Tolerance) {
        let mut ids: Vec<PointOffsetType> = (0..POINTS as PointOffsetType).collect();
        ids.shuffle(&mut StdRng::seed_from_u64(7));
        let leaf = &ids[..200];
        // Overlaps the rows, so some points score against themselves
        let (part_rows, leaders) = (&ids[100..250], &ids[230..270]);

        for (rows, cols) in [
            (leaf, MatrixCols::Rows),
            (part_rows, MatrixCols::Other(leaders)),
        ] {
            let mut out = MatrixTopK::new(K);
            scorer.top_k(rows, cols, &mut out).unwrap();
            let expected = self.expected(rows, cols);
            assert_eq!(out.rows(), rows.len());
            for (r, expected) in expected.iter().enumerate() {
                let (got_cols, got_scores) = out.row(r);
                let allowed = |want: f32| match tolerance {
                    Tolerance::Exact => unreachable!(),
                    Tolerance::Score(tolerance) => tolerance * want.abs().max(1.0),
                    Tolerance::Input(tolerance) => tolerance * self.magnitude(rows[r]),
                };
                match tolerance {
                    Tolerance::Exact => {
                        let got: Vec<_> = got_cols
                            .iter()
                            .copied()
                            .zip(got_scores.iter().copied())
                            .collect();
                        assert_eq!(&got, expected, "row {r}");
                    }
                    // Near-ties may swap, so compare the scores by rank
                    Tolerance::Score(_) | Tolerance::Input(_) => {
                        assert_eq!(got_scores.len(), expected.len(), "row {r}");
                        for (&got, &(_, want)) in got_scores.iter().zip(expected) {
                            let allowed = allowed(want);
                            assert!(
                                (got - want).abs() <= allowed,
                                "row {r}: {got} vs {want}, allowed {allowed}",
                            );
                        }
                    }
                }
            }
        }
    }
}

#[expect(deprecated, reason = "always_ram")]
fn scalar() -> QuantizationConfig {
    ScalarQuantizationConfig {
        r#type: Default::default(),
        quantile: None,
        always_ram: None,
        memory: None,
    }
    .into()
}

#[expect(deprecated, reason = "always_ram")]
fn binary(encoding: BinaryQuantizationEncoding) -> QuantizationConfig {
    QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: None,
            memory: None,
            encoding: Some(encoding),
            query_encoding: None,
        },
    })
}

/// Summation order differs from the SIMD scorers, and Euclid is `2·dot − ‖a‖² − ‖b‖²`.
///
/// On aarch64 the f16 scorers sum in f16 (`vfmaq_f16` in `spaces/metric_f16/cpp/neon.c`), so
/// the reference itself is only about f16-precise there. Replaying that summation over these
/// fixtures puts it up to 4.2e-3 away from the f32 result.
#[rstest]
fn original_vectors(
    #[values(Element::F32, Element::F16, Element::U8)] element: Element,
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    let tolerance = if matches!(element, Element::F16) && cfg!(target_arch = "aarch64") {
        1e-2
    } else {
        1e-4
    };
    Fixture::new(element, distance, None).check(Tolerance::Score(tolerance));
}

/// Integer inner products in the same order of operations: exact.
#[rstest]
fn scalar_quantization(
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::F32, distance, Some(scalar())).check(Tolerance::Exact);
}

/// Popcounts: exact.
#[rstest]
fn binary_quantization(
    #[values(
        BinaryQuantizationEncoding::OneBit,
        BinaryQuantizationEncoding::TwoBits,
        BinaryQuantizationEncoding::OneAndHalfBits
    )]
    encoding: BinaryQuantizationEncoding,
    #[values(Distance::Dot, Distance::Euclid, Distance::Manhattan)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::F32, distance, Some(binary(encoding))).check(Tolerance::Exact);
}

#[expect(deprecated, reason = "always_ram")]
fn turbo(bits: TurboQuantBitSize) -> QuantizationConfig {
    QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: None,
            memory: None,
            bits: Some(bits),
        },
    })
}

/// Popcounts, combined in the same order of operations: exact.
#[rstest]
fn turbo_quantization_one_bit(
    #[values(TurboQuantBitSize::Bits1, TurboQuantBitSize::Bits1_5)] bits: TurboQuantBitSize,
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::F32, distance, Some(turbo(bits))).check(Tolerance::Exact);
}

/// TQ+ below 8 bits: weighted codes summed in f32, about 1e-7 of the input magnitude, see the
/// TurboQuant matrix tests in `quantization`.
#[rstest]
fn turbo_quantization_weighted(
    #[values(TurboQuantBitSize::Bits2, TurboQuantBitSize::Bits4)] bits: TurboQuantBitSize,
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::F32, distance, Some(turbo(bits))).check(Tolerance::Input(2e-6));
}

/// The integer operands, or on aarch64 their codes decoded to f32, which round once the sums
/// pass 2²⁴, see `CODES_IN_F32`.
fn codes_tolerance() -> Tolerance {
    if CODES_IN_F32 {
        Tolerance::Input(2e-6)
    } else {
        Tolerance::Exact
    }
}

/// Integer sums of grid values, in the same order of operations: exact, except on aarch64.
#[rstest]
fn turbo_quantization_eight_bits(
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    let config = turbo(TurboQuantBitSize::Bits8);
    Fixture::new(Element::F32, distance, Some(config)).check(codes_tolerance());
}

/// The aarch64 operands of [`turbo_quantization_eight_bits`] on every platform.
#[rstest]
fn turbo_quantization_eight_bits_in_f32(
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    let config = turbo(TurboQuantBitSize::Bits8);
    Fixture::new(Element::F32, distance, Some(config)).check_codes_in_f32(Tolerance::Input(2e-6));
}

/// As TurboQuant quantization: exact, except on aarch64.
#[rstest]
fn turbo_storage(
    #[values(TQBits::Bits4, TQBits::Bits8)] bits: TQBits,
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::Turbo(bits), distance, None).check(codes_tolerance());
}

/// The aarch64 operands of [`turbo_storage`] on every platform.
#[rstest]
fn turbo_storage_in_f32(
    #[values(TQBits::Bits4, TQBits::Bits8)] bits: TQBits,
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::Turbo(bits), distance, None).check_codes_in_f32(Tolerance::Input(2e-6));
}

/// 16-bit codes summed in f32, restarted every 256: about 5e-7 of the input magnitude, see
/// the TurboQuant matrix tests in `quantization`.
#[rstest]
fn turbo16_storage(
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::Turbo(TQBits::Bits16), distance, None).check(Tolerance::Input(2e-6));
}

#[test]
fn no_matrix_form() {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Manhattan, None);
    assert!(new_matrix_scorer(&fixture.storage, None).is_none());
    let fixture = Fixture::new(Element::F32, Distance::Manhattan, Some(scalar()));
    assert!(new_matrix_scorer(&fixture.storage, fixture.quantized.as_ref()).is_none());
    let fixture = Fixture::new(Element::Turbo(TQBits::Bits16), Distance::Manhattan, None);
    assert!(new_matrix_scorer(&fixture.storage, None).is_none());
}
