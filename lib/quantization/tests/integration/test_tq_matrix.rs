//! TurboQuant matrix operands against `score_internal`: the best columns of each row by brute
//! force, through every kernel the CPU supports. Exact for the widths that keep their codes;
//! within a tolerance for those decoded to f32: 16 bits, TQ+ 2 and 4 bits, and on aarch64
//! (or forced with `codes_in_f32`) the Normal 2, 4, and 8 bits.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::types::PointOffsetType;
use pairwise::{Cols, Kernel, TopK};
use quantization::encoded_storage::{TestEncodedStorage, TestEncodedStorageBuilder};
use quantization::encoded_vectors::{DistanceType, EncodedVectors, VectorParameters};
use quantization::encoded_vectors_tq::{self, EncodedVectorsTQ};
use quantization::turboquant::matrix::CODES_IN_F32;
use quantization::turboquant::{TQBits, TQMode, TQRotation};
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _};

const POINTS: usize = 300;
const K: usize = 5;

struct Fixture {
    vectors: Vec<Vec<f32>>,
    encoded: EncodedVectorsTQ<TestEncodedStorage>,
    distance: DistanceType,
}

impl Fixture {
    fn new(dim: usize, bits: TQBits, mode: TQMode, distance: DistanceType, invert: bool) -> Self {
        let mut rng = StdRng::seed_from_u64(dim as u64);
        let vectors: Vec<Vec<f32>> = (0..POINTS)
            .map(|_| {
                let v: Vec<f32> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();
                match distance {
                    DistanceType::Cosine => {
                        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                        v.iter().map(|x| x / norm).collect()
                    }
                    DistanceType::Dot | DistanceType::L1 | DistanceType::L2 => v,
                }
            })
            .collect();
        let vector_parameters = VectorParameters {
            dim,
            deprecated_count: None,
            distance_type: distance,
            invert,
        };
        let size = encoded_vectors_tq::get_quantized_vector_size(&vector_parameters, bits, mode);
        let encoded = EncodedVectorsTQ::encode(
            vectors.iter(),
            std::iter::repeat_n(true, POINTS),
            TestEncodedStorageBuilder::new(None, size),
            &vector_parameters,
            bits,
            mode,
            TQRotation::Padded,
            false,
            1,
            None,
            &AtomicBool::new(false),
        )
        .unwrap();
        Self {
            vectors,
            encoded,
            distance,
        }
    }

    /// The best `K` columns of each row by `score_internal`, as `(column position, score)`.
    fn expected(&self, rows: &[PointOffsetType], cols: Cols) -> Vec<Vec<(u32, f32)>> {
        let col_ids = match cols {
            Cols::Rows => rows,
            Cols::Other(cols) => cols,
        };
        rows.iter()
            .enumerate()
            .map(|(r, &row)| {
                let mut scores: Vec<(u32, f32)> = col_ids
                    .iter()
                    .enumerate()
                    .filter(|&(c, _)| !matches!(cols, Cols::Rows) || c != r)
                    .map(|(c, &col)| (c as u32, self.encoded.score_internal(row, col)))
                    .collect();
                scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                scores.truncate(K);
                scores
            })
            .collect()
    }

    /// Magnitude of the scores of `row`: the error of a sum in f32 scales with it, not with
    /// the score, which cancels for close pairs under L2.
    fn magnitude(&self, row: PointOffsetType) -> f32 {
        let norm = |v: &[f32]| v.iter().map(|x| x * x).sum::<f32>().sqrt();
        let max = self.vectors.iter().map(|v| norm(v)).fold(0.0, f32::max);
        let own = norm(&self.vectors[row as usize]);
        match self.distance {
            DistanceType::L2 => own * own + max * max,
            DistanceType::Dot | DistanceType::Cosine | DistanceType::L1 => own * max,
        }
    }

    /// `tolerance`: `None` for exact scores and picks, else the allowed score error relative
    /// to [`Self::magnitude`]. `codes_in_f32` as `matrix_operands_with` takes it.
    fn check(&self, codes_in_f32: bool, tolerance: Option<f32>, what: &str) {
        let operands = self
            .encoded
            .matrix_operands_with(codes_in_f32)
            .unwrap_or_else(|| panic!("{what}: no matrix operands"));

        let mut ids: Vec<PointOffsetType> = (0..POINTS as PointOffsetType).collect();
        ids.shuffle(&mut StdRng::seed_from_u64(7));
        let leaf = &ids[..200];
        // Overlaps the rows, so some points score against themselves
        let (part_rows, leaders) = (&ids[100..250], &ids[230..270]);

        for kernel in Kernel::available() {
            for (rows, cols) in [(leaf, Cols::Rows), (part_rows, Cols::Other(leaders))] {
                let mut out = TopK::new(K);
                operands.top_k_with(kernel, rows, cols, &mut out).unwrap();
                let expected = self.expected(rows, cols);
                assert_eq!(out.rows(), rows.len());
                for (r, expected) in expected.iter().enumerate() {
                    let (got_cols, got_scores) = out.row(r);
                    match tolerance {
                        None => {
                            let got: Vec<_> = got_cols
                                .iter()
                                .copied()
                                .zip(got_scores.iter().copied())
                                .collect();
                            assert_eq!(&got, expected, "{what}, {kernel:?}, row {r}");
                        }
                        // Near-ties may swap, so compare the scores by rank
                        Some(tolerance) => {
                            assert_eq!(got_scores.len(), expected.len(), "{what}, row {r}");
                            let allowed = tolerance * self.magnitude(rows[r]);
                            for (&got, &(_, want)) in got_scores.iter().zip(expected) {
                                assert!(
                                    (got - want).abs() <= allowed,
                                    "{what}, {kernel:?}, row {r}: {got} vs {want}, allowed {allowed}",
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

const DISTANCES: [DistanceType; 3] = [DistanceType::Dot, DistanceType::Cosine, DistanceType::L2];

/// Every width, mode, distance, and sign through `check`, at dims that pad the codes, and
/// that cross the depth slices of every kernel (256 f32 values, 1024 codes) with a tail of an
/// odd number of 8-bit pair steps. The sign only at the small one.
fn check_all(bits: TQBits, modes: &[TQMode], tolerance: Option<f32>) {
    check_all_with(bits, modes, CODES_IN_F32, tolerance);
}

/// [`check_all`] with the Normal 2, 4, and 8 bits decoded to f32 if `codes_in_f32`.
fn check_all_with(bits: TQBits, modes: &[TQMode], codes_in_f32: bool, tolerance: Option<f32>) {
    let _scope = ambient::test_guard();
    for (dim, inverts) in [(100, &[false, true][..]), (1030, &[false][..])] {
        for &mode in modes {
            for distance in DISTANCES {
                for &invert in inverts {
                    let what = format!(
                        "{bits:?}, {mode:?}, {distance:?}, invert {invert}, dim {dim}, \
                         codes in f32 {codes_in_f32}"
                    );
                    Fixture::new(dim, bits, mode, distance, invert).check(
                        codes_in_f32,
                        tolerance,
                        &what,
                    );
                }
            }
        }
    }
}

/// Popcounts, combined in the same order of operations: exact.
#[test]
fn one_bit() {
    check_all(TQBits::Bits1, &[TQMode::Normal, TQMode::Plus], None);
    check_all(TQBits::Bits1_5, &[TQMode::Normal, TQMode::Plus], None);
}

/// Integer sums of codebook values, in the same order of operations: exact. Decoded to f32 on
/// aarch64, see [`codebooks_in_f32`].
#[test]
fn codebooks() {
    let tolerance = CODES_IN_F32.then_some(2e-6);
    check_all(TQBits::Bits2, &[TQMode::Normal], tolerance);
    check_all(TQBits::Bits4, &[TQMode::Normal], tolerance);
}

/// The aarch64 operands of [`codebooks`] on every platform: codebook values summed in f32,
/// restarted every 256. Exact here, as the sums stay within 2²⁴.
#[test]
fn codebooks_in_f32() {
    check_all_with(TQBits::Bits2, &[TQMode::Normal], true, Some(2e-6));
    check_all_with(TQBits::Bits4, &[TQMode::Normal], true, Some(2e-6));
}

/// TQ+ codes weighted and summed in f32, restarted every 256: up to 3.6e-7 of the input
/// magnitude here.
#[test]
fn weighted_codebooks() {
    check_all(TQBits::Bits2, &[TQMode::Plus], Some(2e-6));
    check_all(TQBits::Bits4, &[TQMode::Plus], Some(2e-6));
}

/// Integer sums of grid values: exact. Decoded to f32 on aarch64, see
/// [`eight_bits_in_f32`].
#[test]
fn eight_bits() {
    check_all(
        TQBits::Bits8,
        &[TQMode::Normal],
        CODES_IN_F32.then_some(2e-6),
    );
}

/// The aarch64 operands of [`eight_bits`] on every platform: grid values summed in f32,
/// restarted every 256. Exact here, as the sums stay below 2²⁴.
#[test]
fn eight_bits_in_f32() {
    check_all_with(TQBits::Bits8, &[TQMode::Normal], true, Some(2e-6));
}

/// Codes summed in f32, restarted every 256: up to 5.1e-7 of the input magnitude here,
/// including the reference's own error.
#[test]
fn sixteen_bits() {
    check_all(TQBits::Bits16, &[TQMode::Normal], Some(2e-6));
}

#[test]
fn l1_has_no_matrix_form() {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(64, TQBits::Bits4, TQMode::Normal, DistanceType::L1, false);
    assert!(fixture.encoded.matrix_operands().is_none());
}
