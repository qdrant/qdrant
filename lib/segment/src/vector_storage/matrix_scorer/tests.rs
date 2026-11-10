//! The matrix scorer against brute force over `FilteredScorer::new_internal`, which picks
//! quantized or original vectors the way the HNSW build does. Checks both that the operands
//! reproduce `score_internal` and that the matrix scorer chooses the same vectors.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::bitvec::BitVec;
use common::types::PointOffsetType;
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
    QuantizationConfig, ScalarQuantizationConfig,
};
use crate::vector_storage::VectorStorage as _;
use crate::vector_storage::dense::volatile_dense_vector_storage::{
    new_volatile_dense_byte_vector_storage, new_volatile_dense_half_vector_storage,
    new_volatile_dense_vector_storage,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;

const POINTS: usize = 300;
/// Not a multiple of 16, so padded codes are covered.
const DIM: usize = 100;
const K: usize = 5;

#[derive(Clone, Copy, Debug)]
enum Element {
    F32,
    F16,
    U8,
}

struct Fixture {
    storage: VectorStorageEnum,
    quantized: Option<QuantizedVectors>,
    _dir: TempDir,
}

impl Fixture {
    fn new(element: Element, distance: Distance, quantization: Option<QuantizationConfig>) -> Self {
        let mut rng = StdRng::seed_from_u64(42);
        let mut storage = match element {
            Element::F32 => new_volatile_dense_vector_storage(DIM, distance),
            Element::F16 => new_volatile_dense_half_vector_storage(DIM, distance),
            Element::U8 => new_volatile_dense_byte_vector_storage(DIM, distance),
        };
        for id in 0..POINTS as PointOffsetType {
            let vector: Vec<VectorElementType> = match element {
                Element::F32 | Element::F16 => {
                    let v = (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
                    distance.preprocess_vector::<VectorElementType>(v)
                }
                Element::U8 => (0..DIM).map(|_| f32::from(rng.random::<u8>())).collect(),
            };
            storage.insert_vector(id, VectorRef::from(&vector)).unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let quantized = quantization.map(|config| {
            QuantizedVectors::create(
                &storage,
                &config,
                QuantizedVectorsStorageType::Immutable,
                dir.path(),
                1,
                &AtomicBool::new(false),
            )
            .unwrap()
        });
        Self {
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

    /// `tolerance`: `None` for exact scores and picks, else the allowed relative score error.
    fn check(&self, tolerance: Option<f32>) {
        let scorer = new_matrix_scorer(&self.storage, self.quantized.as_ref())
            .expect("storage has a matrix operands");

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
                match tolerance {
                    None => {
                        let got: Vec<_> = got_cols
                            .iter()
                            .copied()
                            .zip(got_scores.iter().copied())
                            .collect();
                        assert_eq!(&got, expected, "row {r}");
                    }
                    // Near-ties may swap, so compare the scores by rank
                    Some(tolerance) => {
                        assert_eq!(got_scores.len(), expected.len(), "row {r}");
                        for (&got, &(_, want)) in got_scores.iter().zip(expected) {
                            let error = (got - want).abs() / want.abs().max(1.0);
                            assert!(error <= tolerance, "row {r}: {got} vs {want}");
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
    Fixture::new(element, distance, None).check(Some(tolerance));
}

/// Integer inner products in the same order of operations: exact.
#[rstest]
fn scalar_quantization(
    #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
) {
    let _scope = ambient::test_guard();
    Fixture::new(Element::F32, distance, Some(scalar())).check(None);
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
    Fixture::new(Element::F32, distance, Some(binary(encoding))).check(None);
}

#[test]
fn no_matrix_form() {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Manhattan, None);
    assert!(new_matrix_scorer(&fixture.storage, None).is_none());
    let fixture = Fixture::new(Element::F32, Distance::Manhattan, Some(scalar()));
    assert!(new_matrix_scorer(&fixture.storage, fixture.quantized.as_ref()).is_none());
}
