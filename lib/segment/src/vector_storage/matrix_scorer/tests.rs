//! The matrix scorer against brute force over `FilteredScorer::new_internal`, which picks
//! quantized or original vectors the way the HNSW build does. Checks both that the operands
//! reproduce `score_internal` and that the matrix scorer chooses the same vectors.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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

use super::cpu_jobs::ROWS_PER_CALL;
use super::top_k_pool::TopKPool;
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

pub(super) const POINTS: usize = 300;
/// Not a multiple of 16, so padded codes are covered.
const DIM: usize = 100;
const K: usize = 5;

#[derive(Clone, Copy, Debug)]
pub(super) enum Element {
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

pub(super) struct Fixture {
    vectors: Vec<Vec<VectorElementType>>,
    distance: Distance,
    pub storage: VectorStorageEnum,
    pub quantized: Option<QuantizedVectors>,
    _dir: TempDir,
}

impl Fixture {
    pub fn new(
        element: Element,
        distance: Distance,
        quantization: Option<QuantizationConfig>,
    ) -> Self {
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
pub(super) fn scalar() -> QuantizationConfig {
    ScalarQuantizationConfig {
        r#type: Default::default(),
        quantile: None,
        always_ram: None,
        memory: None,
    }
    .into()
}

#[expect(deprecated, reason = "always_ram")]
pub(super) fn binary(encoding: BinaryQuantizationEncoding) -> QuantizationConfig {
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
pub(super) fn turbo(bits: TurboQuantBitSize) -> QuantizationConfig {
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

/// Uneven job sizes, including jobs without pairs.
const JOB_SIZES: &[usize] = &[150, 0, 1, 2, 3, 5, 200, 17, 64, 2, 120, 1, 90, 33];

/// What jobs keep per row: all the phase does, fewer, and nothing.
const JOB_KS: &[usize] = &[K, 2, K - 1, 0];

/// Shuffled points, and more of them than a matrix call against other points takes, which
/// may repeat a point.
struct JobPoints {
    ids: Vec<PointOffsetType>,
    many: Vec<PointOffsetType>,
}

impl JobPoints {
    fn new() -> Self {
        let mut ids: Vec<PointOffsetType> = (0..POINTS as PointOffsetType).collect();
        ids.shuffle(&mut StdRng::seed_from_u64(7));
        let many = ids
            .iter()
            .copied()
            .cycle()
            .take(3 * ROWS_PER_CALL)
            .collect();
        Self { ids, many }
    }

    /// Jobs of every shape:
    /// - First, jobs against the leaders `ids[30..70]` that are cut into several matrix calls:
    ///   three calls with a short last one, one row more than a call, and two calls without
    ///   columns, so without picks.
    /// - Then jobs of one call: against the leaders with exactly the rows of a call, without
    ///   rows of either kind, and a job per size of [`JOB_SIZES`], alternating between `Rows`
    ///   and the leaders, over windows of `ids` that overlap each other. Two of those against
    ///   the leaders have leaders among their rows, which score against themselves.
    ///
    /// Each keeps one of [`JOB_KS`] per row, in turn.
    fn jobs(&self) -> Vec<MatrixJob<'_>> {
        let Self { ids, many } = self;
        let leaders = MatrixCols::Other(&ids[30..70]);
        let job = |rows, cols| MatrixJob { rows, cols, k: K };
        let mut jobs = vec![
            job(&many[..2 * ROWS_PER_CALL + 452], leaders),
            job(&many[3..3 + ROWS_PER_CALL + 1], leaders),
            job(&many[..2 * ROWS_PER_CALL], MatrixCols::Other(&[])),
            job(&many[7..7 + ROWS_PER_CALL], leaders),
            job(&[], leaders),
            job(&[], MatrixCols::Rows),
        ];
        jobs.extend(JOB_SIZES.iter().enumerate().map(|(j, &size)| {
            let start = (j * 13) % (ids.len() - size);
            let cols = if j % 2 == 0 {
                MatrixCols::Rows
            } else {
                leaders
            };
            job(&ids[start..start + size], cols)
        }));
        for (job, &k) in jobs.iter_mut().zip(JOB_KS.iter().cycle()) {
            job.k = k;
        }
        jobs
    }
}

type Rows = Vec<(Vec<u32>, Vec<f32>)>;

fn rows_of<'a>(rows: usize, row: impl Fn(usize) -> (&'a [u32], &'a [f32])) -> Rows {
    (0..rows)
        .map(|r| {
            let (cols, scores) = row(r);
            (cols.to_vec(), scores.to_vec())
        })
        .collect()
}

fn pool(threads: usize) -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("matrix-scorer-test-{i}"))
        .build()
        .unwrap()
}

/// Every job is consumed on the current pool, with its own payload and the first `k` of the
/// job of each row of one `top_k` call over all its rows, also if it was cut into several
/// calls. By then, all its rows were reported as scored. Without jobs, nothing is consumed.
#[rstest]
fn for_each_top_k_matches_top_k(
    #[values(None, Some(scalar()))] quantization: Option<QuantizationConfig>,
    #[values(1, 4)] threads: usize,
) {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Dot, quantization);
    let scorer = new_matrix_scorer(&fixture.storage, fixture.quantized.as_ref()).unwrap();
    let points = JobPoints::new();
    let jobs = points.jobs();
    let not_stopped = AtomicBool::new(false);

    // The payload of a job: where its result goes, exclusively, and its rows scored so far
    let mut consumed: Vec<Option<Rows>> = vec![None; jobs.len()];
    pool(threads).install(|| {
        let phase = scorer.phase(K);
        let payloads = consumed
            .iter_mut()
            .zip(&jobs)
            .map(|(slot, job)| (slot, job.rows.len(), AtomicUsize::new(0)));
        phase
            .for_each_top_k(
                jobs.iter().copied().zip(payloads),
                &not_stopped,
                |(_, _, scored), rows| {
                    scored.fetch_add(rows, Ordering::Relaxed);
                },
                |(slot, rows, scored), picks| {
                    let thread = std::thread::current();
                    assert!(thread.name().unwrap().starts_with("matrix-scorer-test-"));
                    assert_eq!(scored.into_inner(), rows);
                    *slot = Some(rows_of(picks.rows(), |r| picks.row(r)));
                    Ok(())
                },
            )
            .unwrap();
        let no_jobs: [(MatrixJob<'_>, ()); 0] = [];
        phase
            .for_each_top_k(
                no_jobs,
                &not_stopped,
                |(), _| panic!("no job to score"),
                |(), _| panic!("no job to consume"),
            )
            .unwrap();
    });

    for (j, (job, consumed)) in jobs.iter().zip(consumed).enumerate() {
        // A fresh buffer per job, so the reference can't share stale state with the pool's.
        // The same kernel on the same operands: the same bits, also for f32
        let mut top = MatrixTopK::new(K);
        scorer.top_k(job.rows, job.cols, &mut top).unwrap();
        assert_eq!(top.rows(), job.rows.len(), "job {j}");
        let expected = rows_of(top.rows(), |r| {
            let (cols, scores) = top.row(r);
            let len = cols.len().min(job.k);
            (&cols[..len], &scores[..len])
        });
        assert_eq!(consumed, Some(expected), "job {j}");
    }
}

/// After a stop no matrix call starts, also not the remaining calls of a job in progress, and
/// that job isn't consumed with a part of its rows.
#[rstest]
fn for_each_top_k_stops_between_matrix_calls(#[values(1, 2)] threads: usize) {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Dot, None);
    let scorer = new_matrix_scorer(&fixture.storage, None).unwrap();
    let points = JobPoints::new();
    // The first job takes three calls, more than there are threads. On one thread, it's first
    let jobs = points.jobs();
    let stopped = AtomicBool::new(false);
    let (calls, consumed) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let result = pool(threads).install(|| {
        cpu_jobs::for_each_top_k(
            &|rows, cols, out| {
                calls.fetch_add(1, Ordering::Relaxed);
                stopped.store(true, Ordering::Relaxed);
                scorer.top_k(rows, cols, out)
            },
            &TopKPool::new(K),
            &jobs,
            &stopped,
            &|_, _| {},
            &|job, _| {
                assert_ne!(job, 0, "consumed with a part of its rows");
                consumed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )
    });
    assert!(matches!(result, Err(OperationError::Cancelled { .. })));
    // A thread stops every other one before its next call
    let calls = calls.into_inner();
    assert!((1..=threads).contains(&calls), "{calls} calls");
    if threads == 1 {
        assert_eq!(consumed.into_inner(), 0);
    }
}

/// The error of a matrix call or of the consumer comes back, and nothing runs after it: a
/// thread is done with its first error.
#[rstest]
fn for_each_top_k_returns_the_error(
    #[values(false, true)] in_matrix_call: bool,
    #[values(1, 4)] threads: usize,
) {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Dot, None);
    let scorer = new_matrix_scorer(&fixture.storage, None).unwrap();
    let points = JobPoints::new();
    // Jobs of one call each, so every call is consumed
    let jobs = &points.jobs()[3..];
    let (calls, consumed) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let result = pool(threads).install(|| {
        cpu_jobs::for_each_top_k(
            &|rows, cols, out| {
                calls.fetch_add(1, Ordering::Relaxed);
                if in_matrix_call {
                    return Err(OperationError::service_error("failed"));
                }
                scorer.top_k(rows, cols, out)
            },
            &TopKPool::new(K),
            jobs,
            &AtomicBool::new(false),
            &|_, _| {},
            &|_, _| {
                consumed.fetch_add(1, Ordering::Relaxed);
                Err(OperationError::service_error("failed"))
            },
        )
    });
    match result {
        Err(OperationError::ServiceError { description, .. }) => {
            assert_eq!(description, "failed");
        }
        other => panic!("{other:?}"),
    }
    let calls = calls.into_inner();
    assert!((1..=threads).contains(&calls), "{calls} calls");
    let consumed = consumed.into_inner();
    assert_eq!(consumed, if in_matrix_call { 0 } else { calls });
}

/// A job is consumed only once all its rows are reported, also if a report of an earlier call
/// is still running when its last call is done.
#[test]
fn for_each_top_k_reports_before_it_consumes() {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Dot, None);
    let scorer = new_matrix_scorer(&fixture.storage, None).unwrap();
    let points = JobPoints::new();
    // Jobs of three and two calls
    let jobs = &points.jobs()[..2];
    let reported: Vec<AtomicUsize> = jobs.iter().map(|_| AtomicUsize::new(0)).collect();
    pool(4)
        .install(|| {
            cpu_jobs::for_each_top_k(
                &|rows, cols, out| scorer.top_k(rows, cols, out),
                &TopKPool::new(K),
                jobs,
                &AtomicBool::new(false),
                &|job, rows| {
                    // The first report of a job outlasts its other calls
                    if reported[job].load(Ordering::Relaxed) == 0 {
                        std::thread::sleep(std::time::Duration::from_millis(30));
                    }
                    reported[job].fetch_add(rows, Ordering::Relaxed);
                },
                &|job, picks| {
                    assert_eq!(reported[job].load(Ordering::Relaxed), picks.rows());
                    Ok(())
                },
            )
        })
        .unwrap();
}

/// A consumer that waits for rayon and scores again, in the same buffers, holds none of them
/// meanwhile: there are never more buffers than threads, where a consumer that kept the buffer
/// of its call would take another one for every job its thread steals while it waits. So the
/// buffers are also reused from one call to the next.
#[rstest]
fn consumers_hold_no_buffer(#[values(1, 3)] threads: usize) {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Dot, None);
    let scorer = new_matrix_scorer(&fixture.storage, None).unwrap();
    let points = JobPoints::new();
    let jobs = points.jobs();
    let tops = TopKPool::new(K);
    let top_k = |rows: &[PointOffsetType], cols: MatrixCols<'_>, out: &mut MatrixTopK| {
        scorer.top_k(rows, cols, out)
    };
    let not_stopped = AtomicBool::new(false);
    let (consumed, nested) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let unreported = |_, _| {};
    pool(threads)
        .install(|| {
            cpu_jobs::for_each_top_k(&top_k, &tops, &jobs, &not_stopped, &unreported, &|_, _| {
                consumed.fetch_add(1, Ordering::Relaxed);
                // Waits for rayon, so this thread runs other calls of the outer loop meanwhile
                cpu_jobs::for_each_top_k(
                    &top_k,
                    &tops,
                    &jobs,
                    &not_stopped,
                    &unreported,
                    &|_, _| {
                        nested.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    },
                )
            })
        })
        .unwrap();
    assert_eq!(consumed.into_inner(), jobs.len());
    assert_eq!(nested.into_inner(), jobs.len() * jobs.len());
    assert!(tops.buffers() <= threads, "{} buffers", tops.buffers());
}

/// Reports and consumers of a phase may score again in the same phase, with their payloads at
/// hand: every job of the nested calls is consumed too.
#[rstest]
fn phase_scores_again_from_its_callbacks(#[values(1, 3)] threads: usize) {
    let _scope = ambient::test_guard();
    let fixture = Fixture::new(Element::F32, Distance::Dot, None);
    let scorer = new_matrix_scorer(&fixture.storage, None).unwrap();
    let points = JobPoints::new();
    let jobs = points.jobs();
    let not_stopped = AtomicBool::new(false);
    let phase = scorer.phase(K);

    // A call of its own for the last job
    let last = *jobs.last().unwrap();
    let score_last = |nested: &AtomicUsize| {
        phase.for_each_top_k(
            [(last, ())],
            &not_stopped,
            |(), _| {},
            |(), picks| {
                assert_eq!(picks.rows(), last.rows.len());
                nested.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )
    };

    let (reports, consumed, nested) = (
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    );
    pool(threads)
        .install(|| {
            phase.for_each_top_k(
                jobs.iter().map(|&job| (job, ())),
                &not_stopped,
                |(), _| {
                    reports.fetch_add(1, Ordering::Relaxed);
                    score_last(&nested).unwrap();
                },
                |(), _| {
                    consumed.fetch_add(1, Ordering::Relaxed);
                    score_last(&nested)
                },
            )
        })
        .unwrap();
    let (reports, consumed) = (reports.into_inner(), consumed.into_inner());
    assert_eq!(consumed, jobs.len());
    // The cut jobs report once per matrix call
    assert!(reports > jobs.len(), "{reports} reports");
    assert_eq!(nested.into_inner(), reports + consumed);
}
