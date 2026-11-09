//! Final prune: cuts each point's reservoir down to the degree limit of its level.
//!
//! Uses the HNSW heuristic (RobustPrune with alpha 1) as HNSW insertion runs it
//! (`link_with_heuristic`): the candidates best first, then
//! [`LinksContainer::fill_from_sorted_with_heuristic`], which drops a candidate that scores higher
//! against a link already selected than against the point. The pair scores come from
//! `score_internal` of the scorer HNSW insertion uses, so quantized vectors are used where
//! configured.
//!
//! - **Candidates keep their reservoir scores**, in the reservoir's order: best first, ties to the
//!   lower id. These come from the matrix kernels, which round differently than `score_internal`
//!   for original vectors (f32, f16, u8) and TurboQuant through f32, and for an edge the point
//!   didn't pick itself from the reverse pick. Measured against rescoring every candidate with
//!   `score_internal` (GIST 100K and 1M, dbpedia 100K, f32 and Turbo16, with duplicate vectors
//!   grouped), the links of 0.03 to 0.07% of the points differ and recall@10 doesn't change.
//!   Rescoring mattered only for exact duplicate vectors, whose ties the heuristic breaks by
//!   exact equality: a link to a copy of the point scores against the other candidates exactly as
//!   the point does, so rounding in the candidates' own scores drops about half of them. The build groups exact duplicates into one
//!   point before partitioning (`duplicates.rs`), so they don't reach the prune. Distinct
//!   vectors whose quantized codes tie still can, and lose some links the same way; rescoring
//!   wasn't measured for them.
//! - **Skipped candidates:** the point itself, and deleted points or vectors, which HNSW search
//!   never finds either. Reservoirs never hold the point itself or NaN scores, and the caller
//!   passes only points that aren't deleted, so none of these should occur.
//!
//! # Memory and Parallelism
//!
//! The heuristic reads the vectors of the candidates it compares, and is bound by memory
//! bandwidth, not compute. Scores are computed from the stored vectors in place: copying the
//! candidates' vectors together first doesn't help, since loading them from DRAM dominates. Only
//! one point's candidates are held per thread at a time, and the result holds at most `level_m`
//! ids per point.
//!
//! Points are pruned in jobs of [`POINTS_PER_JOB`] consecutive points on the current rayon pool,
//! one scorer per job, each writing to its own range of the result. A point's links only depend on
//! its reservoir and on `score_internal`, so the result doesn't depend on the number of threads.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::bitvec::BitSlice;
use common::reason::reason;
use common::types::{PointOffsetType, ScoredPointOffset};
use rayon::prelude::*;

use super::hash_prune::Reservoirs;
use super::progress::Progress;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::index::hnsw_index::links_container::LinksContainer;
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// Points pruned per job, with one scorer, and between checks of the stop flag.
///
/// The same number HNSW insertion runs per job (`HNSW_BUILD_MAX_PAR_LEN`): about 8 ms of work on
/// GIST (1M x 960 at level 0, 10.3 s on 12 threads, so about 124 µs per point and
/// thread). Stopping takes no longer than that, and a level of a million points gives 16k jobs to
/// balance over the threads.
const POINTS_PER_JOB: usize = 64;

/// Links of `points` (the points of one level, sorted, distinct, non-deleted), at most `level_m`
/// each, chosen from their reservoirs by the HNSW heuristic. In the order of `points`.
///
/// Scores with [`FilteredScorer::new_internal`] over `vector_storage` and `quantized_vectors`, as
/// HNSW insertion does, and skips candidates that `point_deleted` or the storage mark as deleted.
/// As for HNSW, `point_deleted` must have a flag for every point: missing flags count as deleted.
///
/// Runs in parallel on the current rayon pool, and checks `stopped` before every
/// [`POINTS_PER_JOB`] points. Adds all of `progress` by the time it returns, each job its share
/// by its points. Fails if a scorer can't be created.
#[expect(
    clippy::too_many_arguments,
    reason = "the level and the inputs of HNSW's scorers"
)]
pub(super) fn prune(
    reservoirs: &Reservoirs,
    points: &[PointOffsetType],
    level_m: usize,
    vector_storage: &VectorStorageEnum,
    quantized_vectors: Option<&QuantizedVectors>,
    point_deleted: &BitSlice,
    stopped: &AtomicBool,
    progress: Progress<'_>,
) -> OperationResult<Vec<Vec<PointOffsetType>>> {
    debug_assert!(
        points.is_sorted_by(|a, b| a < b),
        "points must be sorted and distinct",
    );
    check_process_stopped(stopped)?;
    // Empty lists don't allocate
    let mut links = vec![Vec::new(); points.len()];
    if level_m == 0 || points.is_empty() {
        progress.add();
        return Ok(links);
    }

    let shares = progress.split(points.chunks(POINTS_PER_JOB).map(|job| job.len() as u64));
    points
        .par_chunks(POINTS_PER_JOB)
        .zip(links.par_chunks_mut(POINTS_PER_JOB))
        .zip(shares)
        // Every job stealable until the end, see `HNSW_BUILD_MAX_PAR_LEN`
        .with_max_len(1)
        .try_for_each(|((points, links), progress)| -> OperationResult<()> {
            check_process_stopped(stopped)?;
            let Some(&first) = points.first() else {
                return Ok(());
            };
            let _scope = ambient::unmeasured_guard(reason("Internal operation"));
            // One scorer serves every point of the job: `score_internal(a, b)` scores the stored
            // vectors of `a` and `b`, whatever point the scorer was made for, as HNSW's heuristic
            // relies on for its pairs too. Without quantization, making a scorer copies the
            // point's vector into a query, so this is one copy per job instead of per point.
            // Scorers aren't `Send`, so threads can't share them through a pool.
            let scorer = FilteredScorer::new_internal(
                first,
                vector_storage,
                quantized_vectors,
                None,
                point_deleted,
            )?;
            // Reused, so each point's links are allocated once, at their size
            let mut container = LinksContainer::with_capacity(level_m);
            for (&point, links) in points.iter().zip(links) {
                container.fill_from_sorted_with_heuristic(
                    candidates(reservoirs, point, &scorer).into_iter(),
                    level_m,
                    |a, b| scorer.score_internal(a, b),
                );
                *links = container.links().to_vec();
            }
            progress.add();
            Ok(())
        })?;
    Ok(links)
}

/// The candidates of `point` that can become links, best first by their reservoir scores, ties to
/// the lower id. Leaves out the point itself and deleted points and vectors.
fn candidates(
    reservoirs: &Reservoirs,
    point: PointOffsetType,
    scorer: &FilteredScorer<'_>,
) -> Vec<ScoredPointOffset> {
    let filters = scorer.filters();
    let mut candidates = reservoirs.candidates(point);
    candidates.retain(|candidate| candidate.idx != point && filters.check_vector(candidate.idx));
    candidates
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;

    use common::bitvec::BitVec;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom as _;
    use rand::{RngExt as _, SeedableRng as _};
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::common::operation_error::OperationError;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::types::{
        BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
        QuantizationConfig, ScalarQuantizationConfig,
    };
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;
    use crate::vector_storage::{VectorStorage as _, VectorStorageRead as _};

    /// Points in the storage.
    const POINTS: usize = 400;
    /// Low, so the heuristic drops many candidates.
    const DIM: usize = 16;
    /// Reservoir slots, as at level 0 with m = 16 and a reservoir factor of 2. Room for all
    /// candidates, so the reservoirs keep every one.
    const CAPACITY: usize = 64;
    /// Nearest and random other points that become the candidates of most points.
    const NEAREST: usize = 24;
    const RANDOM: usize = 8;
    /// Small enough that many points reach it.
    const LEVEL_M: usize = 6;
    /// Progress of a call, more units than it has jobs.
    const PROGRESS: u64 = 1_000_003;

    #[derive(Clone, Copy, Debug)]
    enum Data {
        /// Uniform in `[-1, 1)`.
        Random,
        /// Integers in `-3..=3`: many equal scores, so the tie rule matters.
        Integer,
    }

    struct Fixture {
        storage: VectorStorageEnum,
        quantized: Option<QuantizedVectors>,
        deleted: BitVec,
        /// The points of the level: every id but those `% 4 == 3`, so the ids have gaps, as on
        /// upper levels.
        points: Vec<PointOffsetType>,
        _dir: TempDir,
    }

    impl Fixture {
        fn new(data: Data, distance: Distance, quantization: Option<QuantizationConfig>) -> Self {
            let mut rng = StdRng::seed_from_u64(42);
            let mut storage = new_volatile_dense_vector_storage(DIM, distance);
            for id in 0..POINTS as PointOffsetType {
                let vector: Vec<VectorElementType> = match data {
                    Data::Random => (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect(),
                    Data::Integer => (0..DIM)
                        .map(|_| f32::from(rng.random_range(-3i8..=3)))
                        .collect(),
                };
                let vector = distance.preprocess_vector::<VectorElementType>(vector);
                storage.insert_vector(id, VectorRef::from(&vector)).unwrap();
            }
            let dir = tempfile::tempdir().unwrap();
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
                storage,
                quantized,
                deleted: BitVec::repeat(false, POINTS),
                points: (0..POINTS as PointOffsetType)
                    .filter(|p| p % 4 != 3)
                    .collect(),
                _dir: dir,
            }
        }

        /// The scorer of HNSW insertion for `point`, which picks quantized or original vectors.
        fn scorer(&self, point: PointOffsetType) -> FilteredScorer<'_> {
            FilteredScorer::new_internal(
                point,
                &self.storage,
                self.quantized.as_ref(),
                None,
                &self.deleted,
            )
            .unwrap()
        }

        /// The same over the original vectors, as if there were no quantization.
        fn original_scorer(&self, point: PointOffsetType) -> FilteredScorer<'_> {
            FilteredScorer::new_internal(
                point,
                &self.storage,
                None::<&QuantizedVectors>,
                None,
                &self.deleted,
            )
            .unwrap()
        }

        fn is_deleted(&self, point: PointOffsetType) -> bool {
            self.deleted[point as usize] || self.storage.is_deleted_vector(point)
        }

        /// Candidates of each point of the level, scored by `score_internal`, as an exact kernel
        /// scores them:
        /// - none for every 10th point, one random other point for every 10th after it,
        /// - otherwise its [`NEAREST`] nearest points and [`RANDOM`] random others.
        fn candidates(&self, seed: u64) -> Vec<Vec<ScoredPointOffset>> {
            let mut rng = StdRng::seed_from_u64(seed);
            let scorer = self.scorer(0);
            self.points
                .iter()
                .enumerate()
                .map(|(i, &point)| {
                    let mut others: Vec<_> = self
                        .points
                        .iter()
                        .filter(|&&other| other != point)
                        .map(|&idx| ScoredPointOffset {
                            idx,
                            score: scorer.score_internal(point, idx),
                        })
                        .collect();
                    match i % 10 {
                        0 => Vec::new(),
                        1 => vec![others[rng.random_range(0..others.len())]],
                        _ => {
                            others.sort_by(|a, b| {
                                b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx))
                            });
                            let (nearest, rest) = others.split_at_mut(NEAREST);
                            rest.shuffle(&mut rng);
                            nearest.iter().chain(&rest[..RANDOM]).copied().collect()
                        }
                    }
                })
                .collect()
        }

        /// Reservoirs holding exactly `candidates`, each in a bucket of its own.
        fn reservoirs(&self, candidates: &[Vec<ScoredPointOffset>]) -> Reservoirs {
            let reservoirs = Reservoirs::new(&self.points, CAPACITY);
            for (&point, candidates) in self.points.iter().zip(candidates) {
                assert!(candidates.len() <= CAPACITY);
                for (hash, candidate) in candidates.iter().enumerate() {
                    reservoirs.insert(point, candidate.idx, hash as u16, candidate.score);
                }
            }
            reservoirs
        }

        /// [`prune`], which must add all of its progress if it succeeds, and none if stopped
        /// before.
        fn prune(
            &self,
            reservoirs: &Reservoirs,
            level_m: usize,
            stopped: &AtomicBool,
        ) -> OperationResult<Vec<Vec<PointOffsetType>>> {
            let counter = AtomicU64::new(0);
            let links = prune(
                reservoirs,
                &self.points,
                level_m,
                &self.storage,
                self.quantized.as_ref(),
                &self.deleted,
                stopped,
                Progress::new(&counter, PROGRESS),
            );
            let want = if links.is_ok() { PROGRESS } else { 0 };
            assert_eq!(counter.into_inner(), want, "progress");
            links
        }

        /// Prunes reservoirs holding `candidates`, and checks the guarantees of [`prune`].
        fn pruned(
            &self,
            candidates: &[Vec<ScoredPointOffset>],
            level_m: usize,
        ) -> Vec<Vec<PointOffsetType>> {
            let links = self
                .prune(
                    &self.reservoirs(candidates),
                    level_m,
                    &AtomicBool::new(false),
                )
                .unwrap();
            self.check_links(candidates, &links, level_m);
            links
        }

        /// One list per point, each with at most `level_m` distinct links among its candidates,
        /// neither itself nor deleted, and its best candidate first if it may have any.
        #[track_caller]
        fn check_links(
            &self,
            candidates: &[Vec<ScoredPointOffset>],
            links: &[Vec<PointOffsetType>],
            level_m: usize,
        ) {
            assert_eq!(links.len(), self.points.len());
            for ((&point, candidates), links) in self.points.iter().zip(candidates).zip(links) {
                assert!(links.len() <= level_m, "{point}: {links:?}");
                let distinct: HashSet<_> = links.iter().collect();
                assert_eq!(distinct.len(), links.len(), "{point}: {links:?}");
                for link in links {
                    assert_ne!(*link, point, "{point} links to itself");
                    assert!(!self.is_deleted(*link), "{point} links to deleted {link}");
                    assert!(
                        candidates.iter().any(|c| c.idx == *link),
                        "{point} links to {link}, which isn't its candidate",
                    );
                }
                if level_m > 0 {
                    let best = self.sorted(candidates).first().map(|&(idx, _)| idx);
                    assert_eq!(links.first(), best.as_ref(), "{point}");
                }
            }
        }

        /// The candidates that aren't deleted, with their given scores, best first, ties to the
        /// lower id.
        fn sorted(&self, candidates: &[ScoredPointOffset]) -> Vec<(PointOffsetType, f32)> {
            let mut scored: Vec<_> = candidates
                .iter()
                .filter(|c| !self.is_deleted(c.idx))
                .map(|c| (c.idx, c.score))
                .collect();
            scored.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .expect("no NaN in the tests")
                    .then(a.0.cmp(&b.0))
            });
            scored
        }

        /// The links HNSW's heuristic picks on its own for every point, as `link_with_heuristic`
        /// runs it, over the candidates sorted by their given scores, and pair scores from a
        /// scorer made for the point.
        fn expected<'a>(
            &'a self,
            candidates: &[Vec<ScoredPointOffset>],
            level_m: usize,
            scorer: impl Fn(PointOffsetType) -> FilteredScorer<'a>,
        ) -> Vec<Vec<PointOffsetType>> {
            self.points
                .iter()
                .zip(candidates)
                .map(|(&point, candidates)| {
                    heuristic(self.sorted(candidates).into_iter(), level_m, &scorer(point))
                })
                .collect()
        }
    }

    /// [`LinksContainer::fill_from_sorted_with_heuristic`] over `(candidate, score)` pairs.
    fn heuristic(
        sorted: impl Iterator<Item = (PointOffsetType, f32)>,
        level_m: usize,
        scorer: &FilteredScorer<'_>,
    ) -> Vec<PointOffsetType> {
        let mut container = LinksContainer::with_capacity(level_m);
        container.fill_from_sorted_with_heuristic(
            sorted.map(|(idx, score)| ScoredPointOffset { idx, score }),
            level_m,
            |a, b| scorer.score_internal(a, b),
        );
        container.into_vec()
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

    /// Equal links, reporting the first point that differs instead of all of them.
    #[track_caller]
    fn assert_same_links(
        points: &[PointOffsetType],
        got: &[Vec<PointOffsetType>],
        want: &[Vec<PointOffsetType>],
    ) {
        assert_eq!(got.len(), want.len(), "number of points");
        if let Some(i) = (0..got.len()).find(|&i| got[i] != want[i]) {
            panic!("point {}: got {:?}, want {:?}", points[i], got[i], want[i]);
        }
    }

    /// Number of points whose links differ.
    fn differing(a: &[Vec<PointOffsetType>], b: &[Vec<PointOffsetType>]) -> usize {
        a.iter().zip(b).filter(|(a, b)| a != b).count()
    }

    /// The same links as HNSW's heuristic run on its own for every point, also where scores tie.
    /// The heuristic drops candidates, and stops at `level_m` for many points.
    #[rstest]
    fn original_vectors(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid, Distance::Manhattan)]
        distance: Distance,
        #[values(Data::Random, Data::Integer)] data: Data,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(data, distance, None);
        let candidates = fixture.candidates(1);
        let links = fixture.pruned(&candidates, LEVEL_M);
        let expected = fixture.expected(&candidates, LEVEL_M, |p| fixture.scorer(p));
        assert_same_links(&fixture.points, &links, &expected);

        // Both the heuristic and the limit matter
        let full = links.iter().filter(|links| links.len() == LEVEL_M).count();
        let dropped = (candidates.iter().zip(&links))
            .filter(|&(candidates, links)| {
                let sorted: Vec<_> = (fixture.sorted(candidates).into_iter())
                    .map(|(idx, _)| idx)
                    .collect();
                !sorted.starts_with(links)
            })
            .count();
        assert!(
            full >= 10 && dropped >= 10,
            "{full} full, {dropped} dropped"
        );
    }

    /// The heuristic scores with the quantized vectors, like HNSW insertion: the original vectors
    /// would give other links.
    #[rstest]
    fn scalar_quantization(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        check_quantized(&Fixture::new(Data::Random, distance, Some(scalar())));
    }

    /// Popcount scores, with many ties.
    #[rstest]
    fn binary_quantization(
        #[values(
            BinaryQuantizationEncoding::OneBit,
            BinaryQuantizationEncoding::TwoBits
        )]
        encoding: BinaryQuantizationEncoding,
        #[values(Distance::Dot, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        check_quantized(&Fixture::new(
            Data::Random,
            distance,
            Some(binary(encoding)),
        ));
    }

    /// The heuristic on the quantized scores, which differ from the originals' for some points.
    fn check_quantized(fixture: &Fixture) {
        let candidates = fixture.candidates(1);
        let links = fixture.pruned(&candidates, LEVEL_M);
        let expected = fixture.expected(&candidates, LEVEL_M, |p| fixture.scorer(p));
        assert_same_links(&fixture.points, &links, &expected);

        let original = fixture.expected(&candidates, LEVEL_M, |p| fixture.original_scorer(p));
        let changed = differing(&original, &expected);
        assert!(changed > 0, "the originals give the same links");
    }

    /// The candidates keep their reservoir scores, even where they are unrelated to the real
    /// ones: here random and positive.
    #[rstest]
    fn keeps_the_reservoir_scores(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
        #[values(None, Some(scalar()))] quantization: Option<QuantizationConfig>,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, distance, quantization);
        let mut candidates = fixture.candidates(1);
        let mut rng = StdRng::seed_from_u64(2);
        for candidate in candidates.iter_mut().flatten() {
            candidate.score = rng.random_range(0.0..1.0);
        }
        let links = fixture.pruned(&candidates, LEVEL_M);
        let expected = fixture.expected(&candidates, LEVEL_M, |p| fixture.scorer(p));
        assert_same_links(&fixture.points, &links, &expected);

        // Rescoring with `score_internal` would pick other links
        let mut rescored = candidates;
        let scorer = fixture.scorer(0);
        for (&point, candidates) in fixture.points.iter().zip(&mut rescored) {
            for candidate in candidates {
                candidate.score = scorer.score_internal(point, candidate.idx);
            }
        }
        let rescored = fixture.expected(&rescored, LEVEL_M, |p| fixture.scorer(p));
        let changed = differing(&rescored, &expected);
        assert!(
            changed * 2 > fixture.points.len(),
            "{changed} points changed"
        );
    }

    /// Degree limits of 0, 1, below the candidates, and above all of them.
    #[rstest]
    fn degree_limits(#[values(0, 1, 3, 1000)] level_m: usize) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Euclid, Some(scalar()));
        let candidates = fixture.candidates(1);
        let links = fixture.pruned(&candidates, level_m);
        let expected = fixture.expected(&candidates, level_m, |p| fixture.scorer(p));
        assert_same_links(&fixture.points, &links, &expected);

        match level_m {
            0 => assert!(links.iter().all(Vec::is_empty)),
            // `check_links` has the best candidate first, so that's all
            1 => assert!(
                links
                    .iter()
                    .zip(&candidates)
                    .all(|(links, candidates)| links.len() == candidates.len().min(1))
            ),
            // Only the heuristic limits the links
            1000 => assert!(
                links
                    .iter()
                    .zip(&candidates)
                    .any(|(links, candidates)| links.len() > LEVEL_M
                        && links.len() < candidates.len())
            ),
            _ => {}
        }
    }

    /// Points without candidates, or with only deleted ones, have no links, and deleted points,
    /// or points with deleted vectors, are never linked.
    #[test]
    fn deleted_candidates_and_points_without_candidates() {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Data::Random, Distance::Dot, None);
        let candidates = fixture.candidates(1);

        // The candidates of every 10th point after the one without any, and some others
        let mut rng = StdRng::seed_from_u64(3);
        let mut deleted = HashSet::new();
        for (i, candidates) in candidates.iter().enumerate() {
            for candidate in candidates {
                if i % 10 == 1 || rng.random_bool(0.05) {
                    deleted.insert(candidate.idx);
                }
            }
        }
        assert!(deleted.len() > 20);
        // Points of the level aren't deleted, but test it anyway: half as points, half as
        // vectors
        for (i, &point) in deleted.iter().enumerate() {
            if i % 2 == 0 {
                fixture.deleted.set(point as usize, true);
            } else {
                fixture.storage.delete_vector(point).unwrap();
            }
        }

        let links = fixture.pruned(&candidates, LEVEL_M);
        let expected = fixture.expected(&candidates, LEVEL_M, |p| fixture.scorer(p));
        assert_same_links(&fixture.points, &links, &expected);
        for (i, links) in links.iter().enumerate() {
            assert!(links.iter().all(|link| !deleted.contains(link)));
            if i % 10 <= 1 {
                assert!(links.is_empty(), "{}: {links:?}", fixture.points[i]);
            } else {
                assert!(!links.is_empty(), "{}", fixture.points[i]);
            }
        }
    }

    /// Links come in the order of the points: each point's only candidate is the next point.
    #[test]
    fn links_in_the_order_of_points() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Cosine, None);
        let points = &fixture.points;
        let scorer = fixture.scorer(0);
        let next = |i: usize| points[(i + 1) % points.len()];
        let candidates: Vec<_> = (0..points.len())
            .map(|i| {
                vec![ScoredPointOffset {
                    idx: next(i),
                    score: scorer.score_internal(points[i], next(i)),
                }]
            })
            .collect();
        let links = fixture.pruned(&candidates, LEVEL_M);
        for (i, links) in links.iter().enumerate() {
            assert_eq!(links, &[next(i)], "point {}", points[i]);
        }
    }

    /// The same links from any number of threads, in pools that never entered an ambient scope.
    #[test]
    fn same_links_on_any_number_of_threads() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Euclid, Some(scalar()));
        let candidates = fixture.candidates(4);
        let reservoirs = fixture.reservoirs(&candidates);
        let expected = fixture.expected(&candidates, LEVEL_M, |p| fixture.scorer(p));
        // Several jobs
        assert!(fixture.points.len() > 4 * POINTS_PER_JOB);
        for threads in [1, 2, 7] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let links = pool
                .install(|| fixture.prune(&reservoirs, LEVEL_M, &AtomicBool::new(false)))
                .unwrap();
            assert_same_links(&fixture.points, &links, &expected);
        }
    }

    #[rstest]
    fn stopped(#[values(0, LEVEL_M)] level_m: usize) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Dot, None);
        let reservoirs = fixture.reservoirs(&fixture.candidates(1));
        let result = fixture.prune(&reservoirs, level_m, &AtomicBool::new(true));
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
    }

    #[test]
    fn no_points() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Dot, None);
        let reservoirs = Reservoirs::new(&[], CAPACITY);
        let counter = AtomicU64::new(0);
        let links = prune(
            &reservoirs,
            &[],
            LEVEL_M,
            &fixture.storage,
            None,
            &fixture.deleted,
            &AtomicBool::new(false),
            Progress::new(&counter, PROGRESS),
        )
        .unwrap();
        assert!(links.is_empty());
        assert_eq!(counter.into_inner(), PROGRESS);
    }

    #[test]
    fn one_point() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Dot, None);
        let reservoirs = Reservoirs::new(&[7], CAPACITY);
        let counter = AtomicU64::new(0);
        let links = prune(
            &reservoirs,
            &[7],
            LEVEL_M,
            &fixture.storage,
            None,
            &fixture.deleted,
            &AtomicBool::new(false),
            Progress::new(&counter, PROGRESS),
        )
        .unwrap();
        assert_eq!(links, vec![Vec::<PointOffsetType>::new()]);
        assert_eq!(counter.into_inner(), PROGRESS);
    }
}
