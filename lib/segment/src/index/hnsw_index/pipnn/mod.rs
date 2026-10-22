//! PiPNN graph construction (Pick-in-Partitions Nearest Neighbors, arXiv:2602.21247).
//!
//! Builds the links of a graph without searching it, unlike HNSW insertion:
//!
//! 1. Partitioning ([`partition`]): randomized ball carving splits the points into overlapping
//!    leaves of bounded size. Every point joins the clusters of its nearest leaders, several per
//!    level (fanout), recursively until the leaves are small enough.
//! 2. Leaf k-NN ([`leaf`]): inside each leaf, one matrix call scores all pairs, and each point's
//!    `k` nearest neighbors become candidate edges, in both directions.
//! 3. HashPrune ([`hash_prune`]): every point keeps a bounded reservoir of candidates, at most
//!    one per hash bucket of the residual direction ([`sketches`]). The result doesn't depend on
//!    the order candidates arrive in.
//! 4. Final prune ([`prune`]): the HNSW heuristic cuts each reservoir down to the degree limit.
//! 5. Fallback links ([`link_empty_lists`]): a point left without links, such as one with a NaN
//!    in its vector, links the next point of its level that has links.
//!
//! [`build_links`] builds each HNSW level this way, independently, from the levels the caller
//! assigned. The paper builds one flat graph; the levels are our adaptation.
//!
//! This module only computes links. It knows nothing about `HNSWIndex`, the graph builders, or
//! payload blocks: it takes the points, their levels, the degree limits, a matrix scorer, and the
//! vector storage with its quantized vectors and deleted points, from which the sketches are
//! projected and the final prune scores. It returns the links of every point on every level.
//! Writing them into the graph is up to the caller.
//!
//! # Levels
//!
//! - The sketches are projected once, over all points, and serve every level: the upper levels
//!   hold subsets of the points of level 0.
//! - Every random part draws from its own seed, derived from [`PipnnParams::seed`]: the
//!   sketches, and the partitioning of each level.
//! - A level that fits into one leaf takes more neighbors per point, see [`PipnnParams::leaf_k`].
//! - A level with fewer than 2 points, or a degree limit of 0, has no links and is skipped.
//!
//! # Invariants
//!
//! The graph's readers expect the links HNSW builds. On each level `L`, a point's list holds at
//! most `level_m(L)` points of level `L` or above, never the point itself, a point twice, or a
//! deleted point, and at least one point if the level has others. Without a link on level 0, a
//! point ends every search that enters the graph at it, and every point can become an entry
//! point. Debug builds check all of them on the result, see [`broken_invariant`].
//!
//! # Progress
//!
//! One counter for the whole build, with its total fixed before the first level. Every phase of
//! each level that gets links adds its share as its work gets done, by the phase's share of the
//! time, see [`progress`].
//!
//! # Memory
//!
//! The levels are built one after another, from level 0 up, and each one frees its leaves before
//! its final prune and its reservoirs before the next level starts. Within a level, the
//! reservoirs are only allocated once partitioning has returned and freed its own buffers.
//!
//! Level 0 holds every point and reaches the peak, and the order of the levels hardly matters
//! for it: each level above has about `1/m` of the points of the one below. From the bottom up,
//! nothing else is held at the peak; top down, the upper levels' links would be. Per point, at
//! level 0 with `m = 16` (32 links) and the default parameters:
//!
//! | Buffer | Bytes per point | When |
//! |---|---|---|
//! | Reservoirs of 64 slots | 652 | Leaf k-NN and final prune |
//! | Leaves, about 30 per point | 120 | Leaf k-NN |
//! | Partitioning, fanout 10 | about 100 | Partitioning, before the reservoirs |
//! | Result, before the links | about 50 | Throughout |
//! | Sketches | 48 | Throughout |
//! | Links | 4 per link | From the final prune on |
//!
//! That's a peak of about 0.9 GB at 1M points, during the leaf k-NN of level 0, on top of the
//! vectors. A reservoir factor of 4 doubles the reservoirs, to about 1.5 GB.

// TODO: remove once the graph build calls into this module.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "Not connected to the graph build yet")
)]

mod hash_prune;
mod leaf;
mod partition;
mod progress;
mod prune;
mod sketches;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::bitvec::BitSlice;
use common::condition_checker::ConditionChecker as _;
use common::progress_tracker::ProgressTracker;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rayon::prelude::*;

use self::hash_prune::Reservoirs;
pub(super) use self::partition::PartitionParams;
use self::progress::LevelProgress;
use self::sketches::Sketches;
use super::HnswM;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::vector_storage::matrix_scorer::MatrixScorer;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{NotDeletedChecker, VectorStorageEnum, VectorStorageRead as _};

/// Parameters of a PiPNN build.
#[derive(Clone, Debug)]
pub(super) struct PipnnParams {
    /// Randomized ball carving of each level into leaves.
    pub partition: PartitionParams,
    /// Nearest neighbors per point in a leaf, at least 1.
    ///
    /// A level that fits into one leaf takes as many as a reservoir holds instead, at most all
    /// other points of the level. Otherwise each of its points would get its own `leaf_k` picks
    /// and the points that picked it, about `2 · leaf_k` candidates, where a point of a larger
    /// level gets that from each of its many leaves. The leaf is scored in one matrix call either
    /// way, so the larger `k` costs next to nothing.
    pub leaf_k: usize,
    /// Reservoir capacity per point, as a multiple of the level's degree limit, at least 1.
    pub reservoir_factor: usize,
    /// Seed of every random choice of the build, drawn by the caller from the build's rng after
    /// the levels. There's no default, see [`PipnnParams::new`].
    pub seed: u64,
}

impl PipnnParams {
    /// Leaf k 4, twice the degree limit per reservoir, the default partitioning, and `seed`.
    ///
    /// Not `Default`: a fixed seed would build the same graph on every build of the same points,
    /// where HNSW draws a new one. So whoever builds the parameters has to pass a seed.
    pub fn new(seed: u64) -> Self {
        Self {
            partition: PartitionParams::default(),
            leaf_k: 4,
            reservoir_factor: 2,
            seed,
        }
    }

    /// Fails on parameters that leave the points without candidates: leaves without room for a
    /// pair, no neighbors per leaf, or no reservoir slots. Partitioning copes with its other
    /// parameters out of range.
    fn validate(&self) -> OperationResult<()> {
        let problem = if self.partition.max_leaf_size < 2 {
            "leaves must have room for at least 2 points"
        } else if self.leaf_k == 0 {
            "leaf k must be at least 1"
        } else if self.reservoir_factor == 0 {
            "the reservoir factor must be at least 1"
        } else {
            return Ok(());
        };
        Err(OperationError::service_error(format!(
            "Invalid PiPNN parameters: {problem}",
        )))
    }
}

/// Links of every point on every level up to its own: `links[i][level]` for `points[i]`, for
/// each `level` in `0..=point_level(points[i])`, at most `hnsw_m.level_m(level)` other points
/// of that level each, and at least one where the level has others and the limit isn't 0. A
/// point alone on a level has no links there.
///
/// - `points` must be sorted, distinct, and not deleted; `point_level` gives each one's level.
///   `point_deleted` must have a flag for each of them: as for HNSW, a missing flag counts as
///   deleted. HNSW takes both for granted, its points come from the id tracker in order; debug
///   builds check them here, once for all parts.
/// - `scorer` scores the vectors HNSW would, the quantized ones where configured, and so does
///   the final prune through `vector_storage` and `quantized_vectors`. The sketches project the
///   original vectors of `vector_storage`.
/// - `None` if those vectors can't be sketched, because they aren't dense, so the caller builds
///   with HNSW.
/// - Runs on the current rayon pool. The links only depend on the input and `params`, not on the
///   number of threads.
/// - Starts `progress`, and tracks it once the sketches are done, so the caller doesn't. The
///   total is fixed then, [`LevelProgress::units`] for each level that gets links, and the phases
///   add their shares as their work gets done: the count rises with the time spent and ends at
///   the total. On `None`, `progress` isn't tracked yet, so the caller can build with HNSW under
///   it.
/// - Fails if stopped, on invalid parameters, on points past the stored vectors, and on failed
///   reads.
///
/// The caller hands each point's links to `GraphLayersBuilder::add_new_point`, exactly once per
/// point. The links can move there in parallel, but the graph's entry points keep the first
/// point they see of each top level, so the graph is only reproducible if the points are added
/// in a fixed order, such as the order of `points`.
#[expect(clippy::too_many_arguments, reason = "the inputs of a graph build")]
pub(super) fn build_links(
    vector_storage: &VectorStorageEnum,
    quantized_vectors: Option<&QuantizedVectors>,
    point_deleted: &BitSlice,
    scorer: &MatrixScorer<'_>,
    points: &[PointOffsetType],
    point_level: impl Fn(PointOffsetType) -> usize + Sync,
    hnsw_m: HnswM,
    params: &PipnnParams,
    stopped: &AtomicBool,
    progress: &ProgressTracker,
) -> OperationResult<Option<Vec<Vec<Vec<PointOffsetType>>>>> {
    debug_assert!(
        points.is_sorted_by(|a, b| a < b),
        "PiPNN points must be sorted and distinct",
    );
    debug_assert!(
        points
            .last()
            .is_none_or(|&last| (last as usize) < point_deleted.len()),
        "PiPNN points need a deleted flag each, a missing one counts as deleted",
    );
    params.validate()?;
    check_process_stopped(stopped)?;
    progress.start();
    let timer = Instant::now();

    // One stream of seeds: the sketches' first, then one per level in level order, so the
    // sketches and every level draw from different seeds, and a level's seed doesn't depend on
    // how many levels there are
    let mut seeds = StdRng::seed_from_u64(params.seed);
    let Some(sketches) = Sketches::new(vector_storage, points, seeds.random(), stopped)? else {
        return Ok(None);
    };
    log::debug!(
        "PiPNN sketches of {} points in {:.2?}",
        points.len(),
        timer.elapsed(),
    );

    // The result, with an empty list for each level of each point, which also tells the levels
    let mut links: Vec<Vec<Vec<PointOffsetType>>> = points
        .par_iter()
        .map(|&point| vec![Vec::new(); point_level(point) + 1])
        .collect();
    let levels = links.iter().map(Vec::len).max().unwrap_or(0);
    let level_seeds: Vec<u64> = (0..levels).map(|_| seeds.random()).collect();

    // The points of each level, which fix the total of the progress
    let mut level_lens = vec![0; levels];
    for lists in &links {
        level_lens[..lists.len()]
            .iter_mut()
            .for_each(|len| *len += 1);
    }
    // Without pairs or a degree limit, a level has no links
    let has_links = |level: usize| level_lens[level] >= 2 && hnsw_m.level_m(level) > 0;
    let total = (0..levels)
        .filter(|&level| has_links(level))
        .map(|level| LevelProgress::units(level_lens[level]))
        .sum();
    let counter = progress.track_progress(Some(total));

    let build = Build {
        scorer,
        sketches: &sketches,
        vector_storage,
        quantized_vectors,
        point_deleted,
        params,
        stopped,
    };
    for (level, seed) in level_seeds.into_iter().enumerate() {
        check_process_stopped(stopped)?;
        let level_points: Vec<_> = std::iter::zip(points, &links)
            .filter(|(_, lists)| lists.len() > level)
            .map(|(&point, _)| point)
            .collect();
        debug_assert_eq!(level_points.len(), level_lens[level]);
        let level_m = hnsw_m.level_m(level);
        if has_links(level) {
            let progress = LevelProgress::new(&counter, level_points.len());
            let level_links = build.level(level, &level_points, level_m, seed, progress)?;
            debug_assert_eq!(level_links.len(), level_points.len());
            let on_level = links.iter_mut().filter(|lists| lists.len() > level);
            for (lists, level_links) in on_level.zip(level_links) {
                lists[level] = level_links;
            }
        } else {
            log::debug!(
                "PiPNN level {level}: {} points, level m {level_m}, no links",
                level_points.len(),
            );
        }
    }

    log::debug!(
        "PiPNN links of {} points on {levels} levels in {:.2?}",
        points.len(),
        timer.elapsed(),
    );
    debug_assert_eq!(
        counter.load(Ordering::Relaxed),
        total,
        "PiPNN progress must end at its total",
    );
    debug_assert_eq!(
        broken_invariant(
            &links,
            points,
            &point_level,
            hnsw_m,
            &vector_storage.not_deleted_checker(point_deleted),
        ),
        None,
        "PiPNN links break an invariant of the graph",
    );
    Ok(Some(links))
}

/// Gives each of `points` whose list in `links` came out empty one link: the next point after
/// it, in the order of `points` and wrapping around, that has links of its own and isn't
/// deleted. If no point has links, as when every vector of the level has a NaN, the next point
/// that isn't deleted, so that they form a ring. Returns how many points it linked.
///
/// `points` are the points of one level, sorted, and `links` their lists after the final prune.
/// A list comes out empty when none of the point's candidates can become a link, or it has none:
/// - A vector with a NaN scores NaN against every other point, which the leaf k-NN never picks,
///   wherever partitioning puts it.
/// - A point can be alone in each of its leaves, though only with odd parameters, such as leaves
///   of at most 2 points.
/// - The final prune skips candidates that are deleted, which `points` shouldn't hold, and those
///   that `score_internal` scores NaN where the matrix kernels didn't.
///
/// A point with links leads into the graph, which is all a search that starts at the point
/// needs. No link is better for a NaN vector, and the other cases are rare, so the order of ids
/// is enough: one pass over the level, the same for any number of threads. The levels above 0
/// get the same: there, the greedy descent of a search can't move on from a point without links,
/// and HNSW leaves no point without links on a level with others either.
fn link_empty_lists(
    links: &mut [Vec<PointOffsetType>],
    points: &[PointOffsetType],
    not_deleted: &NotDeletedChecker<'_>,
) -> usize {
    debug_assert_eq!(links.len(), points.len());
    debug_assert!(
        points.is_sorted_by(|a, b| a < b),
        "points must be sorted and distinct",
    );
    if links.iter().all(|links| !links.is_empty()) {
        return 0;
    }
    let live = |&point: &PointOffsetType| not_deleted.check_infallible(point);
    let mut targets: Vec<PointOffsetType> = std::iter::zip(points, &*links)
        .filter(|(_, links)| !links.is_empty())
        .map(|(&point, _)| point)
        .filter(live)
        .collect();
    if targets.is_empty() {
        targets = points.iter().copied().filter(live).collect();
    }

    let mut linked = 0;
    for (&point, links) in std::iter::zip(points, links) {
        if !links.is_empty() {
            continue;
        }
        // In a ring, the point itself is a target, and comes last
        let after = targets.partition_point(|&target| target <= point);
        let next = targets[after..]
            .iter()
            .chain(&targets[..after])
            .find(|&&target| target != point);
        if let Some(&next) = next {
            links.push(next);
            linked += 1;
        }
    }
    linked
}

/// The first point whose links break an invariant of the graph, and how, for a debug assertion
/// on the result of [`build_links`]. `links` must hold the lists of `points` on the levels up to
/// `point_level`, and each list on level `L`:
/// 1. Only points of `points` on level `L` or above. The links file finds the list of a point on
///    an upper level by its position among the points of that level, so a link to a point below
///    would read another point's list.
/// 2. At most `level_m(L)` points, all of them main links.
/// 3. Neither the point itself, a point twice, nor a deleted point.
/// 4. At least one point, if `level_m(L)` isn't 0 and the level has another point that isn't
///    deleted, see [`link_empty_lists`].
fn broken_invariant(
    links: &[Vec<Vec<PointOffsetType>>],
    points: &[PointOffsetType],
    point_level: impl Fn(PointOffsetType) -> usize + Sync,
    hnsw_m: HnswM,
    not_deleted: &NotDeletedChecker<'_>,
) -> Option<String> {
    if links.len() != points.len() {
        return Some(format!("{} lists for {} points", links.len(), points.len()));
    }
    // Points of each level that aren't deleted
    let mut live = Vec::new();
    for (&point, lists) in std::iter::zip(points, links) {
        if live.len() < lists.len() {
            live.resize(lists.len(), 0);
        }
        if not_deleted.check_infallible(point) {
            live[..lists.len()].iter_mut().for_each(|count| *count += 1);
        }
    }

    let broken = |point: PointOffsetType, lists: &[Vec<PointOffsetType>]| {
        let own = point_level(point);
        if lists.len() != own + 1 {
            let count = lists.len();
            return Some(format!("point {point} of level {own} has {count} lists"));
        }
        // Counted in `live` itself, unless deleted
        let counted = usize::from(not_deleted.check_infallible(point));
        for (level, list) in lists.iter().enumerate() {
            let level_m = hnsw_m.level_m(level);
            if list.len() > level_m {
                let count = list.len();
                return Some(format!(
                    "point {point} has {count} links on level {level}, more than {level_m}",
                ));
            }
            if list.is_empty() && level_m > 0 && live[level] > counted {
                return Some(format!("point {point} has no links on level {level}"));
            }
            for (i, &link) in list.iter().enumerate() {
                let problem = if link == point {
                    "is the point itself"
                } else if list[..i].contains(&link) {
                    "comes twice"
                } else if !not_deleted.check_infallible(link) {
                    "is deleted"
                } else {
                    match points.binary_search(&link) {
                        Ok(position) if links[position].len() > level => continue,
                        Ok(_) => "is on a lower level",
                        Err(_) => "isn't one of the points",
                    }
                };
                return Some(format!(
                    "link {link} of point {point} on level {level} {problem}",
                ));
            }
        }
        None
    };
    points
        .par_iter()
        .zip(links)
        .find_map_first(|(&point, lists)| broken(point, lists))
}

/// What all levels of one [`build_links`] call share.
struct Build<'a, 's> {
    scorer: &'a MatrixScorer<'s>,
    sketches: &'a Sketches,
    vector_storage: &'a VectorStorageEnum,
    quantized_vectors: Option<&'a QuantizedVectors>,
    point_deleted: &'a BitSlice,
    params: &'a PipnnParams,
    stopped: &'a AtomicBool,
}

/// What partitioning and the leaf k-NN of a level did, for the debug log and the tests.
#[derive(Default)]
struct LevelStats {
    leaves: usize,
    /// Leaf sizes summed up: the number of leaves each point is in, summed over the points.
    memberships: usize,
    /// Neighbors per point in a leaf.
    leaf_k: usize,
    partition_time: Duration,
    /// Including the allocation of the reservoirs.
    leaf_knn_time: Duration,
}

impl Build<'_, '_> {
    /// Links of `points`, the points of `level` (sorted, distinct, at least 2), in their order,
    /// at least one and at most `level_m` each, which must be at least 1. `seed` is the level's
    /// partitioning seed. Adds all of `progress`, and logs the level once it's done.
    fn level(
        &self,
        level: usize,
        points: &[PointOffsetType],
        level_m: usize,
        seed: u64,
        progress: LevelProgress<'_>,
    ) -> OperationResult<Vec<Vec<PointOffsetType>>> {
        let capacity = level_m.saturating_mul(self.params.reservoir_factor);
        let mut stats = LevelStats::default();
        let reservoirs = self.candidates(points, capacity, seed, progress, &mut stats)?;

        let timer = Instant::now();
        let mut links = prune::prune(
            &reservoirs,
            points,
            level_m,
            self.vector_storage,
            self.quantized_vectors,
            self.point_deleted,
            self.stopped,
            progress.prune,
        )?;
        drop(reservoirs);
        let not_deleted = self.vector_storage.not_deleted_checker(self.point_deleted);
        let fallbacks = link_empty_lists(&mut links, points, &not_deleted);
        let prune_time = timer.elapsed();

        let len = points.len() as f64;
        let linked: usize = links.iter().map(Vec::len).sum();
        log::debug!(
            "PiPNN level {level}: {} points, {} leaves of {:.1} points, each point in {:.1}, leaf \
             k {}, {:.1} links per point, {fallbacks} fallback links; partitioning {:.2?}, leaf \
             k-NN {:.2?}, final prune {prune_time:.2?}",
            points.len(),
            stats.leaves,
            stats.memberships as f64 / stats.leaves as f64,
            stats.memberships as f64 / len,
            stats.leaf_k,
            linked as f64 / len,
            stats.partition_time,
            stats.leaf_knn_time,
        );
        Ok(links)
    }

    /// The reservoirs of `points`, the points of a level (sorted, distinct, at least 2),
    /// `capacity` each, filled with the bidirected k-NN edges of their leaves. Adds the shares
    /// of `progress` for partitioning and the leaf k-NN, and records both in `stats`.
    fn candidates(
        &self,
        points: &[PointOffsetType],
        capacity: usize,
        seed: u64,
        progress: LevelProgress<'_>,
        stats: &mut LevelStats,
    ) -> OperationResult<Reservoirs> {
        let timer = Instant::now();
        let leaves = partition::partition(
            self.scorer,
            points.to_vec(),
            &self.params.partition,
            seed,
            self.stopped,
            progress.partition,
        )?;
        stats.partition_time = timer.elapsed();
        stats.leaves = leaves.len();
        stats.memberships = leaves.iter().map(Vec::len).sum();

        let timer = Instant::now();
        // Only now, when partitioning has freed its buffers
        let reservoirs = Reservoirs::new(points, capacity);
        // See `PipnnParams::leaf_k`
        let leaf_k = match leaves.as_slice() {
            [leaf] => self.params.leaf_k.max(capacity).min(leaf.len() - 1),
            _ => self.params.leaf_k,
        };
        stats.leaf_k = leaf_k;
        let sketches = self.sketches;
        leaf::for_each_leaf_edge(
            self.scorer,
            &leaves,
            leaf_k,
            self.stopped,
            progress.leaf_knn,
            |point, candidate, score| {
                reservoirs.insert(point, candidate, sketches.hash(point, candidate), score);
            },
        )?;
        drop(leaves);
        stats.leaf_knn_time = timer.elapsed();
        Ok(reservoirs)
    }
}

#[cfg(test)]
mod orchestration_tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};

    use common::ambient;
    use common::bitvec::BitVec;
    use common::progress_tracker::{ProgressView, new_progress_tracker};
    use rstest::rstest;
    use tempfile::TempDir;

    use super::progress::{LEAF_KNN_UNITS, PARTITION_UNITS};
    use super::*;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::types::{
        BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
        MultiVectorConfig, QuantizationConfig, ScalarQuantizationConfig,
    };
    use crate::vector_storage::VectorStorage as _;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::matrix_scorer::new_matrix_scorer;
    use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
    use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;
    use crate::vector_storage::sparse::volatile_sparse_vector_storage::new_volatile_sparse_vector_storage;

    /// Stored vectors, of which every tenth is deleted.
    const POINTS: usize = 2000;
    const DIM: usize = 16;
    /// Small leaves, so that level 0 takes two steps of carving and the upper levels range from
    /// several leaves to a single one.
    const LEAF: usize = 64;
    /// Links above level 0; twice as many on it.
    const M: usize = 8;

    type Links = Vec<Vec<Vec<PointOffsetType>>>;

    fn hnsw_m() -> HnswM {
        HnswM::new2(M)
    }

    /// Partitioning with leaders in the same ratio to the leaf size as the defaults.
    fn params() -> PipnnParams {
        PipnnParams {
            partition: PartitionParams {
                max_leaf_size: LEAF,
                min_leaf_size: LEAF / 4,
                leader_fraction: 10.0 / LEAF as f64,
                max_leaders: 100,
                fanout: vec![10, 3],
                max_depth: 16,
            },
            ..PipnnParams::new(1)
        }
    }

    struct Fixture {
        storage: VectorStorageEnum,
        quantized: Option<QuantizedVectors>,
        deleted: BitVec,
        /// The points that aren't deleted.
        points: Vec<PointOffsetType>,
        _dir: TempDir,
    }

    impl Fixture {
        fn new(distance: Distance, quantization: Option<QuantizationConfig>) -> Self {
            let mut rng = StdRng::seed_from_u64(42);
            let mut storage = new_volatile_dense_vector_storage(DIM, distance);
            for id in 0..POINTS as PointOffsetType {
                let vector: Vec<VectorElementType> =
                    (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect();
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
            let deleted: BitVec = (0..POINTS).map(|id| id % 10 == 9).collect();
            let points = (0..POINTS as PointOffsetType)
                .filter(|&id| !deleted[id as usize])
                .collect();
            Self {
                storage,
                quantized,
                deleted,
                points,
                _dir: dir,
            }
        }

        fn scorer(&self) -> MatrixScorer<'_> {
            new_matrix_scorer(&self.storage, self.quantized.as_ref()).unwrap()
        }

        /// Makes the vectors of `points` NaN in every dimension. Only without quantization,
        /// whose vectors would keep the old values.
        fn set_nan(&mut self, points: impl IntoIterator<Item = PointOffsetType>) {
            assert!(self.quantized.is_none(), "quantized vectors aren't updated");
            let nan = vec![VectorElementType::NAN; DIM];
            for point in points {
                self.storage
                    .insert_vector(point, VectorRef::from(&nan))
                    .unwrap();
            }
        }

        /// [`build_links`] with `levels` by id.
        fn build_links(
            &self,
            points: &[PointOffsetType],
            levels: &[usize],
            hnsw_m: HnswM,
            params: &PipnnParams,
            stopped: &AtomicBool,
            progress: &ProgressTracker,
        ) -> OperationResult<Option<Links>> {
            build_links(
                &self.storage,
                self.quantized.as_ref(),
                &self.deleted,
                &self.scorer(),
                points,
                |point| levels[point as usize],
                hnsw_m,
                params,
                stopped,
                progress,
            )
        }

        /// [`Self::build_links`], checking the progress: a build with links ends at
        /// [`progress_total`], one that can't sketch leaves it untracked, and one that fails
        /// stays at most at its total.
        fn build_with(
            &self,
            points: &[PointOffsetType],
            levels: &[usize],
            hnsw_m: HnswM,
            params: &PipnnParams,
            stopped: &AtomicBool,
        ) -> OperationResult<Option<Links>> {
            let (view, progress) = new_progress_tracker();
            let links = self.build_links(points, levels, hnsw_m, params, stopped, &progress);
            let (done, total) = done_and_total(&view);
            match &links {
                Ok(Some(_)) => {
                    let want = Some(progress_total(points, levels, hnsw_m));
                    assert_eq!((done, total), (want, want), "progress");
                }
                Ok(None) => assert_eq!((done, total), (None, None), "progress"),
                Err(_) => assert!(done <= total, "progress {done:?} of {total:?}"),
            }
            links
        }

        fn build(&self, points: &[PointOffsetType], levels: &[usize]) -> Links {
            self.build_with(points, levels, hnsw_m(), &params(), &AtomicBool::new(false))
                .unwrap()
                .expect("dense vectors")
        }

        /// The reservoirs of a level of `points`, as [`build_links`] fills them, and the
        /// sketches that hashed their candidates.
        fn candidates(
            &self,
            points: &[PointOffsetType],
            capacity: usize,
            progress: LevelProgress<'_>,
            stats: &mut LevelStats,
        ) -> (Reservoirs, Sketches) {
            let stopped = AtomicBool::new(false);
            let sketches = Sketches::new(&self.storage, points, 3, &stopped)
                .unwrap()
                .unwrap();
            let scorer = self.scorer();
            let params = params();
            let build = Build {
                scorer: &scorer,
                sketches: &sketches,
                vector_storage: &self.storage,
                quantized_vectors: self.quantized.as_ref(),
                point_deleted: &self.deleted,
                params: &params,
                stopped: &stopped,
            };
            let reservoirs = build
                .candidates(points, capacity, 4, progress, stats)
                .unwrap();
            (reservoirs, sketches)
        }
    }

    /// Progress units of a build of `points` with `levels` by id: [`LevelProgress::units`] for
    /// each level with at least 2 points and a degree limit.
    fn progress_total(points: &[PointOffsetType], levels: &[usize], hnsw_m: HnswM) -> u64 {
        let top = points.iter().map(|&point| levels[point as usize]).max();
        let level_units = |level| {
            let len = points
                .iter()
                .filter(|&&point| levels[point as usize] >= level)
                .count();
            if len >= 2 && hnsw_m.level_m(level) > 0 {
                LevelProgress::units(len)
            } else {
                0
            }
        };
        top.map_or(0, |top| (0..=top).map(level_units).sum())
    }

    /// Done and total of the progress that [`build_links`] tracked, `None` if it didn't.
    fn done_and_total(view: &ProgressView) -> (Option<u64>, Option<u64>) {
        let tree = view.snapshot("");
        (tree.done, tree.total)
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
    fn binary() -> QuantizationConfig {
        QuantizationConfig::Binary(BinaryQuantization {
            binary: BinaryQuantizationConfig {
                always_ram: None,
                memory: None,
                encoding: Some(BinaryQuantizationEncoding::OneBit),
                query_encoding: None,
            },
        })
    }

    /// Levels of all stored points as HNSW draws them: about one in `M` points of each level is
    /// also on the next.
    fn random_levels(seed: u64) -> Vec<usize> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..POINTS)
            .map(|_| {
                let mut level = 0;
                while rng.random_range(0..M) == 0 {
                    level += 1;
                }
                level
            })
            .collect()
    }

    /// 1800 points on level 0, 400 on level 1, 40 on level 2, 5 on level 3, and point 10 alone on
    /// level 4. Levels 2 and 3 fit into one leaf.
    fn few_points_on_top() -> Vec<usize> {
        (0..POINTS)
            .map(|id| match id {
                10 => 4,
                _ if id % 400 == 10 => 3,
                _ if id % 50 == 10 => 2,
                _ if id % 5 == 0 => 1,
                _ => 0,
            })
            .collect()
    }

    /// The fallback link of `point` on `level`: the next point of the level after it, in id
    /// order and wrapping around, that isn't `deleted` and has links of its own by `has_links`,
    /// or that isn't deleted if no point of the level has links. `None` if there is none.
    fn fallback(
        points: &[PointOffsetType],
        levels: &[usize],
        level: usize,
        point: PointOffsetType,
        deleted: impl Fn(PointOffsetType) -> bool,
        has_links: impl Fn(PointOffsetType) -> bool,
    ) -> Option<PointOffsetType> {
        let on_level: Vec<_> = points
            .iter()
            .copied()
            .filter(|&p| levels[p as usize] >= level && !deleted(p))
            .collect();
        let any_links = on_level.iter().any(|&p| has_links(p));
        let after = on_level.partition_point(|&p| p <= point);
        on_level[after..]
            .iter()
            .chain(&on_level[..after])
            .copied()
            .find(|&p| p != point && (!any_links || has_links(p)))
    }

    /// The guarantees of [`build_links`] for `points` with `levels` by id. Returns the degrees
    /// of the points of each level.
    #[track_caller]
    fn check_links(
        links: &Links,
        points: &[PointOffsetType],
        levels: &[usize],
        hnsw_m: HnswM,
    ) -> Vec<Vec<usize>> {
        assert_eq!(links.len(), points.len(), "one entry per point");
        let given: HashSet<_> = points.iter().copied().collect();
        let mut degrees: Vec<Vec<usize>> = Vec::new();
        for (&point, lists) in points.iter().zip(links) {
            let level = levels[point as usize];
            assert_eq!(
                lists.len(),
                level + 1,
                "lists of point {point}, on level {level}"
            );
            for (l, list) in lists.iter().enumerate() {
                assert!(
                    list.len() <= hnsw_m.level_m(l),
                    "point {point} has {} links on level {l}",
                    list.len(),
                );
                let distinct: HashSet<_> = list.iter().collect();
                assert_eq!(distinct.len(), list.len(), "point {point} repeats a link");
                for &link in list {
                    assert_ne!(link, point, "point {point} links itself on level {l}");
                    assert!(
                        given.contains(&link),
                        "point {point} links {link}, not given"
                    );
                    assert!(
                        levels[link as usize] >= l,
                        "point {point} links {link} on level {l}, above its level",
                    );
                }
                if degrees.len() == l {
                    degrees.push(Vec::new());
                }
                degrees[l].push(list.len());
            }
        }
        // A point that has others on its level links some of them
        for (level, degrees) in degrees.iter().enumerate() {
            if degrees.len() >= 2 && hnsw_m.level_m(level) > 0 {
                assert!(
                    degrees.iter().all(|&d| d > 0),
                    "a point without links on level {level}",
                );
            }
        }
        degrees
    }

    fn mean(degrees: &[usize]) -> f64 {
        degrees.iter().sum::<usize>() as f64 / degrees.len() as f64
    }

    /// Partitioned and single-leaf levels, with ids missing for deleted points.
    #[rstest]
    fn links_keep_the_contract(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(distance, None);
        let levels = random_levels(3);
        let links = fixture.build(&fixture.points, &levels);
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        assert!(degrees.len() >= 3, "{} levels", degrees.len());
        assert!(
            mean(&degrees[0]) >= 2.0,
            "level 0 degree {}",
            mean(&degrees[0])
        );
    }

    #[test]
    fn empty_input() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        assert!(fixture.build(&[], &random_levels(3)).is_empty());
    }

    #[rstest]
    fn one_point(#[values(0, 3)] level: usize) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let mut levels = vec![0; POINTS];
        levels[5] = level;
        let want: Links = vec![vec![Vec::new(); level + 1]];
        assert_eq!(fixture.build(&[5], &levels), want);
    }

    /// Each point's only candidate, on every level they share.
    #[test]
    fn two_points() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Dot, None);
        let mut levels = vec![0; POINTS];
        levels[250] = 2;
        let want: Links = vec![vec![vec![250]], vec![vec![3], vec![], vec![]]];
        assert_eq!(fixture.build(&[3, 250], &levels), want);
    }

    #[test]
    fn all_points_on_level_0() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Cosine, None);
        let levels = vec![0; POINTS];
        let links = fixture.build(&fixture.points, &levels);
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        assert_eq!(degrees.len(), 1);
        assert!(mean(&degrees[0]) >= 2.0, "degree {}", mean(&degrees[0]));
    }

    /// See [`few_points_on_top`].
    #[test]
    fn several_levels_with_few_points_on_top() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = few_points_on_top();
        let links = fixture.build(&fixture.points, &levels);
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        let sizes: Vec<_> = degrees.iter().map(Vec::len).collect();
        assert_eq!(sizes, [1800, 400, 40, 5, 1]);

        assert_eq!(degrees[4], [0], "alone on the top level");
        // A single leaf, with every candidate the reservoirs hold: the HNSW heuristic keeps
        // most of them on uniform data in 16 dimensions
        let single_leaf = mean(&degrees[2]);
        assert!(
            single_leaf >= M as f64 / 2.0,
            "level 2 degree {single_leaf}"
        );
    }

    /// A level that fits into one leaf gives every point as many candidates as its reservoir
    /// holds: all other points, one per bucket, if they fit, otherwise a full reservoir. That's
    /// up to four times what `leaf_k` alone would give at a capacity of 16.
    #[rstest]
    fn single_leaf_level_takes_every_candidate(#[values(2, 5, 17, LEAF)] len: usize) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let points = &fixture.points[..len];
        let capacity = 2 * M;
        let mut stats = LevelStats::default();
        let counter = AtomicU64::new(0);
        let progress = LevelProgress::new(&counter, len);
        let (reservoirs, sketches) = fixture.candidates(points, capacity, progress, &mut stats);
        assert_eq!(stats.leaves, 1);
        assert_eq!(stats.leaf_k, capacity.min(len - 1));

        let mut total = 0;
        for &point in points {
            let held = reservoirs.candidates(point).len();
            total += held;
            if len - 1 <= capacity {
                let buckets: HashSet<_> = points
                    .iter()
                    .filter(|&&other| other != point)
                    .map(|&other| sketches.hash(point, other))
                    .collect();
                assert_eq!(held, buckets.len(), "point {point}");
            } else {
                assert!(held <= capacity, "point {point}: {held}");
            }
        }
        // Others pick a point too, which fills its buckets that collided
        if len - 1 > capacity {
            assert!(total * 100 >= len * capacity * 95, "{total} candidates");
        }
    }

    /// Larger levels keep `leaf_k`.
    #[test]
    fn partitioned_level_keeps_leaf_k() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let mut stats = LevelStats::default();
        let counter = AtomicU64::new(0);
        let progress = LevelProgress::new(&counter, LEAF + 1);
        fixture.candidates(&fixture.points[..LEAF + 1], 2 * M, progress, &mut stats);
        assert!(stats.leaves > 1);
        assert_eq!(stats.leaf_k, params().leaf_k);
    }

    /// A level of one point more than a leaf holds is cut into a few leaves that each hold most
    /// of it, so its points find about the same `leaf_k` neighbors in all of them, and get fewer
    /// links than in a single leaf. PiPNN is meant for segments well above that size, but such a
    /// level still keeps the contract, as level 0 and as an upper level.
    #[rstest]
    fn level_just_above_one_leaf(#[values(0, 1)] level: usize) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Cosine, None);
        let (points, levels) = if level == 0 {
            (&fixture.points[..LEAF + 1], vec![0; POINTS])
        } else {
            let mut levels = vec![0; POINTS];
            for &point in &fixture.points[..LEAF + 1] {
                levels[point as usize] = level;
            }
            (&fixture.points[..], levels)
        };
        let links = fixture.build(points, &levels);
        let degrees = check_links(&links, points, &levels, hnsw_m());
        assert_eq!(degrees[level].len(), LEAF + 1);
    }

    #[rstest]
    #[case::euclid_scalar(Distance::Euclid, Some(scalar()))]
    #[case::dot(Distance::Dot, None)]
    #[case::cosine(Distance::Cosine, None)]
    fn deterministic_across_thread_pools(
        #[case] distance: Distance,
        #[case] quantization: Option<QuantizationConfig>,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(distance, quantization);
        let levels = random_levels(5);
        let links = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| fixture.build(&fixture.points, &levels))
        };
        let expected = links(1);
        check_links(&expected, &fixture.points, &levels, hnsw_m());
        for threads in [1, 2, 7] {
            assert!(links(threads) == expected, "{threads} threads");
        }
    }

    #[test]
    fn seeds_change_the_links() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(5);
        let links = fixture.build(&fixture.points, &levels);
        let stopped = AtomicBool::new(false);
        let other_params = PipnnParams {
            seed: 2,
            ..params()
        };
        let other = fixture
            .build_with(&fixture.points, &levels, hnsw_m(), &other_params, &stopped)
            .unwrap()
            .unwrap();
        check_links(&other, &fixture.points, &levels, hnsw_m());
        assert!(links != other);
    }

    /// Scalar quantization, and binary quantization at a low dimension, whose scores tie a lot.
    #[rstest]
    fn quantized(
        #[values(scalar(), binary())] quantization: QuantizationConfig,
        #[values(Distance::Dot, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(distance, Some(quantization));
        let levels = random_levels(3);
        let links = fixture.build(&fixture.points, &levels);
        check_links(&links, &fixture.points, &levels, hnsw_m());
    }

    /// Sparse vectors and multivectors have no sketches, whatever the scorer.
    #[test]
    fn none_without_sketches() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Dot, None);
        let sparse = new_volatile_sparse_vector_storage();
        let multi = new_volatile_multi_dense_vector_storage(
            DIM,
            Distance::Dot,
            MultiVectorConfig::default(),
        );
        let levels = vec![0; POINTS];
        for storage in [&sparse, &multi] {
            for points in [&[][..], &[0, 1, 2]] {
                let (view, progress) = new_progress_tracker();
                let links = build_links(
                    storage,
                    None,
                    &fixture.deleted,
                    &fixture.scorer(),
                    points,
                    |point| levels[point as usize],
                    hnsw_m(),
                    &params(),
                    &AtomicBool::new(false),
                    &progress,
                );
                assert_eq!(links.unwrap(), None, "{storage:?}");
                // Left for HNSW to track
                assert_eq!(done_and_total(&view), (None, None));
            }
        }
    }

    #[test]
    fn stopped() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        for points in [&fixture.points[..], &[]] {
            let result =
                fixture.build_with(points, &levels, hnsw_m(), &params(), &AtomicBool::new(true));
            assert!(matches!(result, Err(OperationError::Cancelled { .. })));
        }
    }

    /// A stop that arrives after the sketches still ends the build.
    #[test]
    fn stopped_during_the_build() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        let stopped = AtomicBool::new(false);
        let result = build_links(
            &fixture.storage,
            None,
            &fixture.deleted,
            &fixture.scorer(),
            &fixture.points,
            |point| {
                stopped.store(true, Ordering::Relaxed);
                levels[point as usize]
            },
            hnsw_m(),
            &params(),
            &stopped,
            &ProgressTracker::new_for_test(),
        );
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
    }

    /// A contract of the caller, which HNSW takes for granted too, so only debug builds check it.
    #[rstest]
    #[case::unsorted(&[2, 1])]
    #[case::repeated(&[1, 1])]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "PiPNN points must be sorted and distinct")]
    fn unsorted_or_repeated_points_fail_in_debug_builds(#[case] points: &[PointOffsetType]) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let _ = fixture.build(points, &random_levels(3));
    }

    /// Without a flag, a point counts as deleted, so nothing links it: a short bitslice would
    /// silently give an empty graph. Only debug builds check, as for
    /// [`unsorted_or_repeated_points_fail_in_debug_builds`].
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "PiPNN points need a deleted flag each")]
    fn points_without_deleted_flags_fail_in_debug_builds() {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Distance::Euclid, None);
        fixture.deleted.truncate(POINTS / 2);
        let _ = fixture.build(&fixture.points, &random_levels(3));
    }

    #[rstest]
    #[case::leaves_of_none(PipnnParams { partition: PartitionParams { max_leaf_size: 0, ..params().partition }, ..params() })]
    #[case::leaves_of_one(PipnnParams { partition: PartitionParams { max_leaf_size: 1, ..params().partition }, ..params() })]
    #[case::no_leaf_k(PipnnParams { leaf_k: 0, ..params() })]
    #[case::no_reservoir(PipnnParams { reservoir_factor: 0, ..params() })]
    fn rejects_params_without_links(#[case] params: PipnnParams) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        let stopped = AtomicBool::new(false);
        let result = fixture.build_with(&fixture.points, &levels, hnsw_m(), &params, &stopped);
        assert!(matches!(result, Err(OperationError::ServiceError { .. })));
    }

    /// A degree limit of 0 leaves its levels without links, and the others as they'd be. The
    /// progress only counts the levels with links.
    #[rstest]
    #[case::upper_levels(HnswM::new(0, 2 * M))]
    #[case::level_0(HnswM::new(M, 0))]
    #[case::every_level(HnswM::new(0, 0))]
    fn no_links_where_m_is_zero(#[case] hnsw_m: HnswM) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        let links = fixture
            .build_with(
                &fixture.points,
                &levels,
                hnsw_m,
                &params(),
                &AtomicBool::new(false),
            )
            .unwrap()
            .unwrap();
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m);
        assert!(degrees.len() >= 3);
        let full = fixture.build(&fixture.points, &levels);
        for (level, degrees) in degrees.iter().enumerate() {
            if hnsw_m.level_m(level) == 0 {
                assert!(degrees.iter().all(|&d| d == 0), "level {level}");
            } else {
                let unchanged = full.iter().filter_map(|lists| lists.get(level));
                let with_m = links.iter().filter_map(|lists| lists.get(level));
                assert!(unchanged.eq(with_m), "level {level}");
            }
        }
    }

    /// The progress ends at its total, which counts the points of every level with links, also
    /// with a point alone on level 0, or on several levels.
    #[rstest]
    #[case::one_point(&[7])]
    #[case::two_points(&[7, 8])]
    #[case::subset(&[0, 3, 5, 11, 12, 13, 20, 40, 41, 42])]
    fn progress_ends_at_its_total(#[case] points: &[PointOffsetType]) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels: Vec<usize> = (0..POINTS).map(|id| id % 4).collect();
        // Checked in `build_with`
        let links = fixture.build(points, &levels);
        check_links(&links, points, &levels, hnsw_m());
    }

    /// While a build runs, its progress only rises, under a total that's fixed from the start,
    /// and ends at it.
    #[test]
    fn progress_rises_to_its_total() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        let (view, progress) = new_progress_tracker();
        let finished = AtomicBool::new(false);
        let samples = std::thread::scope(|scope| {
            let sampler = scope.spawn(|| {
                let mut samples = Vec::new();
                while !finished.load(Ordering::Acquire) {
                    if let (Some(done), Some(total)) = done_and_total(&view) {
                        samples.push((done, total));
                    }
                    std::thread::yield_now();
                }
                samples
            });
            let stopped = AtomicBool::new(false);
            let links = fixture.build_links(
                &fixture.points,
                &levels,
                hnsw_m(),
                &params(),
                &stopped,
                &progress,
            );
            finished.store(true, Ordering::Release);
            links.unwrap().unwrap();
            sampler.join().unwrap()
        });
        let total = progress_total(&fixture.points, &levels, hnsw_m());
        assert!(
            samples
                .iter()
                .all(|&(done, at)| done <= total && at == total)
        );
        assert!(samples.is_sorted(), "progress went down");
        assert_eq!(done_and_total(&view), (Some(total), Some(total)));
    }

    /// Partitioning and the leaf k-NN of a level add their shares before its final prune, so the
    /// progress is well past 0 before level 0 ends. The final prune adds the rest.
    #[test]
    fn progress_passes_0_before_level_0_ends() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let points = &fixture.points;
        let counter = AtomicU64::new(0);
        let progress = LevelProgress::new(&counter, points.len());
        let mut stats = LevelStats::default();
        let (reservoirs, _) = fixture.candidates(points, 2 * M, progress, &mut stats);
        assert!(stats.leaves > 1);
        let before_prune = counter.load(Ordering::Relaxed);
        let len = points.len() as u64;
        assert_eq!(before_prune, (PARTITION_UNITS + LEAF_KNN_UNITS) * len);

        prune::prune(
            &reservoirs,
            points,
            2 * M,
            &fixture.storage,
            None,
            &fixture.deleted,
            &AtomicBool::new(false),
            progress.prune,
        )
        .unwrap();
        assert_eq!(counter.into_inner(), LevelProgress::units(points.len()));
    }

    /// A build stopped once it made progress fails, with its progress between what it had then
    /// and its total.
    #[test]
    fn progress_stays_below_its_total_when_stopped() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        let (view, progress) = new_progress_tracker();
        let stopped = AtomicBool::new(false);
        let finished = AtomicBool::new(false);
        let (result, at_stop) = std::thread::scope(|scope| {
            let stopper = scope.spawn(|| {
                while !finished.load(Ordering::Acquire) {
                    if let (Some(done @ 1..), _) = done_and_total(&view) {
                        stopped.store(true, Ordering::Relaxed);
                        return Some(done);
                    }
                    std::thread::yield_now();
                }
                None
            });
            let result = fixture.build_links(
                &fixture.points,
                &levels,
                hnsw_m(),
                &params(),
                &stopped,
                &progress,
            );
            finished.store(true, Ordering::Release);
            (result, stopper.join().unwrap())
        });
        let at_stop = at_stop.expect("the build ended before it made progress");
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
        let total = progress_total(&fixture.points, &levels, hnsw_m());
        let (Some(done), Some(at)) = done_and_total(&view) else {
            panic!("no progress");
        };
        assert_eq!(at, total);
        assert!(
            at_stop <= done && done < total,
            "{at_stop}, {done} of {total}"
        );
    }

    /// A NaN vector scores NaN against every other point, so it has no candidates. On each level
    /// with others, it links the next point of the level, in id order and wrapping around, that
    /// has links of its own, and no other point links it.
    #[rstest]
    #[case::one(&[500])]
    // A run, scattered points, and a run at the end, whose next point wraps around
    #[case::several(&[7, 100, 101, 102, 103, 104, 105, 333, 1234, 1990, 1993, 1995, 1996, 1997, 1998])]
    fn nan_points_link_the_next_point_with_links(
        #[case] nan: &[PointOffsetType],
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(distance, None);
        fixture.set_nan(nan.iter().copied());
        let levels = random_levels(3);
        let links = fixture.build(&fixture.points, &levels);
        check_links(&links, &fixture.points, &levels, hnsw_m());
        let is_nan = |point| nan.contains(&point);
        for (&point, lists) in fixture.points.iter().zip(&links) {
            for (level, list) in lists.iter().enumerate() {
                if is_nan(point) {
                    let next = fallback(
                        &fixture.points,
                        &levels,
                        level,
                        point,
                        |_| false,
                        |p| !is_nan(p),
                    );
                    assert_eq!(
                        *list,
                        Vec::from_iter(next),
                        "point {point} on level {level}"
                    );
                } else {
                    assert!(
                        !list.iter().any(|&link| is_nan(link)),
                        "point {point} links a NaN point on level {level}: {list:?}",
                    );
                }
            }
        }
    }

    /// With every vector NaN, no point has candidates: each links the next point of its level,
    /// in id order and wrapping around, so that every level with others is a ring.
    #[test]
    fn all_points_nan() {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Distance::Euclid, None);
        fixture.set_nan(0..POINTS as PointOffsetType);
        let levels = random_levels(3);
        let links = fixture.build(&fixture.points, &levels);
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        assert!(degrees.len() >= 3, "{} levels", degrees.len());
        for (&point, lists) in fixture.points.iter().zip(&links) {
            for (level, list) in lists.iter().enumerate() {
                let next = fallback(&fixture.points, &levels, level, point, |_| false, |_| false);
                assert_eq!(
                    *list,
                    Vec::from_iter(next),
                    "point {point} on level {level}"
                );
            }
        }
    }

    /// Two points without candidates, as one or both are NaN, link each other as in
    /// [`two_points`].
    #[rstest]
    #[case::first(&[3])]
    #[case::second(&[250])]
    #[case::both(&[3, 250])]
    fn two_points_with_nan(#[case] nan: &[PointOffsetType]) {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Distance::Dot, None);
        fixture.set_nan(nan.iter().copied());
        let mut levels = vec![0; POINTS];
        levels[250] = 2;
        let want: Links = vec![vec![vec![250]], vec![vec![3], vec![], vec![]]];
        assert_eq!(fixture.build(&[3, 250], &levels), want);
    }

    /// NaN points on the upper levels, the one alone on the top level among them: a fallback
    /// link on each level with others, none where it's alone.
    #[test]
    fn nan_points_on_upper_levels() {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Distance::Euclid, None);
        // Up to levels 4, 3, 2, 2, and 1, see `few_points_on_top`
        let nan: [PointOffsetType; 5] = [10, 410, 60, 110, 15];
        fixture.set_nan(nan);
        let levels = few_points_on_top();
        let links = fixture.build(&fixture.points, &levels);
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        assert_eq!(degrees[4], [0], "alone on the top level");

        let lists = |point| &links[fixture.points.binary_search(&point).unwrap()];
        for point in nan {
            assert_eq!(lists(point).len(), levels[point as usize] + 1);
            for (level, list) in lists(point).iter().enumerate() {
                let next = fallback(
                    &fixture.points,
                    &levels,
                    level,
                    point,
                    |_| false,
                    |p| !nan.contains(&p),
                );
                assert_eq!(
                    *list,
                    Vec::from_iter(next),
                    "point {point} on level {level}"
                );
            }
        }
        // Of the 5 points of level 3, 10 and 410 skip each other
        assert_eq!(lists(10)[3], [810]);
        assert_eq!(lists(410)[3], [810]);
    }

    /// Points given although deleted, by their flag or their vector, are never linked, also not
    /// by a fallback: NaN points skip them for the next point with links, and the ring of a level
    /// of NaN points skips them too.
    #[rstest]
    fn fallback_skips_deleted_points(#[values(false, true)] all_nan: bool) {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Distance::Euclid, None);
        // Every id, so also those flagged as deleted: 9, 19, and so on
        let points: Vec<PointOffsetType> = (0..200).collect();
        // Right before deleted points: 9 and 10, 19, and 199, after which the next point wraps
        // around
        let nan = if all_nan {
            points.clone()
        } else {
            vec![7, 8, 18, 198]
        };
        fixture.set_nan(nan.iter().copied());
        // After making it NaN, which would restore it
        fixture.storage.delete_vector(10).unwrap();
        let deleted = |point: PointOffsetType| fixture.deleted[point as usize] || point == 10;

        let levels = vec![0; POINTS];
        let links = fixture.build(&points, &levels);
        check_links(&links, &points, &levels, hnsw_m());
        for (&point, lists) in points.iter().zip(&links) {
            assert!(
                !lists[0].iter().any(|&link| deleted(link)),
                "point {point} links a deleted point: {:?}",
                lists[0],
            );
            if nan.contains(&point) {
                let next = fallback(&points, &levels, 0, point, deleted, |p| !nan.contains(&p));
                assert_eq!(lists[0], Vec::from_iter(next), "point {point}");
            }
        }
    }

    /// Fallback links don't depend on the number of threads either: NaN points on every level,
    /// or only NaN points.
    #[rstest]
    fn fallback_deterministic_across_thread_pools(#[values(7, 1)] nan_every: usize) {
        let _scope = ambient::test_guard();
        let mut fixture = Fixture::new(Distance::Euclid, None);
        fixture.set_nan((0..POINTS as PointOffsetType).step_by(nan_every));
        let levels = random_levels(5);
        let links = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| fixture.build(&fixture.points, &levels))
        };
        let expected = links(1);
        check_links(&expected, &fixture.points, &levels, hnsw_m());
        for threads in [1, 2, 7] {
            assert!(links(threads) == expected, "{threads} threads");
        }
    }

    /// A point alone in each of its leaves has no candidates either. Leaves of at most 2 points,
    /// cut at random, leave one of 7 points alone: the others link their partner, which links
    /// them back, and the one alone links the next point.
    #[test]
    fn point_alone_in_its_leaf() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let points = &fixture.points[..7];
        let levels = vec![0; POINTS];
        let params = PipnnParams {
            partition: PartitionParams {
                max_leaf_size: 2,
                max_depth: 0,
                ..params().partition
            },
            ..params()
        };
        let links = fixture
            .build_with(points, &levels, hnsw_m(), &params, &AtomicBool::new(false))
            .unwrap()
            .unwrap();
        check_links(&links, points, &levels, hnsw_m());
        assert!(
            links
                .iter()
                .all(|lists| lists.len() == 1 && lists[0].len() == 1),
            "{links:?}",
        );
        let link = |i: usize| links[i][0][0];
        let position = |point| points.binary_search(&point).unwrap();
        let alone: Vec<_> = (0..points.len())
            .filter(|&i| link(position(link(i))) != points[i])
            .collect();
        assert_eq!(alone.len(), 1, "{links:?}");
        assert_eq!(link(alone[0]), points[(alone[0] + 1) % points.len()]);
    }

    /// The fallback on its own, over points of which 2 is flagged as deleted, 4 has a deleted
    /// vector, and 9 has no flag, which counts as deleted: they get fallback links, but never
    /// become one.
    #[rstest]
    #[case::none_empty(
        &[1, 2, 4, 6, 8, 9],
        vec![vec![6], vec![1], vec![8], vec![1], vec![6], vec![1]],
        vec![vec![6], vec![1], vec![8], vec![1], vec![6], vec![1]],
    )]
    // Only the links of 6 count, so all go there, 8 wrapping around
    #[case::next_with_links(
        &[1, 2, 4, 6, 8, 9],
        vec![vec![], vec![6], vec![], vec![1], vec![], vec![1]],
        vec![vec![6], vec![6], vec![6], vec![1], vec![6], vec![1]],
    )]
    // A ring of 1, 6, and 8
    #[case::ring(
        &[1, 2, 4, 6, 8, 9],
        vec![vec![]; 6],
        vec![vec![6], vec![6], vec![6], vec![8], vec![1], vec![1]],
    )]
    // Nothing for 6 to link
    #[case::one_not_deleted(&[2, 4, 6, 9], vec![vec![]; 4], vec![vec![6], vec![6], vec![], vec![6]])]
    fn fallback_links_without_a_build(
        #[case] points: &[PointOffsetType],
        #[case] lists: Vec<Vec<PointOffsetType>>,
        #[case] want: Vec<Vec<PointOffsetType>>,
    ) {
        let mut point_deleted = BitVec::repeat(false, 9);
        point_deleted.set(2, true);
        let mut vector_deleted = BitVec::repeat(false, 10);
        vector_deleted.set(4, true);
        let not_deleted = NotDeletedChecker {
            point_deleted: &point_deleted,
            vec_deleted: &vector_deleted,
        };
        let mut links = lists.clone();
        let linked = link_empty_lists(&mut links, points, &not_deleted);
        assert_eq!(links, want);
        let filled =
            std::iter::zip(&lists, &want).filter(|(before, after)| before.len() < after.len());
        assert_eq!(linked, filled.count());
    }

    /// The fallback on random lists, some empty, of random points with gaps, deleted flags,
    /// deleted vectors, and sometimes too few flags: lists with links keep them, and each empty
    /// one gets the link [`fallback`] picks, if any.
    #[test]
    fn fallback_links_of_random_lists() {
        let mut rng = StdRng::seed_from_u64(99);
        for case in 0..2000 {
            let ids = rng.random_range(1..60);
            let len = rng.random_range(1..=ids);
            let mut points: Vec<PointOffsetType> = rand::seq::index::sample(&mut rng, ids, len)
                .into_iter()
                .map(|id| id as PointOffsetType)
                .collect();
            points.sort_unstable();
            // Without a flag, the last points count as deleted
            let flags = if rng.random_bool(0.1) {
                rng.random_range(0..ids)
            } else {
                ids
            };
            let deleted_share = rng.random_range(0.0..0.6);
            let point_deleted: BitVec =
                (0..flags).map(|_| rng.random_bool(deleted_share)).collect();
            let vector_deleted: BitVec = (0..ids).map(|_| rng.random_bool(deleted_share)).collect();
            let not_deleted = NotDeletedChecker {
                point_deleted: &point_deleted,
                vec_deleted: &vector_deleted,
            };
            let deleted = |point: PointOffsetType| !not_deleted.check_infallible(point);

            let empty_share = rng.random_range(0.0..1.0);
            let lists: Vec<Vec<PointOffsetType>> = points
                .iter()
                .map(|&point| {
                    if rng.random_bool(empty_share) {
                        return Vec::new();
                    }
                    let others: Vec<_> = points
                        .iter()
                        .copied()
                        .filter(|&other| other != point && !deleted(other))
                        .collect();
                    let len = rng.random_range(0..=others.len().min(4));
                    rand::seq::index::sample(&mut rng, others.len(), len)
                        .into_iter()
                        .map(|i| others[i])
                        .collect()
                })
                .collect();

            let mut links = lists.clone();
            let linked = link_empty_lists(&mut links, &points, &not_deleted);
            let levels = vec![0; ids];
            let has_links =
                |point: PointOffsetType| !lists[points.binary_search(&point).unwrap()].is_empty();
            let mut filled = 0;
            for ((&point, before), after) in points.iter().zip(&lists).zip(&links) {
                if before.is_empty() {
                    let next = fallback(&points, &levels, 0, point, deleted, has_links);
                    filled += usize::from(next.is_some());
                    assert_eq!(*after, Vec::from_iter(next), "case {case}, point {point}");
                } else {
                    assert_eq!(after, before, "case {case}, point {point}");
                }
            }
            assert_eq!(linked, filled, "case {case}");
        }
    }

    /// The debug check of [`build_links`] finds each broken invariant, starting from valid links
    /// of points 1 and 3, on level 1, and 2 and 4, on level 0, with `m` 1 and `m0` 2. Point 5 is
    /// deleted, and 7 isn't one of the points.
    #[rstest]
    #[case::valid(|_: &mut Links| {}, None)]
    #[case::shape(|links: &mut Links| links[1].push(vec![]), Some("point 2 of level 0 has 2 lists"))]
    #[case::lower_level(|links: &mut Links| links[0][1] = vec![2], Some("link 2 of point 1 on level 1 is on a lower level"))]
    #[case::too_many(|links: &mut Links| links[0][1] = vec![3, 2], Some("point 1 has 2 links on level 1, more than 1"))]
    #[case::itself(|links: &mut Links| links[1][0] = vec![2], Some("link 2 of point 2 on level 0 is the point itself"))]
    #[case::twice(|links: &mut Links| links[1][0] = vec![1, 1], Some("link 1 of point 2 on level 0 comes twice"))]
    #[case::deleted(|links: &mut Links| links[1][0] = vec![5], Some("link 5 of point 2 on level 0 is deleted"))]
    #[case::not_a_point(|links: &mut Links| links[1][0] = vec![7], Some("link 7 of point 2 on level 0 isn't one of the points"))]
    #[case::no_links(|links: &mut Links| links[1][0].clear(), Some("point 2 has no links on level 0"))]
    #[case::no_links_above(|links: &mut Links| links[2][1].clear(), Some("point 3 has no links on level 1"))]
    fn debug_check_finds_broken_invariants(
        #[case] corrupt: fn(&mut Links),
        #[case] want: Option<&str>,
    ) {
        let points = [1, 2, 3, 4];
        let levels = [0, 1, 0, 1, 0];
        let mut links: Links = vec![
            vec![vec![2, 3], vec![3]],
            vec![vec![1]],
            vec![vec![4], vec![1]],
            vec![vec![3]],
        ];
        corrupt(&mut links);
        let mut point_deleted = BitVec::repeat(false, 8);
        point_deleted.set(5, true);
        let not_deleted = NotDeletedChecker {
            point_deleted: &point_deleted,
            vec_deleted: &BitVec::new(),
        };
        let found = broken_invariant(
            &links,
            &points,
            |point| levels[point as usize],
            HnswM::new(1, 2),
            &not_deleted,
        );
        assert_eq!(found.as_deref(), want);
    }

    /// Whether `links` break one of the invariants of [`broken_invariant`], checked the obvious
    /// way.
    fn breaks_an_invariant(
        links: &Links,
        points: &[PointOffsetType],
        levels: &[usize],
        hnsw_m: HnswM,
        deleted: impl Fn(PointOffsetType) -> bool,
    ) -> bool {
        let on_level = |point: PointOffsetType, level: usize| {
            points.contains(&point) && levels[point as usize] >= level
        };
        let broken = |point: PointOffsetType, level: usize, list: &[PointOffsetType]| {
            let level_m = hnsw_m.level_m(level);
            let others = points
                .iter()
                .any(|&other| other != point && on_level(other, level) && !deleted(other));
            list.len() > level_m
                || (list.is_empty() && level_m > 0 && others)
                || list.iter().enumerate().any(|(i, &link)| {
                    link == point
                        || list[..i].contains(&link)
                        || deleted(link)
                        || !on_level(link, level)
                })
        };
        links.len() != points.len()
            || points.iter().zip(links).any(|(&point, lists)| {
                lists.len() != levels[point as usize] + 1
                    || lists
                        .iter()
                        .enumerate()
                        .any(|(level, list)| broken(point, level, list))
            })
    }

    /// The debug check agrees with [`breaks_an_invariant`] on random graphs over random points
    /// with gaps and deleted ones: valid ones, and ones with one random defect each.
    #[test]
    fn debug_check_agrees_on_random_graphs() {
        let mut rng = StdRng::seed_from_u64(17);
        let cases = 2000;
        let mut broken = 0;
        for case in 0..cases {
            let ids = rng.random_range(1..40);
            let len = rng.random_range(1..=ids);
            let mut points: Vec<PointOffsetType> = rand::seq::index::sample(&mut rng, ids, len)
                .into_iter()
                .map(|id| id as PointOffsetType)
                .collect();
            points.sort_unstable();
            let levels: Vec<usize> = (0..ids)
                .map(|_| (0..3).take_while(|_| rng.random_bool(0.3)).count())
                .collect();
            let hnsw_m = HnswM::new(rng.random_range(0..3), rng.random_range(0..4));
            let point_deleted: BitVec = (0..ids).map(|_| rng.random_bool(0.15)).collect();
            let vector_deleted: BitVec = (0..ids).map(|_| rng.random_bool(0.15)).collect();
            let not_deleted = NotDeletedChecker {
                point_deleted: &point_deleted,
                vec_deleted: &vector_deleted,
            };
            let deleted = |point: PointOffsetType| !not_deleted.check_infallible(point);

            // Valid: each list some points of its level or above that aren't deleted, at least
            // one where the limit and the level allow
            let mut links: Links = points
                .iter()
                .map(|&point| {
                    (0..=levels[point as usize])
                        .map(|level| {
                            let others: Vec<_> = points
                                .iter()
                                .copied()
                                .filter(|&other| {
                                    other != point
                                        && levels[other as usize] >= level
                                        && !deleted(other)
                                })
                                .collect();
                            let most = hnsw_m.level_m(level).min(others.len());
                            let len = if most == 0 {
                                0
                            } else {
                                rng.random_range(1..=most)
                            };
                            rand::seq::index::sample(&mut rng, others.len(), len)
                                .into_iter()
                                .map(|i| others[i])
                                .collect()
                        })
                        .collect()
                })
                .collect();
            if rng.random_bool(0.5) {
                let i = rng.random_range(0..points.len());
                let level = rng.random_range(0..links[i].len());
                let list = &mut links[i][level];
                match rng.random_range(0..6) {
                    0 => list.clear(),
                    1 => list.push(points[i]),
                    // Possibly no point at all
                    2 => list.push(rng.random_range(0..ids as PointOffsetType + 2)),
                    3 => list.extend(list.first().copied()),
                    4 => links[i].push(Vec::new()),
                    _ => list.extend_from_slice(&points),
                }
            }

            let want = breaks_an_invariant(&links, &points, &levels, hnsw_m, deleted);
            let found = broken_invariant(
                &links,
                &points,
                |point| levels[point as usize],
                hnsw_m,
                &not_deleted,
            );
            assert_eq!(found.is_some(), want, "case {case}: {found:?} in {links:?}");
            broken += usize::from(want);
        }
        // Both kinds, in about the expected shares
        assert!(
            (cases / 4..cases * 3 / 4).contains(&broken),
            "{broken} of {cases} broken",
        );
    }
}
