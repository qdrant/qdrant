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
//! vectors. Leaf k 8 with a reservoir factor of 4 doubles the reservoirs, to about 1.5 GB.

// TODO: remove once the graph build calls into this module.
#![expect(dead_code, reason = "Not connected to the graph build yet")]

mod hash_prune;
mod leaf;
mod partition;
mod prune;
#[cfg(test)]
mod review_tests;
mod sketches;
#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::bitvec::BitSlice;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rayon::prelude::*;

use self::hash_prune::Reservoirs;
pub(super) use self::partition::PartitionParams;
use self::sketches::Sketches;
use super::HnswM;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::vector_storage::VectorStorageEnum;
use crate::vector_storage::matrix_scorer::MatrixScorer;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

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
    /// Seed of every random choice of the build, drawn by the caller from the build's rng.
    pub seed: u64,
}

impl Default for PipnnParams {
    /// The prototype's parameters: leaf k 4 and twice the degree limit per reservoir, with the
    /// default partitioning. On dbpedia-openai 100K, leaf k 8 with a factor of 4 matched the
    /// recall of HNSW at `ef_construct` 100 from search `ef` 64 up, for about 17% more build
    /// time than these.
    fn default() -> Self {
        Self {
            partition: PartitionParams::default(),
            leaf_k: 4,
            reservoir_factor: 2,
            seed: 0,
        }
    }
}

impl PipnnParams {
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
/// of that level each. A point alone on a level has no links there.
///
/// - `points` must be sorted, distinct, and not deleted; `point_level` gives each one's level.
/// - `scorer` scores the vectors HNSW would, the quantized ones where configured, and so does
///   the final prune through `vector_storage` and `quantized_vectors`. The sketches project the
///   original vectors of `vector_storage`.
/// - `None` if those vectors can't be sketched, because they aren't dense, so the caller builds
///   with HNSW.
/// - Runs on the current rayon pool. The links only depend on the input and `params`, not on the
///   number of threads.
/// - Adds every point to `progress` once, when its links on level 0 are final, so it ends at
///   `points.len()`. Level 0 is built first and takes most of the time: the levels above hold
///   about `1/m` of the points each.
/// - Fails if stopped, on invalid parameters, on points out of order, repeated, or past the
///   stored vectors, and on failed reads.
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
    progress: &AtomicU64,
) -> OperationResult<Option<Vec<Vec<Vec<PointOffsetType>>>>> {
    params.validate()?;
    if !points.is_sorted_by(|a, b| a < b) {
        return Err(OperationError::service_error(
            "PiPNN points must be sorted and distinct",
        ));
    }
    check_process_stopped(stopped)?;
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
        // Without pairs or a degree limit, a level has no links
        let level_m = hnsw_m.level_m(level);
        if level_points.len() >= 2 && level_m > 0 {
            let level_links = build.level(level, &level_points, level_m, seed)?;
            debug_assert_eq!(level_links.len(), level_points.len());
            let on_level = links.iter_mut().filter(|lists| lists.len() > level);
            for (lists, level_links) in on_level.zip(level_links) {
                lists[level] = level_links;
            }
        }
        if level == 0 {
            progress.fetch_add(level_points.len() as u64, Ordering::Relaxed);
        }
    }

    log::debug!(
        "PiPNN links of {} points on {levels} levels in {:.2?}",
        points.len(),
        timer.elapsed(),
    );
    Ok(Some(links))
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

/// What one level's build did, for the debug log.
#[derive(Default)]
struct LevelStats {
    leaves: usize,
    /// Leaf sizes summed up: the number of leaves each point is in, summed over the points.
    memberships: usize,
    /// Neighbors per point in a leaf.
    leaf_k: usize,
    partition: Duration,
    /// Including the allocation of the reservoirs.
    leaf_knn: Duration,
}

impl Build<'_, '_> {
    /// Links of `points`, the points of `level` (sorted, distinct, at least 2), in their order,
    /// at most `level_m` each. `seed` is the level's partitioning seed.
    fn level(
        &self,
        level: usize,
        points: &[PointOffsetType],
        level_m: usize,
        seed: u64,
    ) -> OperationResult<Vec<Vec<PointOffsetType>>> {
        let mut stats = LevelStats::default();
        let capacity = level_m.saturating_mul(self.params.reservoir_factor);
        let reservoirs = self.candidates(points, capacity, seed, &mut stats)?;

        let timer = Instant::now();
        let links = prune::prune(
            &reservoirs,
            points,
            level_m,
            self.vector_storage,
            self.quantized_vectors,
            self.point_deleted,
            self.stopped,
        )?;
        drop(reservoirs);

        log::debug!(
            "PiPNN level {level}: {} points in {} leaves ({:.1} per point), leaf k {}, \
             partition {:.2?}, leaf k-NN {:.2?}, final prune {:.2?}",
            points.len(),
            stats.leaves,
            stats.memberships as f64 / points.len() as f64,
            stats.leaf_k,
            stats.partition,
            stats.leaf_knn,
            timer.elapsed(),
        );
        Ok(links)
    }

    /// The reservoirs of `points` (sorted, distinct, at least 2), `capacity` each, filled with
    /// the bidirected k-NN edges of their leaves.
    fn candidates(
        &self,
        points: &[PointOffsetType],
        capacity: usize,
        seed: u64,
        stats: &mut LevelStats,
    ) -> OperationResult<Reservoirs> {
        let timer = Instant::now();
        let leaves = partition::partition(
            self.scorer,
            points.to_vec(),
            &self.params.partition,
            seed,
            self.stopped,
        )?;
        stats.partition = timer.elapsed();
        if let Some(path) = std::env::var_os("PIPNN_DUMP_LEAVES") {
            // TODO(profiling): remove
            let sizes: Vec<String> = leaves.iter().map(|l| l.len().to_string()).collect();
            let mut path = std::path::PathBuf::from(path);
            path.push(format!("leaves_{}.txt", points.len()));
            fs_err::write(path, sizes.join("\n")).unwrap();
        }
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
            |point, candidate, score| {
                reservoirs.insert(point, candidate, sketches.hash(point, candidate), score);
            },
        )?;
        drop(leaves);
        stats.leaf_knn = timer.elapsed();
        Ok(reservoirs)
    }
}

#[cfg(test)]
mod orchestration_tests {
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;

    use common::ambient;
    use common::bitvec::BitVec;
    use rstest::rstest;
    use tempfile::TempDir;

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
            seed: 1,
            ..PipnnParams::default()
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

        /// [`build_links`] with `levels` by id. Checks that a finished build counted every
        /// point once.
        fn build_with(
            &self,
            points: &[PointOffsetType],
            levels: &[usize],
            hnsw_m: HnswM,
            params: &PipnnParams,
            stopped: &AtomicBool,
        ) -> OperationResult<Option<Links>> {
            let progress = AtomicU64::new(0);
            let links = build_links(
                &self.storage,
                self.quantized.as_ref(),
                &self.deleted,
                &self.scorer(),
                points,
                |point| levels[point as usize],
                hnsw_m,
                params,
                stopped,
                &progress,
            );
            if let Ok(Some(_)) = links {
                assert_eq!(progress.into_inner(), points.len() as u64, "progress");
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
            let reservoirs = build.candidates(points, capacity, 4, stats).unwrap();
            (reservoirs, sketches)
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
        // Not degenerate: a point that has others on its level links some of them
        for (level, degrees) in degrees.iter().enumerate() {
            if degrees.len() >= 2 {
                assert!(
                    degrees.iter().all(|&d| d > 0),
                    "a point without links on {level}"
                );
            }
        }
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
        assert!(degrees[0].iter().all(|&d| d > 0), "a point without links");
        assert!(mean(&degrees[0]) >= 2.0, "degree {}", mean(&degrees[0]));
    }

    /// 1800 points on level 0, 400 on level 1, 40 on level 2, 5 on level 3, and one alone on
    /// level 4. Levels 2 and 3 fit into one leaf.
    #[test]
    fn several_levels_with_few_points_on_top() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels: Vec<usize> = (0..POINTS)
            .map(|id| match id {
                10 => 4,
                _ if id % 400 == 10 => 3,
                _ if id % 50 == 10 => 2,
                _ if id % 5 == 0 => 1,
                _ => 0,
            })
            .collect();
        let links = fixture.build(&fixture.points, &levels);
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        let sizes: Vec<_> = degrees.iter().map(Vec::len).collect();
        assert_eq!(sizes, [1800, 400, 40, 5, 1]);

        assert_eq!(degrees[4], [0], "alone on the top level");
        for (level, degrees) in degrees[..4].iter().enumerate() {
            assert!(degrees.iter().all(|&d| d > 0), "level {level}");
        }
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
        let (reservoirs, sketches) = fixture.candidates(points, capacity, &mut stats);
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
        fixture.candidates(&fixture.points[..LEAF + 1], 2 * M, &mut stats);
        assert!(stats.leaves > 1);
        assert_eq!(stats.leaf_k, params().leaf_k);
    }

    #[test]
    fn deterministic_across_thread_pools() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, Some(scalar()));
        let levels = random_levels(5);
        let links = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| {
                let _scope = ambient::test_guard();
                fixture.build(&fixture.points, &levels)
            })
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
        let degrees = check_links(&links, &fixture.points, &levels, hnsw_m());
        assert!(degrees[0].iter().all(|&d| d > 0), "a point without links");
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
                let progress = AtomicU64::new(0);
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
                assert_eq!(progress.into_inner(), 0);
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
            &AtomicU64::new(0),
        );
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
    }

    #[test]
    fn rejects_unsorted_or_repeated_points() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels = random_levels(3);
        let stopped = AtomicBool::new(false);
        for points in [&[2, 1][..], &[1, 1]] {
            let result = fixture.build_with(points, &levels, hnsw_m(), &params(), &stopped);
            assert!(
                matches!(result, Err(OperationError::ServiceError { .. })),
                "{points:?}",
            );
        }
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

    /// A degree limit of 0 leaves its levels without links, and the others as they'd be. Every
    /// point is still counted once.
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

    /// Every point counts once, also when it's alone on level 0, or on several levels.
    #[rstest]
    #[case::one_point(&[7])]
    #[case::two_points(&[7, 8])]
    #[case::subset(&[0, 3, 5, 11, 12, 13, 20, 40, 41, 42])]
    fn progress_counts_every_point_once(#[case] points: &[PointOffsetType]) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Distance::Euclid, None);
        let levels: Vec<usize> = (0..POINTS).map(|id| id % 4).collect();
        // Checked in `build_with`
        let links = fixture.build(points, &levels);
        check_links(&links, points, &levels, hnsw_m());
    }
}
