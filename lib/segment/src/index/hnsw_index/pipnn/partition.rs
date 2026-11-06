//! Randomized ball carving: splits the points of one level into overlapping leaves.
//!
//! - Samples leaders from the points, at most a fixed number per step, and assigns every point
//!   to its `fanout` nearest leaders through a matrix call against the leaders.
//! - Recurses into every cluster larger than the leaf size, with the next level's fanout.
//! - Merges small clusters at random, never above the leaf size.
//! - Sorts and deduplicates every cluster: with fanout above 1, a point can land in two clusters
//!   that get merged, and leaf scoring requires distinct rows.
//! - Cuts a cluster into random chunks if carving doesn't shrink it: past the maximum depth, or
//!   when two steps in a row kept almost all of its points together, as duplicate vectors do.
//!   Points whose scores are all NaN get no leader, and are not carved any further.
//!
//! Randomness comes from seeds derived from the build's rng, so a build is reproducible. Each
//! step draws its leaders and its children's seeds from its own seed, and the picks of a point
//! don't depend on the other points of its matrix call, so the leaves don't depend on the number
//! of threads either.
//!
//! Progress: a step that carves keeps half of its share for its matrix calls, adding it per call,
//! and hands the other half to its clusters by size. A cluster that becomes a leaf, or is cut
//! into chunks, adds its share at once; one that is carved again splits it the same way. The top
//! step of a large level and the steps below it take about the same time.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::reason::reason;
use common::types::PointOffsetType;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _};
use rayon::prelude::*;

use super::progress::Progress;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::vector_storage::matrix_scorer::{MatrixCols, MatrixScorer, MatrixTopK};

/// Points per matrix call against the leaders. Every call packs the leaders again, so smaller
/// calls would pay that more often.
const POINTS_PER_CALL: usize = 1024;

/// An unused slot of a point's nearest leaders. Points with NaN scores get fewer picks.
const NO_LEADER: u32 = u32::MAX;

/// A child cluster that kept at least this many tenths of its parent's points didn't shrink.
const STALL_TENTHS: usize = 9;

/// Steps in a row without shrinking, after which a cluster is cut into random chunks: one more
/// sample of leaders, for a bad first sample, but no more for duplicate vectors, which no
/// leaders can separate.
const MAX_STALLS: usize = 2;

/// Parameters of randomized ball carving.
#[derive(Clone, Debug)]
pub(in crate::index::hnsw_index) struct PartitionParams {
    /// Leaves have at most this many points.
    pub max_leaf_size: usize,
    /// Clusters with fewer points are merged at random, never above `max_leaf_size`, so that
    /// every point gets enough candidates in its leaf.
    pub min_leaf_size: usize,
    /// Share of a cluster's points that become its leaders, rounded up. At least 2, at most
    /// `max_leaders`.
    ///
    /// A cluster's children then have about `fanout / leader_fraction` points: 1000 with the
    /// default fanout of 10 at the top level, close to the leaf size, and 300 with the 3 of the
    /// second level.
    pub leader_fraction: f64,
    /// At most this many leaders per step, at least 2.
    pub max_leaders: usize,
    /// To how many nearest leaders each point is assigned, by recursion depth (multi-level
    /// fanout). 1 beyond the list.
    pub fanout: Vec<usize>,
    /// Clusters still larger than a leaf at this depth are cut into random chunks.
    pub max_depth: usize,
}

impl Default for PartitionParams {
    fn default() -> Self {
        Self {
            max_leaf_size: 1024,
            min_leaf_size: 256,
            leader_fraction: 0.01,
            max_leaders: 1000,
            fanout: vec![10, 3],
            max_depth: 16,
        }
    }
}

/// Splits `points` into overlapping leaves by randomized ball carving, on the current rayon
/// pool.
///
/// Leaves: each sorted ascending, without duplicates, at most `max_leaf_size` points; every input
/// point in at least one leaf. Deterministic for the same input and seed. The order of `points`
/// and repeated points don't matter, nor does the number of threads.
///
/// Adds all of `progress` by the time it returns the leaves, per step, see the module docs.
///
/// Memory: one step holds `fanout` leader slots per point of its cluster, and then the clusters
/// themselves, 40 bytes each per point at the top level with a fanout of 10. The leaves hold
/// about 4 bytes per point and leaf it's in, typically 30.
pub(super) fn partition(
    scorer: &MatrixScorer<'_>,
    mut points: Vec<PointOffsetType>,
    params: &PartitionParams,
    seed: u64,
    stopped: &AtomicBool,
    progress: Progress<'_>,
) -> OperationResult<Vec<Vec<PointOffsetType>>> {
    if params.max_leaf_size == 0 {
        return Err(OperationError::service_error(
            "PiPNN leaves must have room for at least one point",
        ));
    }
    check_process_stopped(stopped)?;

    // Every step relies on sorted and distinct points, which also makes the leaves independent
    // of the input order
    points.par_sort_unstable();
    points.dedup();
    if points.is_empty() {
        progress.add();
        return Ok(Vec::new());
    }

    let carving = Carving {
        scorer,
        params,
        stopped,
        top_k: TopKPool {
            k: top_k_size(params, points.len()),
            free: Mutex::default(),
        },
    };
    let leaves = carving.split(points, 0, seed, 0, progress)?;
    debug_assert!(leaves.iter().all(|leaf| {
        !leaf.is_empty() && leaf.len() <= params.max_leaf_size && leaf.is_sorted_by(|a, b| a < b)
    }));
    Ok(leaves)
}

/// What all steps of one [`partition`] call share.
struct Carving<'a, 's> {
    scorer: &'a MatrixScorer<'s>,
    params: &'a PartitionParams,
    stopped: &'a AtomicBool,
    top_k: TopKPool,
}

/// What a step does with one of its clusters.
enum Next {
    /// Small enough already.
    Leaf(Vec<PointOffsetType>),
    /// Carve it again, one level deeper.
    Split {
        points: Vec<PointOffsetType>,
        seed: u64,
        /// Steps in a row that didn't shrink it.
        stalls: usize,
    },
    /// Cut it into random chunks.
    Chunks {
        points: Vec<PointOffsetType>,
        seed: u64,
    },
}

impl Next {
    fn len(&self) -> usize {
        match self {
            Next::Leaf(points) | Next::Split { points, .. } | Next::Chunks { points, .. } => {
                points.len()
            }
        }
    }
}

impl Carving<'_, '_> {
    /// Leaves of `points`, sorted and distinct, at recursion `depth`, after `stalls` steps in a
    /// row that kept them together. Adds all of `progress` by the time it returns them.
    fn split(
        &self,
        points: Vec<PointOffsetType>,
        depth: usize,
        seed: u64,
        stalls: usize,
        progress: Progress<'_>,
    ) -> OperationResult<Vec<Vec<PointOffsetType>>> {
        let params = self.params;
        if points.len() <= params.max_leaf_size {
            progress.add();
            return Ok(vec![points]);
        }
        if depth >= params.max_depth || stalls >= MAX_STALLS {
            progress.add();
            return Ok(random_chunks(points, params.max_leaf_size, seed));
        }
        check_process_stopped(self.stopped)?;
        // Half for this step's matrix calls, half for its clusters
        let (own, children) = progress.halves();

        let mut rng = StdRng::seed_from_u64(seed);
        let leaders = sample_leaders(&points, self.leader_count(points.len()), &mut rng);
        let fanout = params
            .fanout
            .get(depth)
            .copied()
            .unwrap_or(1)
            .clamp(1, leaders.len());
        let nearest = self.nearest_leaders(&points, &leaders, fanout, own)?;
        let (clusters, orphans) = group_by_leader(&points, &nearest, fanout, leaders.len());
        drop(nearest);
        let parent_len = points.len();
        drop(points);

        let mut next = Vec::new();
        let mut small = Vec::new();
        // Points without any leader, because all their scores were NaN, can't be carved
        let clusters = clusters.into_iter().map(|cluster| (cluster, true));
        for (cluster, carve) in clusters.chain([(orphans, false)]) {
            if cluster.is_empty() {
                continue;
            }
            if cluster.len() > params.max_leaf_size {
                let seed = rng.random();
                next.push(if carve {
                    let stalls = stalls_after(stalls, parent_len, cluster.len());
                    Next::Split {
                        points: cluster,
                        seed,
                        stalls,
                    }
                } else {
                    Next::Chunks {
                        points: cluster,
                        seed,
                    }
                });
            } else if cluster.len() < params.min_leaf_size {
                small.push(cluster);
            } else {
                next.push(Next::Leaf(cluster));
            }
        }
        next.extend(merge_small(small, params.max_leaf_size, &mut rng).map(Next::Leaf));

        // Every point is in some cluster, so there's at least one
        let shares = children.split(next.iter().map(|next| next.len() as u64));
        let leaves = next
            .into_par_iter()
            .zip(shares)
            .map(|(next, progress)| match next {
                Next::Leaf(leaf) => {
                    progress.add();
                    Ok(vec![leaf])
                }
                Next::Split {
                    points,
                    seed,
                    stalls,
                } => self.split(points, depth + 1, seed, stalls, progress),
                Next::Chunks { points, seed } => {
                    progress.add();
                    Ok(random_chunks(points, params.max_leaf_size, seed))
                }
            })
            .collect::<OperationResult<Vec<_>>>()?;
        Ok(leaves.into_iter().flatten().collect())
    }

    /// How many leaders a cluster of `len` points gets, `len` at least 2: the paper's
    /// `min(P_samp · |P|, cap)`, and at least 2.
    fn leader_count(&self, len: usize) -> usize {
        // Saturates, so a NaN or negative fraction gives 0
        let wanted = (len as f64 * self.params.leader_fraction).ceil() as usize;
        wanted.clamp(2, self.params.max_leaders.max(2)).min(len)
    }

    /// The `fanout` nearest leaders of each point, as positions in `leaders`: `fanout` slots
    /// per point, best first, the unused ones [`NO_LEADER`]. Adds `progress` per matrix call,
    /// by its points.
    fn nearest_leaders(
        &self,
        points: &[PointOffsetType],
        leaders: &[PointOffsetType],
        fanout: usize,
        progress: Progress<'_>,
    ) -> OperationResult<Vec<u32>> {
        let mut nearest = vec![NO_LEADER; points.len() * fanout];
        let shares = progress.split(points.chunks(POINTS_PER_CALL).map(|rows| rows.len() as u64));
        points
            .par_chunks(POINTS_PER_CALL)
            .zip(nearest.par_chunks_mut(POINTS_PER_CALL * fanout))
            .zip(shares)
            .try_for_each(|((rows, nearest), progress)| -> OperationResult<()> {
                check_process_stopped(self.stopped)?;
                let _scope = ambient::unmeasured_guard(reason("Internal operation"));
                self.top_k.with(|top_k| {
                    self.scorer.top_k(rows, MatrixCols::Other(leaders), top_k)?;
                    for (r, slots) in nearest.chunks_exact_mut(fanout).enumerate() {
                        // `top_k` may keep more than `fanout`, best first
                        let (cols, _) = top_k.row(r);
                        let picked = cols.len().min(fanout);
                        slots[..picked].copy_from_slice(&cols[..picked]);
                    }
                    Ok::<_, OperationError>(())
                })?;
                progress.add();
                Ok(())
            })?;
        Ok(nearest)
    }
}

/// `points` grouped by their nearest leaders: one list per leader, and one of the points without
/// any leader. Each sorted and distinct, as `points` is.
fn group_by_leader(
    points: &[PointOffsetType],
    nearest: &[u32],
    fanout: usize,
    leaders: usize,
) -> (Vec<Vec<PointOffsetType>>, Vec<PointOffsetType>) {
    // Counted first, so every cluster is allocated once, at its size
    let mut sizes = vec![0; leaders];
    for &leader in nearest {
        if leader != NO_LEADER {
            sizes[leader as usize] += 1;
        }
    }
    let mut clusters: Vec<Vec<_>> = sizes.into_iter().map(Vec::with_capacity).collect();
    let mut orphans = Vec::new();
    for (&point, slots) in points.iter().zip(nearest.chunks_exact(fanout)) {
        let mut assigned = false;
        // Slots fill from the front
        for &leader in slots.iter().take_while(|&&leader| leader != NO_LEADER) {
            clusters[leader as usize].push(point);
            assigned = true;
        }
        if !assigned {
            orphans.push(point);
        }
    }
    (clusters, orphans)
}

/// `count` distinct points of `points`, at random, in the order of `points`.
fn sample_leaders(
    points: &[PointOffsetType],
    count: usize,
    rng: &mut StdRng,
) -> Vec<PointOffsetType> {
    let mut positions = rand::seq::index::sample(rng, points.len(), count).into_vec();
    positions.sort_unstable();
    positions.into_iter().map(|p| points[p]).collect()
}

/// Steps in a row without shrinking, for a child of `child_len` of the `parent_len` points of a
/// step that followed `stalls` such steps.
fn stalls_after(stalls: usize, parent_len: usize, child_len: usize) -> usize {
    if child_len * 10 >= parent_len * STALL_TENTHS {
        stalls + 1
    } else {
        0
    }
}

/// Merges clusters of at most `max_len` points, in random order, into groups of at most
/// `max_len` points, each sorted and deduplicated. Empty clusters disappear.
fn merge_small(
    mut small: Vec<Vec<PointOffsetType>>,
    max_len: usize,
    rng: &mut StdRng,
) -> impl Iterator<Item = Vec<PointOffsetType>> {
    small.shuffle(rng);
    let mut groups = Vec::new();
    let mut group: Vec<PointOffsetType> = Vec::new();
    for cluster in small {
        if group.len() + cluster.len() > max_len {
            groups.push(std::mem::take(&mut group));
        }
        group.extend(cluster);
    }
    groups.push(group);
    groups
        .into_iter()
        .filter(|group| !group.is_empty())
        .map(|mut group| {
            // Fanout can put a point into several of the merged clusters
            group.sort_unstable();
            group.dedup();
            group
        })
}

/// Cuts `points` into the fewest chunks of at most `max_len` points, at random, all of about the
/// same size, each sorted.
fn random_chunks(
    mut points: Vec<PointOffsetType>,
    max_len: usize,
    seed: u64,
) -> Vec<Vec<PointOffsetType>> {
    let count = points.len().div_ceil(max_len);
    if count == 0 {
        return Vec::new();
    }
    points.shuffle(&mut StdRng::seed_from_u64(seed));
    let (len, longer) = (points.len() / count, points.len() % count);
    let mut rest = points.as_slice();
    (0..count)
        .map(|i| {
            let (chunk, tail) = rest.split_at(len + usize::from(i < longer));
            rest = tail;
            let mut chunk = chunk.to_vec();
            chunk.sort_unstable();
            chunk
        })
        .collect()
}

/// `k` of the top-k buffers for partitioning `len` points: the largest fanout, but at most the
/// leaders a step can have, which every step clamps its fanout to. The buffers hold `k` slots per
/// row, so an unclamped fanout far above the leaders would size them for picks that can't exist.
fn top_k_size(params: &PartitionParams, len: usize) -> usize {
    // The upper bound of `Carving::leader_count`: no step has more points than the input
    let leaders = params.max_leaders.max(2).min(len);
    let fanout = params.fanout.iter().copied().max().unwrap_or(1);
    fanout.min(leaders).max(1)
}

/// Top-k buffers for the matrix calls of all steps, with `k` from [`top_k_size`]. A matrix call
/// doesn't use rayon, so there's at most one buffer per thread.
///
/// Not one per step, as `map_init` would give: a buffer grows its scratch space to the rows and
/// columns of its first call (about 7 MB for f32 vectors of dim 1536), and there are thousands of
/// steps. Not one per fanout either, which would hold that much per thread and fanout; steps
/// with a smaller fanout read the first picks of each row, at a slightly higher cost of the
/// top-k sink.
struct TopKPool {
    k: usize,
    free: Mutex<Vec<MatrixTopK>>,
}

impl TopKPool {
    fn with<R>(&self, f: impl FnOnce(&mut MatrixTopK) -> R) -> R {
        let mut top_k = self
            .free
            .lock()
            .pop()
            .unwrap_or_else(|| MatrixTopK::new(self.k));
        let result = f(&mut top_k);
        self.free.lock().push(top_k);
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;

    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::types::{
        BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
        QuantizationConfig, ScalarQuantizationConfig,
    };
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::matrix_scorer::new_matrix_scorer;
    use crate::vector_storage::quantized::quantized_vectors::{
        QuantizedVectors, QuantizedVectorsStorageType,
    };
    use crate::vector_storage::{VectorStorage as _, VectorStorageEnum};

    const LEAF: usize = 64;
    /// Progress of a call, more units than it has pieces of work.
    const PROGRESS: u64 = 1_000_003;

    /// Small leaves, so that a few thousand points take two or three levels of carving. Leaders
    /// in the same ratio to the leaf size as the defaults.
    fn params() -> PartitionParams {
        PartitionParams {
            max_leaf_size: LEAF,
            min_leaf_size: LEAF / 4,
            leader_fraction: 10.0 / LEAF as f64,
            max_leaders: 100,
            fanout: vec![10, 3],
            max_depth: 16,
        }
    }

    struct Fixture {
        storage: VectorStorageEnum,
        quantized: Option<QuantizedVectors>,
        _dir: TempDir,
    }

    impl Fixture {
        fn new(
            vectors: &[Vec<VectorElementType>],
            distance: Distance,
            quantization: Option<QuantizationConfig>,
        ) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let mut storage = new_volatile_dense_vector_storage(vectors[0].len(), distance);
            for (id, vector) in vectors.iter().enumerate() {
                let vector = distance.preprocess_vector::<VectorElementType>(vector.clone());
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(&vector))
                    .unwrap();
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
                storage,
                quantized,
                _dir: dir,
            }
        }

        fn uniform(points: usize, dim: usize, distance: Distance) -> Self {
            Self::new(&uniform(points, dim, 42), distance, None)
        }

        fn scorer(&self) -> MatrixScorer<'_> {
            new_matrix_scorer(&self.storage, self.quantized.as_ref()).unwrap()
        }

        /// [`partition`], which must add all of its progress.
        fn partition(
            &self,
            points: Vec<PointOffsetType>,
            params: &PartitionParams,
            seed: u64,
        ) -> Vec<Vec<PointOffsetType>> {
            let counter = AtomicU64::new(0);
            let leaves = partition(
                &self.scorer(),
                points,
                params,
                seed,
                &AtomicBool::new(false),
                Progress::new(&counter, PROGRESS),
            )
            .unwrap();
            assert_eq!(counter.into_inner(), PROGRESS, "progress");
            leaves
        }
    }

    fn uniform(points: usize, dim: usize, seed: u64) -> Vec<Vec<VectorElementType>> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..points)
            .map(|_| (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect())
            .collect()
    }

    fn all(points: usize) -> Vec<PointOffsetType> {
        (0..points as PointOffsetType).collect()
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

    /// The guarantees of [`partition`], for leaves of `points`.
    #[track_caller]
    fn check_leaves(leaves: &[Vec<PointOffsetType>], points: &[PointOffsetType], max_len: usize) {
        let points: HashSet<_> = points.iter().copied().collect();
        let mut covered = HashSet::new();
        for (i, leaf) in leaves.iter().enumerate() {
            assert!(!leaf.is_empty(), "leaf {i} is empty");
            assert!(leaf.len() <= max_len, "leaf {i} has {} points", leaf.len());
            assert!(
                leaf.is_sorted_by(|a, b| a < b),
                "leaf {i} isn't sorted or repeats a point",
            );
            for point in leaf {
                assert!(points.contains(point), "leaf {i} has foreign point {point}");
                covered.insert(*point);
            }
        }
        assert_eq!(
            covered.len(),
            points.len(),
            "points missing from all leaves"
        );
    }

    /// In how many leaves each of the points `0..points` is.
    fn memberships(leaves: &[Vec<PointOffsetType>], points: usize) -> Vec<usize> {
        let mut count = vec![0; points];
        for &point in leaves.iter().flatten() {
            count[point as usize] += 1;
        }
        count
    }

    #[test]
    fn empty_input() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(10, 4, Distance::Euclid);
        assert!(fixture.partition(Vec::new(), &params(), 1).is_empty());
    }

    #[test]
    fn one_point() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(10, 4, Distance::Euclid);
        assert_eq!(fixture.partition(vec![7], &params(), 1), vec![vec![7]]);
    }

    /// Unsorted, with repeats: one leaf, sorted and deduplicated, without scoring.
    #[test]
    fn one_leaf() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(100, 4, Distance::Euclid);
        let mut points: Vec<_> = (0..LEAF as PointOffsetType).rev().collect();
        points.extend([3, 5, 3, 0]);
        let leaves = fixture.partition(points, &params(), 1);
        assert_eq!(leaves, vec![all(LEAF)]);
    }

    #[rstest]
    fn leaf_invariants(
        #[values(LEAF + 1, 500, 3000)] points: usize,
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(points, 16, distance);
        let leaves = fixture.partition(all(points), &params(), 1);
        check_leaves(&leaves, &all(points), LEAF);
        // About the product of the fanouts
        let memberships = memberships(&leaves, points);
        let mean = memberships.iter().sum::<usize>() as f64 / points as f64;
        assert!(mean >= 5.0, "{mean} leaves per point");
    }

    /// Scalar quantization, and binary quantization at a low dimension, whose scores tie a lot.
    #[rstest]
    fn leaf_invariants_quantized(
        #[values(scalar(), binary())] quantization: QuantizationConfig,
        #[values(Distance::Dot, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let points = 2000;
        let fixture = Fixture::new(&uniform(points, 16, 42), distance, Some(quantization));
        let leaves = fixture.partition(all(points), &params(), 1);
        check_leaves(&leaves, &all(points), LEAF);
    }

    /// Only some of the points, given in random order, with repeats.
    #[test]
    fn subset_of_the_storage() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(3000, 8, Distance::Cosine);
        let mut rng = StdRng::seed_from_u64(3);
        let points: Vec<PointOffsetType> = (0..2500).map(|_| rng.random_range(0..3000)).collect();
        let leaves = fixture.partition(points.clone(), &params(), 1);
        check_leaves(&leaves, &points, LEAF);

        let mut sorted = points;
        sorted.sort_unstable();
        assert_eq!(fixture.partition(sorted, &params(), 1), leaves);
    }

    #[test]
    fn deterministic_across_thread_pools() {
        let _scope = ambient::test_guard();
        let points = 3000;
        let fixture = Fixture::uniform(points, 16, Distance::Euclid);
        let leaves = |threads| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            pool.install(|| fixture.partition(all(points), &params(), 5))
        };
        let expected = leaves(1);
        check_leaves(&expected, &all(points), LEAF);
        for threads in [1, 2, 7] {
            assert_eq!(leaves(threads), expected, "{threads} threads");
        }
    }

    #[test]
    fn seeds_change_the_leaves() {
        let _scope = ambient::test_guard();
        let points = 3000;
        let fixture = Fixture::uniform(points, 16, Distance::Euclid);
        let leaves = fixture.partition(all(points), &params(), 1);
        let other = fixture.partition(all(points), &params(), 2);
        check_leaves(&other, &all(points), LEAF);
        assert_ne!(leaves, other);
    }

    /// Without carving, the leaves are as few and as even as possible, and disjoint.
    #[test]
    fn max_depth_zero() {
        let _scope = ambient::test_guard();
        let points = 1000;
        let fixture = Fixture::uniform(points, 8, Distance::Euclid);
        let params = PartitionParams {
            max_depth: 0,
            ..params()
        };
        let leaves = fixture.partition(all(points), &params, 1);
        check_leaves(&leaves, &all(points), LEAF);
        assert_eq!(leaves.len(), points.div_ceil(LEAF));
        assert!(
            leaves
                .iter()
                .all(|leaf| leaf.len().abs_diff(points / leaves.len()) <= 1)
        );
        assert!(memberships(&leaves, points).iter().all(|&count| count == 1));
        // Random, not in point order
        assert_ne!(leaves[0], all(leaves[0].len()));
    }

    /// One step of carving, then chunks for the clusters still too large.
    #[test]
    fn max_depth_one() {
        let _scope = ambient::test_guard();
        let points = 3000;
        let fixture = Fixture::uniform(points, 8, Distance::Euclid);
        let params = PartitionParams {
            max_depth: 1,
            ..params()
        };
        let leaves = fixture.partition(all(points), &params, 1);
        check_leaves(&leaves, &all(points), LEAF);
        // Top level fanout only
        assert!(
            memberships(&leaves, points)
                .iter()
                .all(|&count| count <= 10)
        );
    }

    /// No leaders can separate equal vectors. After two steps that keep every cluster whole,
    /// the clusters are cut into chunks, without carving them down to the maximum depth.
    #[test]
    fn duplicate_vectors() {
        let _scope = ambient::test_guard();
        let points = 400;
        let vectors = vec![vec![0.5; 8]; points];
        let fixture = Fixture::new(&vectors, Distance::Euclid, None);
        let leaves = fixture.partition(all(points), &params(), 1);
        check_leaves(&leaves, &all(points), LEAF);
        // Every point joins the same leaders: 10 copies of all points, 3 copies of each, and
        // each copy cut into chunks
        assert_eq!(leaves.len(), 10 * 3 * points.div_ceil(LEAF));
        assert!(
            memberships(&leaves, points)
                .iter()
                .all(|&count| count == 30)
        );
        // Stopped at depth 2 either way
        let shallow = PartitionParams {
            max_depth: 3,
            ..params()
        };
        assert_eq!(fixture.partition(all(points), &shallow, 1), leaves);
    }

    /// The duplicates are kept together, the other points are still carved.
    #[test]
    fn some_duplicate_vectors() {
        let _scope = ambient::test_guard();
        let points = 3000;
        let mut vectors = uniform(points, 8, 42);
        vectors[..1000].fill(vec![0.5; 8]);
        let fixture = Fixture::new(&vectors, Distance::Euclid, None);
        let leaves = fixture.partition(all(points), &params(), 1);
        check_leaves(&leaves, &all(points), LEAF);
    }

    /// Points whose every score is NaN are never picked, but still get leaves.
    #[test]
    fn nan_vectors() {
        let _scope = ambient::test_guard();
        let points = 2000;
        let mut vectors = uniform(points, 8, 42);
        for vector in vectors.iter_mut().step_by(7) {
            vector[0] = f32::NAN;
        }
        // All of them, more than a leaf
        let mut many = uniform(points, 8, 42);
        many[..300].fill(vec![f32::NAN; 8]);
        for vectors in [vectors, many] {
            let fixture = Fixture::new(&vectors, Distance::Dot, None);
            let leaves = fixture.partition(all(points), &params(), 1);
            check_leaves(&leaves, &all(points), LEAF);
        }
    }

    #[test]
    fn stopped() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(1000, 8, Distance::Euclid);
        let counter = AtomicU64::new(0);
        let result = partition(
            &fixture.scorer(),
            all(1000),
            &params(),
            1,
            &AtomicBool::new(true),
            Progress::new(&counter, PROGRESS),
        );
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
        assert_eq!(counter.into_inner(), 0);
    }

    /// Every step adds its share, also when the share is smaller than its pieces of work, for
    /// leaves, merged clusters, and the chunks of duplicates.
    #[rstest]
    fn progress_ends_at_its_share(
        #[values(0, 1, 7, 1_000_003)] units: u64,
        #[values(false, true)] duplicates: bool,
    ) {
        let _scope = ambient::test_guard();
        let points = 3000;
        let mut vectors = uniform(points, 8, 42);
        if duplicates {
            vectors[..1000].fill(vec![0.5; 8]);
        }
        let fixture = Fixture::new(&vectors, Distance::Euclid, None);
        let counter = AtomicU64::new(0);
        let leaves = partition(
            &fixture.scorer(),
            all(points),
            &params(),
            1,
            &AtomicBool::new(false),
            Progress::new(&counter, units),
        )
        .unwrap();
        check_leaves(&leaves, &all(points), LEAF);
        assert_eq!(counter.into_inner(), units);
    }

    #[test]
    fn no_room_in_leaves() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(10, 4, Distance::Euclid);
        let params = PartitionParams {
            max_leaf_size: 0,
            ..params()
        };
        let result = partition(
            &fixture.scorer(),
            all(10),
            &params,
            1,
            &AtomicBool::new(false),
            Progress::new(&AtomicU64::new(0), PROGRESS),
        );
        assert!(result.is_err());
    }

    /// Parameters out of their sensible ranges still give valid leaves.
    #[rstest]
    #[case::no_fanout(PartitionParams { fanout: vec![], ..params() })]
    #[case::zero_fanout(PartitionParams { fanout: vec![0, 0], ..params() })]
    #[case::fanout_above_leaders(PartitionParams { fanout: vec![500], ..params() })]
    #[case::huge_fanout(PartitionParams { fanout: vec![usize::MAX], ..params() })]
    #[case::no_merging(PartitionParams { min_leaf_size: 0, ..params() })]
    #[case::merge_everything(PartitionParams { min_leaf_size: 10 * LEAF, ..params() })]
    #[case::no_leaders(PartitionParams { max_leaders: 0, ..params() })]
    #[case::all_leaders(PartitionParams { leader_fraction: 1.0, max_leaders: usize::MAX, ..params() })]
    #[case::zero_fraction(PartitionParams { leader_fraction: 0.0, ..params() })]
    #[case::nan_fraction(PartitionParams { leader_fraction: f64::NAN, ..params() })]
    #[case::leaves_of_one(PartitionParams { max_leaf_size: 1, min_leaf_size: 1, ..params() })]
    fn odd_params(#[case] params: PartitionParams) {
        let _scope = ambient::test_guard();
        let points = 600;
        let fixture = Fixture::uniform(points, 8, Distance::Euclid);
        let leaves = fixture.partition(all(points), &params, 1);
        check_leaves(&leaves, &all(points), params.max_leaf_size);
    }

    /// The top-k buffers have room for the largest fanout, but not for more leaders than a step
    /// can have.
    #[test]
    fn top_k_size_is_clamped_to_the_leaders() {
        let with = |fanout: Vec<usize>, max_leaders| PartitionParams {
            max_leaders,
            fanout,
            ..params()
        };
        assert_eq!(top_k_size(&with(vec![10, 3], 1000), 100_000), 10);
        assert_eq!(top_k_size(&with(vec![3, 10], 1000), 100_000), 10);
        assert_eq!(top_k_size(&with(vec![100_000], 1000), 100_000), 1000);
        assert_eq!(top_k_size(&with(vec![usize::MAX], usize::MAX), 600), 600);
        // `leader_count` gives at least 2 leaders, and fanout 0 or none still picks 1
        assert_eq!(top_k_size(&with(vec![10], 0), 100_000), 2);
        assert_eq!(top_k_size(&with(vec![0, 0], 1000), 100_000), 1);
        assert_eq!(top_k_size(&with(vec![], 1000), 100_000), 1);
    }

    #[test]
    fn merging() {
        let mut rng = StdRng::seed_from_u64(1);
        let small: Vec<Vec<PointOffsetType>> = (0..50)
            .map(|i| {
                let len = rng.random_range(0..LEAF / 2);
                // Overlapping clusters, as fanout gives
                (0..len as PointOffsetType).map(|p| p * 3 + i).collect()
            })
            .collect();
        let expected: HashSet<_> = small.iter().flatten().copied().collect();
        let total: usize = small.iter().map(Vec::len).sum();

        let groups: Vec<_> = merge_small(small, LEAF, &mut rng).collect();
        let points: Vec<_> = expected.iter().copied().collect();
        check_leaves(&groups, &points, LEAF);
        // Greedy: two groups in a row never fit into one
        assert!(groups.len() <= 2 * total.div_ceil(LEAF));

        assert_eq!(merge_small(vec![vec![], vec![]], LEAF, &mut rng).count(), 0);
    }

    #[test]
    fn chunks() {
        let chunks = random_chunks(all(130), LEAF, 1);
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            [44, 43, 43]
        );
        check_leaves(&chunks, &all(130), LEAF);
        assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 130);
        assert_eq!(random_chunks(all(LEAF), LEAF, 1), vec![all(LEAF)]);
        assert!(random_chunks(Vec::new(), LEAF, 1).is_empty());
    }

    #[test]
    fn stall_counting() {
        // A step that shrinks resets the count
        assert_eq!(stalls_after(1, 1000, 899), 0);
        assert_eq!(stalls_after(0, 1000, 900), 1);
        assert_eq!(stalls_after(1, 1000, 1000), 2);
    }

    /// Carving keeps close points together: most points share a leaf with most of their 10
    /// nearest neighbors, by the same scores. Random leaves of the same sizes would give far
    /// fewer.
    #[rstest]
    fn nearest_neighbors_share_a_leaf(
        #[values(Distance::Cosine, Distance::Euclid)] distance: Distance,
        #[values(None, Some(scalar()))] quantization: Option<QuantizationConfig>,
    ) {
        let _scope = ambient::test_guard();
        let (points, k) = (4000, 10);
        let fixture = Fixture::new(&uniform(points, 16, 42), distance, quantization);
        let leaves = fixture.partition(all(points), &params(), 1);
        check_leaves(&leaves, &all(points), LEAF);

        let mut nearest = MatrixTopK::new(k);
        let ids = all(points);
        fixture
            .scorer()
            .top_k(&ids, MatrixCols::Rows, &mut nearest)
            .unwrap();
        let mut leaves_of = vec![Vec::new(); points];
        for (i, leaf) in leaves.iter().enumerate() {
            for &point in leaf {
                leaves_of[point as usize].push(i);
            }
        }
        let shared = (0..points)
            .map(|point| {
                let (neighbors, _) = nearest.row(point);
                neighbors
                    .iter()
                    .filter(|neighbor| {
                        leaves_of[point]
                            .iter()
                            .any(|&leaf| leaves[leaf].binary_search(neighbor).is_ok())
                    })
                    .count()
            })
            .sum::<usize>() as f64
            / (points * k) as f64;

        // The chance that one of a point's leaves has a given other point, were they random
        let random = leaves_of
            .iter()
            .map(|of| {
                let miss: f64 = of
                    .iter()
                    .map(|&leaf| 1.0 - (leaves[leaf].len() - 1) as f64 / (points - 1) as f64)
                    .product();
                1.0 - miss
            })
            .sum::<f64>()
            / points as f64;
        assert!(
            shared >= 0.95 && random < 0.4,
            "shared {shared}, random {random}"
        );
    }
}
