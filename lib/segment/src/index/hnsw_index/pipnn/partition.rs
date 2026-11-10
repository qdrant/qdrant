//! Randomized ball carving: splits the points of one level into overlapping leaves.
//!
//! - Samples leaders from the points, at most a fixed number per step, and assigns every point
//!   to its `fanout` nearest leaders through a matrix call against the leaders.
//! - Recurses into every cluster larger than the leaf size, with the next level's fanout.
//! - Merges small clusters at random, never above the leaf size.
//! - Sorts and deduplicates every merged group: with fanout above 1, a point can land in two
//!   clusters that get merged, and leaf scoring requires distinct rows.
//! - Cuts a cluster into random chunks if carving doesn't shrink it: past the maximum depth, or
//!   when two steps in a row kept almost all of its points together. Points whose scores are all
//!   NaN get no leader, and are not carved any further.
//!
//! # Stalls
//!
//! The paper's ball carving (Algorithm 5) recurses on every cluster above the leaf size, and its
//! analysis (appendix A.7) assumes roughly even splits. A step stalls when a child that would be
//! carved again keeps at least [`STALL_TENTHS`] tenths of its parent's points.
//!
//! Stalls have two causes:
//! - A fanout close to the number of leaders. A child keeps about `fanout / leaders` of its
//!   parent's points, and a central leader more. With the defaults, that's a cluster of about
//!   1,000 to 1,600 points at depth 0, with 11 to 16 leaders and a fanout of 10, on data whose
//!   vectors share a common direction, as embeddings do. Its children overlap heavily, and each
//!   is carved once more with the next, smaller fanout, which separates them into local leaves:
//!   about 20 per point there. Cutting each stalled child into chunks instead would give about
//!   10 random leaves per point, and cutting the whole cluster into chunks 1.
//! - Score ties that break the same way for every sample of leaders: duplicate vectors, or
//!   quantized codes that tie. No step separates them, so a child that stalls a second time in a
//!   row is cut into random chunks ([`MAX_STALLS`]). Its `fanout` copies are each carved and cut
//!   separately, which costs a few times the partitioning of those points. The build groups exact
//!   duplicates before partitioning (`duplicates.rs`), which leaves quantized ties.
//!
//! Randomness comes from seeds derived from the build's rng, so a build is reproducible. Each
//! step draws its leaders and its children's seeds from its own seed, and the picks of a point
//! don't depend on the other points of its matrix call, so the leaves don't depend on the number
//! of threads either.
//!
//! Progress: partitioning adds the points of the level as point-equivalents of its work, handed
//! out as shares, see [`progress`](super::progress). A step must hand shares to its clusters
//! before it knows their work, so a step that carves keeps half of its share for its matrix
//! calls, adding it per call, and hands the other half to its clusters by size. A cluster that
//! becomes a leaf, or is cut into chunks, adds its share at once; one that is carved again splits
//! it the same way. The top step of a large level and the steps below it take about the same
//! time.

use std::num::NonZeroU16;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::reason::reason;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::{RngExt as _, SeedableRng as _};
use rayon::prelude::*;

use super::progress::Share;
use super::top_k_pool::TopKPool;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::vector_storage::matrix_scorer::{MatrixCols, MatrixScorer};

/// Points per matrix call against the leaders. Every call packs the leaders again, so smaller
/// calls would pay that more often.
const POINTS_PER_CALL: usize = 1024;

/// Most leaders a step can have, so that a leader's position plus 1 fits into the
/// `NonZeroU16` slots of [`Carving::nearest_leaders`].
const MAX_STEP_LEADERS: usize = u16::MAX as usize;

/// A child cluster that kept at least this many tenths of its parent's points didn't shrink, see
/// [Stalls](self#stalls).
const STALL_TENTHS: usize = 9;

/// Steps in a row without shrinking, after which a cluster is cut into random chunks, see
/// [Stalls](self#stalls).
const MAX_STALLS: usize = 2;

/// Default [`PartitionParams::max_leaf_size`]: the paper's `C_max` is typically 1024 to 2048
/// (section 4.1).
const DEFAULT_MAX_LEAF_SIZE: usize = 1024;

/// Default [`PartitionParams::min_leaf_size`]: a quarter of the leaf size. The paper names a
/// `C_min` but gives no value.
const DEFAULT_MIN_LEAF_SIZE: usize = DEFAULT_MAX_LEAF_SIZE / 4;

/// Default [`PartitionParams::leader_fraction`]: the paper's `P_samp`, for which it gives no
/// value. With the default fanout, a top-level child gets about 1000 points, close to the leaf
/// size.
const DEFAULT_LEADER_FRACTION: f64 = 0.01;

/// Default [`PartitionParams::max_leaders`]: the paper's hard cap, "typically 1000" (section 4.1).
const DEFAULT_MAX_LEADERS: usize = 1000;

/// Default [`PartitionParams::fanout`]: the paper's multi-level fanout, "10 on the top level, 3 on
/// the second level" (section 4.1).
const DEFAULT_FANOUT: [usize; 2] = [10, 3];

/// Default [`PartitionParams::max_depth`]: not in the paper, whose recursion has no limit. Carving
/// with the default parameters ends after 2 to 3 levels, so this only stops degenerate input.
const DEFAULT_MAX_DEPTH: usize = 16;

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
    /// At most this many leaders per step, at least 2, and at most 65,535.
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
            max_leaf_size: DEFAULT_MAX_LEAF_SIZE,
            min_leaf_size: DEFAULT_MIN_LEAF_SIZE,
            leader_fraction: DEFAULT_LEADER_FRACTION,
            max_leaders: DEFAULT_MAX_LEADERS,
            fanout: DEFAULT_FANOUT.to_vec(),
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }
}

/// Splits `points` into overlapping leaves by randomized ball carving, on the current rayon
/// pool.
///
/// `points` must be sorted and distinct, as the points of a level are; only debug builds check
/// it.
///
/// Leaves: each sorted ascending, without duplicates, at most `max_leaf_size` points; every input
/// point in at least one leaf. Deterministic for the same input and seed, whatever the number of
/// threads.
///
/// Adds all of `progress` by the time it returns the leaves, per step, see the module docs.
///
/// Memory: one step holds `fanout` leader slots per point of its cluster, 20 bytes per point at
/// the top level with a fanout of 10, and then the clusters themselves, 40 bytes per point. The
/// leaves hold about 4 bytes per point and leaf it's in, typically 30.
pub(super) fn partition(
    scorer: &MatrixScorer<'_>,
    points: Vec<PointOffsetType>,
    params: &PartitionParams,
    seed: u64,
    stopped: &AtomicBool,
    progress: Share<'_>,
) -> OperationResult<Vec<Vec<PointOffsetType>>> {
    // Every step relies on it
    debug_assert!(
        points.is_sorted_by(|a, b| a < b),
        "points must be sorted and distinct",
    );
    if params.max_leaf_size == 0 {
        return Err(OperationError::service_error(
            "PiPNN leaves must have room for at least one point",
        ));
    }
    check_process_stopped(stopped)?;

    if points.is_empty() {
        progress.add();
        return Ok(Vec::new());
    }

    let carving = Carving {
        scorer,
        params,
        stopped,
        top_k: TopKPool::new(top_k_size(params, points.len())),
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
    /// Buffers for the matrix calls of all steps, with `k` from [`top_k_size`]. One pool for all
    /// steps, not one per step: there are thousands of them. Not one per fanout either, which
    /// would hold a buffer per thread and fanout; steps with a smaller fanout read the first
    /// picks of each row, at a slightly higher cost of the top-k sink.
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
            Next::Leaf(points)
            | Next::Split {
                points,
                seed: _,
                stalls: _,
            }
            | Next::Chunks { points, seed: _ } => points.len(),
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
        progress: Share<'_>,
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
                    let stalls = if stalled(parent_len, cluster.len()) {
                        stalls + 1
                    } else {
                        0
                    };
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
    /// `min(P_samp · |P|, cap)`, and at least 2. The cap is `max_leaders`, at most
    /// [`MAX_STEP_LEADERS`].
    fn leader_count(&self, len: usize) -> usize {
        // Saturates, so a NaN or negative fraction gives 0
        let wanted = (len as f64 * self.params.leader_fraction).ceil() as usize;
        let max = self.params.max_leaders.clamp(2, MAX_STEP_LEADERS);
        wanted.clamp(2, max).min(len)
    }

    /// The `fanout` nearest leaders of each point, as positions in `leaders`: `fanout` slots
    /// per point, best first, the unused ones `None`. Adds `progress` per matrix call,
    /// by its points.
    fn nearest_leaders(
        &self,
        points: &[PointOffsetType],
        leaders: &[PointOffsetType],
        fanout: usize,
        progress: Share<'_>,
    ) -> OperationResult<Vec<Option<NonZeroU16>>> {
        // Points with NaN scores get fewer picks
        let mut nearest = vec![None; points.len() * fanout];
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
                        for (slot, &col) in slots.iter_mut().zip(cols) {
                            *slot = Some(leader_slot(col));
                        }
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
    nearest: &[Option<NonZeroU16>],
    fanout: usize,
    leaders: usize,
) -> (Vec<Vec<PointOffsetType>>, Vec<PointOffsetType>) {
    // Counted first, so every cluster is allocated once, at its size
    let mut sizes = vec![0; leaders];
    for &slot in nearest.iter().flatten() {
        sizes[leader_position(slot)] += 1;
    }
    let mut clusters: Vec<Vec<_>> = sizes.into_iter().map(Vec::with_capacity).collect();
    let mut orphans = Vec::new();
    for (&point, slots) in points.iter().zip(nearest.chunks_exact(fanout)) {
        let mut assigned = false;
        // Slots fill from the front
        for slot in slots.iter().map_while(|&slot| slot) {
            clusters[leader_position(slot)].push(point);
            assigned = true;
        }
        if !assigned {
            orphans.push(point);
        }
    }
    (clusters, orphans)
}

/// The slot of the leader at `position` among a step's leaders: the position plus 1, which fits
/// since a step has at most [`MAX_STEP_LEADERS`] leaders. 2 bytes per slot, where an
/// `Option<u32>` would take 8.
fn leader_slot(position: u32) -> NonZeroU16 {
    u16::try_from(position + 1)
        .ok()
        .and_then(NonZeroU16::new)
        .expect("a step has at most `MAX_STEP_LEADERS` leaders")
}

/// The position among a step's leaders of the leader in `slot`, see [`leader_slot`].
fn leader_position(slot: NonZeroU16) -> usize {
    usize::from(slot.get()) - 1
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

/// Whether a child of `child_len` of the `parent_len` points of a step didn't shrink, see
/// [Stalls](self#stalls).
fn stalled(parent_len: usize, child_len: usize) -> bool {
    child_len * 10 >= parent_len * STALL_TENTHS
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
    let leaders = params.max_leaders.clamp(2, MAX_STEP_LEADERS).min(len);
    let fanout = params.fanout.iter().copied().max().unwrap_or(1);
    fanout.min(leaders).max(1)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::index::hnsw_index::pipnn::progress::TestCounter;
    use crate::types::{
        BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
        QuantizationConfig, ScalarQuantizationConfig,
    };
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::matrix_scorer::{MatrixTopK, new_matrix_scorer};
    use crate::vector_storage::quantized::quantized_vectors::{
        QuantizedVectors, QuantizedVectorsStorageType,
    };
    use crate::vector_storage::{VectorStorage as _, VectorStorageEnum};

    const LEAF: usize = 64;
    /// Points of a call's share, more than it has pieces of work.
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
            let counter = TestCounter::default();
            let leaves = partition(
                &self.scorer(),
                points,
                params,
                seed,
                &AtomicBool::new(false),
                counter.share(PROGRESS),
            )
            .unwrap();
            assert_eq!(counter.load(), PROGRESS, "progress");
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

    /// One leaf, without scoring.
    #[test]
    fn one_leaf() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(100, 4, Distance::Euclid);
        let leaves = fixture.partition(all(LEAF), &params(), 1);
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

    /// Only some of the points, with gaps between their ids.
    #[test]
    fn subset_of_the_storage() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(3000, 8, Distance::Cosine);
        let mut rng = StdRng::seed_from_u64(3);
        let points: Vec<PointOffsetType> = (0..3000).filter(|_| rng.random_bool(0.8)).collect();
        let leaves = fixture.partition(points.clone(), &params(), 1);
        check_leaves(&leaves, &points, LEAF);
    }

    /// Unsorted input breaks the contract, which debug builds check.
    #[test]
    #[should_panic(expected = "points must be sorted and distinct")]
    #[cfg(debug_assertions)]
    fn unsorted_points() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::uniform(100, 4, Distance::Euclid);
        fixture.partition(vec![3, 1, 2], &params(), 1);
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

    /// Slightly above a leaf, the top step has hardly more leaders than its fanout, and on
    /// vectors that share a direction a central leader keeps nearly all points (both cases stall
    /// at depth 0). The stalled children are carved once more with the next fanout, about 21
    /// leaves per point here. Cutting each of them into chunks would give 10, and cutting the
    /// whole cluster 1.
    #[rstest]
    fn stall_from_a_large_fanout(#[values(LEAF + 1, LEAF * 5 / 4)] points: usize) {
        let _scope = ambient::test_guard();
        let mut rng = StdRng::seed_from_u64(7);
        let vectors: Vec<Vec<VectorElementType>> = (0..points)
            .map(|_| {
                (0..16)
                    .map(|i| {
                        let offset = if i == 0 { 3.0 } else { 0.0 };
                        offset + rng.random_range(-1.0..1.0) / (i + 1) as f32
                    })
                    .collect()
            })
            .collect();
        let fixture = Fixture::new(&vectors, Distance::Euclid, None);
        let leaves = fixture.partition(all(points), &params(), 1);
        check_leaves(&leaves, &all(points), LEAF);
        let memberships = memberships(&leaves, points);
        let mean = memberships.iter().sum::<usize>() as f64 / points as f64;
        assert!(mean >= 15.0, "{mean} leaves per point");
    }

    /// Steps that can never shrink a cluster stop after two of them, whatever the depth limit,
    /// instead of copying all points `fanout` times per level.
    #[rstest]
    fn steps_that_cannot_shrink(#[values(3, 16)] max_depth: usize) {
        let _scope = ambient::test_guard();
        let points = 600;
        let fixture = Fixture::uniform(points, 8, Distance::Euclid);
        let params = PartitionParams {
            max_leaders: 2,
            fanout: vec![2; 16],
            max_depth,
            ..params()
        };
        let leaves = fixture.partition(all(points), &params, 1);
        check_leaves(&leaves, &all(points), LEAF);
        // Every child is the whole cluster: 2 copies at each of the 2 steps
        assert!(memberships(&leaves, points).iter().all(|&count| count == 4));
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
        let counter = TestCounter::default();
        let result = partition(
            &fixture.scorer(),
            all(1000),
            &params(),
            1,
            &AtomicBool::new(true),
            counter.share(PROGRESS),
        );
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
        assert_eq!(counter.load(), 0);
    }

    /// Every step adds its share, also when the share is smaller than its pieces of work, for
    /// leaves, merged clusters, and the chunks of duplicates.
    #[rstest]
    fn progress_ends_at_its_share(
        #[values(0, 1, 7, 1_000_003)] share: u64,
        #[values(false, true)] duplicates: bool,
    ) {
        let _scope = ambient::test_guard();
        let points = 3000;
        let mut vectors = uniform(points, 8, 42);
        if duplicates {
            vectors[..1000].fill(vec![0.5; 8]);
        }
        let fixture = Fixture::new(&vectors, Distance::Euclid, None);
        let counter = TestCounter::default();
        let leaves = partition(
            &fixture.scorer(),
            all(points),
            &params(),
            1,
            &AtomicBool::new(false),
            counter.share(share),
        )
        .unwrap();
        check_leaves(&leaves, &all(points), LEAF);
        assert_eq!(counter.load(), share);
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
            TestCounter::default().share(PROGRESS),
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
        // Leader positions must fit into the slots
        assert_eq!(
            top_k_size(&with(vec![usize::MAX], usize::MAX), 100_000),
            MAX_STEP_LEADERS
        );
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
    fn leader_slots() {
        assert_eq!(size_of::<Option<NonZeroU16>>(), 2);
        let last = (MAX_STEP_LEADERS - 1) as u32;
        for position in [0, 1, 999, last] {
            assert_eq!(leader_position(leader_slot(position)), position as usize);
        }
    }

    #[test]
    fn stall_threshold() {
        assert!(!stalled(1000, 899));
        assert!(stalled(1000, 900));
        assert!(stalled(1000, 1000));
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
