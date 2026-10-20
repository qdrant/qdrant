//! Leaf k-NN: candidate edges from the nearest neighbors of every point within its leaf.
//!
//! One matrix call per leaf scores all pairs of its points (one triangle), and each point's `k`
//! best become candidate edges in both directions: the bidirected k-NN graph of the leaf. The
//! paper's ablation (appendix A.3) found it better than the directed or the inverted k-NN graph
//! alone and than RobustPrune within the leaf, and at least as good as a degree-limited minimum
//! spanning tree, with `k` to tune its density.
//!
//! Each directed edge is emitted once per leaf, also where two points pick each other. HashPrune
//! would keep the same reservoirs with the duplicate, but every edge costs it a hash, a lock, and
//! likely a cache miss on a reservoir, while finding a mutual pick only takes a look at the
//! neighbor's `k` picks, which are in cache. The edge then also carries the score of its own
//! direction. Leaves that share points still send the same edge once each.

use std::sync::atomic::AtomicBool;

use common::ambient;
use common::reason::reason;
use common::types::{PointOffsetType, ScoreType};
use parking_lot::Mutex;
use rayon::prelude::*;

use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::vector_storage::matrix_scorer::{MatrixCols, MatrixScorer, MatrixTopK};

/// Calls `edge(p, q, score)` for every edge `p → q` of the bidirected k-NN graph of each leaf:
/// from every point of the leaf to each of its `k` best neighbors in the leaf, by
/// `score_internal` and excluding the point itself, and back.
///
/// - Each directed edge comes once per leaf, also if both points picked each other. Leaves that
///   share points send the same edge once each.
/// - `score` is `score_internal(p, q)` if `p` picked `q`, otherwise the score of the pick
///   `q → p`. The two differ at most by rounding.
/// - A leaf must not repeat a point. Ties go to the lower position in the leaf, which is the lower
///   id in a sorted leaf. Leaves with fewer than 2 points have no edges.
/// - Runs in parallel over the leaves, in the current rayon pool: `edge` is called from many
///   threads at once, in no particular order.
pub(super) fn for_each_leaf_edge(
    scorer: &MatrixScorer<'_>,
    leaves: &[Vec<PointOffsetType>],
    k: usize,
    stopped: &AtomicBool,
    edge: impl Fn(PointOffsetType, PointOffsetType, ScoreType) + Sync,
) -> OperationResult<()> {
    // A point has at most `len - 1` neighbors in its leaf, so a larger `k` would only size the
    // buffers of every matrix call
    let max_leaf = leaves.iter().map(Vec::len).max().unwrap_or(0);
    let k = k.min(max_leaf.saturating_sub(1));
    if k == 0 {
        return Ok(());
    }

    // Buffers of the matrix calls, reused across leaves: with large vectors, a call keeps
    // megabytes of packed operands in them. One per leaf in progress, so about one per thread.
    let spare_tops = Mutex::new(Vec::new());
    leaves
        .par_iter()
        // Every leaf its own job, stealable until the end: a leaf takes up to milliseconds, sizes
        // differ, and rayon's splitting can leave one thread with a long run of them otherwise,
        // see `HNSW_BUILD_MAX_PAR_LEN`
        .with_max_len(1)
        .try_for_each(|leaf| {
            check_process_stopped(stopped)?;
            if leaf.len() < 2 {
                return Ok(());
            }
            let _scope = ambient::unmeasured_guard(reason("Internal operation"));
            let mut top = spare_tops
                .lock()
                .pop()
                .unwrap_or_else(|| MatrixTopK::new(k));
            let result = leaf_edges(scorer, leaf, &mut top, &edge);
            spare_tops.lock().push(top);
            result
        })
}

/// [`for_each_leaf_edge`] for one leaf of at least 2 points, with `top` for the matrix call.
fn leaf_edges(
    scorer: &MatrixScorer<'_>,
    leaf: &[PointOffsetType],
    top: &mut MatrixTopK,
    edge: &impl Fn(PointOffsetType, PointOffsetType, ScoreType),
) -> OperationResult<()> {
    scorer.top_k(leaf, MatrixCols::Rows, top)?;
    for (r, &point) in leaf.iter().enumerate() {
        let (cols, scores) = top.row(r);
        for (&c, &score) in std::iter::zip(cols, scores) {
            let neighbor = leaf[c as usize];
            edge(point, neighbor, score);
            // A mutual pick sends the reverse edge from the neighbor's own row, with its score
            let mutual = top.row(c as usize).0.contains(&(r as u32));
            if !mutual {
                edge(neighbor, point, score);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use common::bitvec::BitVec;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom as _;
    use rand::{RngExt as _, SeedableRng as _};
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;
    use crate::common::operation_error::OperationError;
    use crate::data_types::vectors::{VectorElementType, VectorRef};
    use crate::index::hnsw_index::point_scorer::FilteredScorer;
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

    const POINTS: usize = 300;
    const DIM: usize = 100;
    /// The prototype's leaf k.
    const K: usize = 4;
    /// Uneven leaf sizes, including leaves without pairs.
    const SIZES: &[usize] = &[150, 0, 1, 2, 3, 5, 200, 17, 64, 2, 120, 1, 90, 33];

    type Edge = (PointOffsetType, PointOffsetType, ScoreType);

    #[derive(Clone, Copy, Debug)]
    enum Data {
        /// Uniform in `[-1, 1)`: the kernels round differently than `score_internal`.
        Random,
        /// Small integers, or for Cosine 16 entries of ±1, which normalize to ±0.25: every
        /// product and sum is exact in f32, so the kernels score exactly as `score_internal`,
        /// with many ties.
        Exact,
    }

    struct Fixture {
        storage: VectorStorageEnum,
        quantized: Option<QuantizedVectors>,
        deleted: BitVec,
        _dir: TempDir,
    }

    impl Fixture {
        fn new(data: Data, distance: Distance, quantization: Option<QuantizationConfig>) -> Self {
            let mut rng = StdRng::seed_from_u64(42);
            let mut storage = new_volatile_dense_vector_storage(DIM, distance);
            for id in 0..POINTS as PointOffsetType {
                let vector: Vec<VectorElementType> = match data {
                    Data::Random => (0..DIM).map(|_| rng.random_range(-1.0..1.0)).collect(),
                    Data::Exact => match distance {
                        Distance::Cosine => {
                            let mut dims: Vec<usize> = (0..DIM).collect();
                            dims.shuffle(&mut rng);
                            let mut vector = vec![0.0; DIM];
                            for &dim in &dims[..16] {
                                vector[dim] = if rng.random::<bool>() { 1.0 } else { -1.0 };
                            }
                            vector
                        }
                        Distance::Dot | Distance::Euclid | Distance::Manhattan => (0..DIM)
                            .map(|_| f32::from(rng.random_range(-8i8..=8)))
                            .collect(),
                    },
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
                _dir: dir,
            }
        }

        fn scorer(&self) -> MatrixScorer<'_> {
            new_matrix_scorer(&self.storage, self.quantized.as_ref())
                .expect("storage has matrix operands")
        }

        /// The scorer of HNSW insertion, which picks quantized or original vectors alike.
        fn reference(&self) -> FilteredScorer<'_> {
            FilteredScorer::new_internal(
                0,
                &self.storage,
                self.quantized.as_ref(),
                None,
                &self.deleted,
            )
            .unwrap()
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

    /// Leaves of these sizes from random points, sorted. Sizes add up to more than `POINTS`, so
    /// they overlap.
    fn random_leaves(sizes: &[usize], seed: u64) -> Vec<Vec<PointOffsetType>> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut ids: Vec<PointOffsetType> = (0..POINTS as PointOffsetType).collect();
        sizes
            .iter()
            .map(|&size| {
                ids.shuffle(&mut rng);
                let mut leaf = ids[..size].to_vec();
                leaf.sort_unstable();
                leaf
            })
            .collect()
    }

    fn sorted(mut edges: Vec<Edge>) -> Vec<Edge> {
        edges.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)).then(a.2.total_cmp(&b.2)));
        edges
    }

    /// Runs [`for_each_leaf_edge`] in the current pool, and returns its edges, sorted.
    fn collect_edges(
        scorer: &MatrixScorer<'_>,
        leaves: &[Vec<PointOffsetType>],
        k: usize,
        stopped: &AtomicBool,
    ) -> (OperationResult<()>, Vec<Edge>) {
        let edges = Mutex::new(Vec::new());
        let result = for_each_leaf_edge(scorer, leaves, k, stopped, |p, q, score| {
            edges.lock().push((p, q, score));
        });
        (result, sorted(edges.into_inner()))
    }

    /// For each position of `leaf`, all other positions with their scores by `score_internal`,
    /// best first, ties to the lower position.
    fn ranked(reference: &FilteredScorer<'_>, leaf: &[PointOffsetType]) -> Vec<Vec<(usize, f32)>> {
        leaf.iter()
            .enumerate()
            .map(|(r, &point)| {
                let mut scores: Vec<(usize, f32)> = leaf
                    .iter()
                    .enumerate()
                    .filter(|&(c, _)| c != r)
                    .map(|(c, &other)| (c, reference.score_internal(point, other)))
                    .collect();
                // `partial_cmp`, as the kernels compare: -0 ties with 0
                scores.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("no NaN").then(a.0.cmp(&b.0)));
                scores
            })
            .collect()
    }

    /// The edges of each leaf's bidirected k-NN graph by brute force over `score_internal`: each
    /// directed edge once per leaf, with the score of its own pick, else of the reverse one.
    fn expected_edges(
        reference: &FilteredScorer<'_>,
        leaves: &[Vec<PointOffsetType>],
        k: usize,
    ) -> Vec<Edge> {
        let mut edges = Vec::new();
        for leaf in leaves {
            let mut leaf_edges = HashMap::new();
            for (r, ranked) in ranked(reference, leaf).iter().enumerate() {
                for &(c, score) in ranked.iter().take(k) {
                    leaf_edges.insert((leaf[r], leaf[c]), score);
                    leaf_edges.entry((leaf[c], leaf[r])).or_insert(score);
                }
            }
            edges.extend(leaf_edges.into_iter().map(|((p, q), score)| (p, q, score)));
        }
        sorted(edges)
    }

    /// Equal sorted edges, reporting the first difference instead of both lists.
    fn assert_same_edges(got: &[Edge], want: &[Edge]) {
        if let Some(i) = (0..got.len().min(want.len())).find(|&i| got[i] != want[i]) {
            panic!("edge {i}: got {:?}, want {:?}", got[i], want[i]);
        }
        assert_eq!(got.len(), want.len(), "number of edges");
        assert!(got.iter().all(|&(p, q, _)| p != q), "edge to itself");
    }

    /// The same edges and scores as brute force, for kernels that score exactly.
    fn check_exact(fixture: &Fixture, leaves: &[Vec<PointOffsetType>], k: usize) {
        let (result, got) = collect_edges(&fixture.scorer(), leaves, k, &AtomicBool::new(false));
        result.unwrap();
        assert_same_edges(&got, &expected_edges(&fixture.reference(), leaves, k));
    }

    /// Brute force with a tolerance, relative to the score and at least 1, for kernels that round
    /// differently than `score_internal`. Near ties may then swap picks, so each leaf runs alone
    /// and, instead of the exact edges:
    /// - every edge goes to a point at most `tolerance` below the sender's `k`-th best score, or
    ///   comes from one that has the sender within that,
    /// - its score is within `tolerance` of `score_internal`,
    /// - every point has edges to and from the points it ranks above its `k + 1`-th best by more
    ///   than `tolerance`,
    /// - each directed edge comes once, and with its reverse.
    fn check_with_tolerance(
        fixture: &Fixture,
        leaves: &[Vec<PointOffsetType>],
        k: usize,
        tolerance: f32,
    ) {
        let scorer = fixture.scorer();
        let reference = fixture.reference();
        let allowed = |score: f32| tolerance * score.abs().max(1.0);
        for leaf in leaves {
            let (result, got) = collect_edges(
                &scorer,
                std::slice::from_ref(leaf),
                k,
                &AtomicBool::new(false),
            );
            result.unwrap();
            let ranked = ranked(&reference, leaf);
            let score = |r: usize, c: usize| ranked[r].iter().find(|&&(o, _)| o == c).unwrap().1;
            // Leaves sorted, ids distinct
            let position = |id| leaf.binary_search(&id).unwrap();
            let near = |r: usize, c: usize| {
                ranked[r]
                    .get(k - 1)
                    .is_none_or(|&(_, kth)| score(r, c) >= kth - allowed(kth))
            };

            let mut edges = HashSet::new();
            for &(p, q, got_score) in &got {
                let (r, c) = (position(p), position(q));
                assert_ne!(r, c, "edge to itself");
                assert!(edges.insert((r, c)), "edge {p} → {q} came twice");
                assert!(near(r, c) || near(c, r), "edge {p} → {q} isn't a pick");
                let want = score(r, c);
                assert!(
                    (got_score - want).abs() <= allowed(want),
                    "edge {p} → {q}: {got_score} vs {want}",
                );
            }
            for &(r, c) in &edges {
                assert!(edges.contains(&(c, r)), "no reverse of {r} → {c}");
            }
            for (r, row) in ranked.iter().enumerate() {
                let next = row.get(k).map(|&(_, next)| next + allowed(next));
                let certain = |&&(_, score): &&(usize, f32)| next.is_none_or(|next| score > next);
                for &(c, _) in row.iter().take_while(certain) {
                    assert!(edges.contains(&(r, c)), "{r} doesn't pick {c}");
                }
            }
        }
    }

    /// Integer data scores exactly in any summation order: the same edges and scores as brute
    /// force over overlapping leaves of uneven sizes.
    #[rstest]
    fn original_vectors(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
        #[values(1, K)] k: usize,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, distance, None);
        check_exact(&fixture, &random_leaves(SIZES, 1), k);
    }

    /// Integer inner products in the same order of operations as `score_internal`: exact.
    #[rstest]
    fn scalar_quantization(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
        #[values(1, K)] k: usize,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, distance, Some(scalar()));
        check_exact(&fixture, &random_leaves(SIZES, 1), k);
    }

    /// Popcounts: exact, with many ties.
    #[rstest]
    fn binary_quantization(
        #[values(
            BinaryQuantizationEncoding::OneBit,
            BinaryQuantizationEncoding::TwoBits
        )]
        encoding: BinaryQuantizationEncoding,
        #[values(Distance::Dot, Distance::Euclid, Distance::Manhattan)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, distance, Some(binary(encoding)));
        check_exact(&fixture, &random_leaves(SIZES, 1), K);
    }

    /// Float data that rounds differently in the kernels, with the tolerance of the matrix
    /// scorer tests.
    #[rstest]
    fn original_vectors_rounded(
        #[values(Distance::Dot, Distance::Cosine, Distance::Euclid)] distance: Distance,
    ) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, distance, None);
        check_with_tolerance(&fixture, &random_leaves(SIZES, 1), K, 1e-4);
    }

    /// Ties go to the lower position, in whatever order a leaf lists its points.
    #[test]
    fn unsorted_leaves() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Cosine, None);
        let mut rng = StdRng::seed_from_u64(7);
        let mut leaves = random_leaves(SIZES, 7);
        for leaf in &mut leaves {
            leaf.shuffle(&mut rng);
        }
        check_exact(&fixture, &leaves, K);
    }

    /// Every point sends its own picks, every edge comes with its reverse, each directed edge
    /// once, and no point links to itself.
    #[test]
    fn both_directions_once() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Euclid, None);
        let leaves = random_leaves(&[200], 2);
        let (result, edges) = collect_edges(&fixture.scorer(), &leaves, K, &AtomicBool::new(false));
        result.unwrap();

        let pairs: HashSet<_> = edges.iter().map(|&(p, q, _)| (p, q)).collect();
        assert_eq!(pairs.len(), edges.len(), "a directed edge came twice");
        assert!(pairs.iter().all(|&(p, q)| p != q), "edge to itself");
        assert!(pairs.iter().all(|&(p, q)| pairs.contains(&(q, p))));
        let mut sent = HashMap::<PointOffsetType, usize>::new();
        for &(p, _) in &pairs {
            *sent.entry(p).or_default() += 1;
        }
        assert!(leaves[0].iter().all(|p| sent[p] >= K));
        // Some picks are mutual and some aren't, so both cases are covered
        let picks = leaves[0].len() * K;
        assert!(picks < edges.len() && edges.len() < 2 * picks);
    }

    /// A pair picks each other: one edge each way, each with its own score.
    #[test]
    fn two_points() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Euclid, None);
        let reference = fixture.reference();
        let (result, edges) = collect_edges(
            &fixture.scorer(),
            &[vec![3, 250]],
            K,
            &AtomicBool::new(false),
        );
        result.unwrap();
        let want = vec![
            (3, 250, reference.score_internal(3, 250)),
            (250, 3, reference.score_internal(250, 3)),
        ];
        assert_eq!(edges, want);
    }

    /// With `k` from the leaf size on, every pair is a pick both ways: the complete graph, each
    /// direction with its own score. A huge `k` must not size the buffers.
    #[rstest]
    fn k_covers_the_leaf(#[values(5, 6, 1000, usize::MAX)] k: usize) {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Dot, Some(scalar()));
        let reference = fixture.reference();
        let leaves = random_leaves(&[0, 1, 2, 3, 4, 5, 6], 3);
        let mut complete = Vec::new();
        for leaf in &leaves {
            for &p in leaf {
                for &q in leaf.iter().filter(|&&q| q != p) {
                    complete.push((p, q, reference.score_internal(p, q)));
                }
            }
        }
        assert_eq!(complete.len(), 2 + 6 + 12 + 20 + 30);
        let (result, got) = collect_edges(&fixture.scorer(), &leaves, k, &AtomicBool::new(false));
        result.unwrap();
        assert_same_edges(&got, &sorted(complete));
    }

    #[test]
    fn no_edges_without_k() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Dot, None);
        let (result, edges) = collect_edges(
            &fixture.scorer(),
            &random_leaves(SIZES, 1),
            0,
            &AtomicBool::new(false),
        );
        result.unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn no_edges_without_pairs() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Dot, None);
        let scorer = fixture.scorer();
        for leaves in [vec![], vec![vec![]], random_leaves(&[0, 1, 1, 0, 1], 4)] {
            let (result, edges) = collect_edges(&scorer, &leaves, K, &AtomicBool::new(false));
            result.unwrap();
            assert!(edges.is_empty());
        }
    }

    /// The same edges from any number of threads, over many overlapping leaves of uneven sizes,
    /// one of them twice.
    #[test]
    fn any_number_of_threads() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Random, Distance::Cosine, Some(scalar()));
        let scorer = fixture.scorer();
        let mut rng = StdRng::seed_from_u64(5);
        let sizes: Vec<usize> = (0..60).map(|_| rng.random_range(0..=120)).collect();
        let mut leaves = random_leaves(&sizes, 6);
        let largest = leaves.iter().max_by_key(|leaf| leaf.len()).unwrap().clone();
        leaves.push(largest);

        let want = expected_edges(&fixture.reference(), &leaves, K);
        for threads in [1, 2, 5] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let (result, got) =
                pool.install(|| collect_edges(&scorer, &leaves, K, &AtomicBool::new(false)));
            result.unwrap();
            assert_same_edges(&got, &want);
        }
    }

    #[test]
    fn stopped_before_the_first_leaf() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Dot, None);
        let (result, edges) = collect_edges(
            &fixture.scorer(),
            &random_leaves(SIZES, 1),
            K,
            &AtomicBool::new(true),
        );
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
        assert!(edges.is_empty());
    }

    /// A leaf in progress finishes, the next doesn't start.
    #[test]
    fn stopped_between_leaves() {
        let _scope = ambient::test_guard();
        let fixture = Fixture::new(Data::Exact, Distance::Dot, None);
        let scorer = fixture.scorer();
        let leaves = random_leaves(SIZES, 1);
        let (_, all) = collect_edges(&scorer, &leaves, K, &AtomicBool::new(false));

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let stopped = AtomicBool::new(false);
        let count = AtomicUsize::new(0);
        let result = pool.install(|| {
            for_each_leaf_edge(&scorer, &leaves, K, &stopped, |_, _, _| {
                count.fetch_add(1, Ordering::Relaxed);
                stopped.store(true, Ordering::Relaxed);
            })
        });
        assert!(matches!(result, Err(OperationError::Cancelled { .. })));
        let count = count.into_inner();
        assert!(0 < count && count < all.len(), "{count} of {}", all.len());
    }
}
