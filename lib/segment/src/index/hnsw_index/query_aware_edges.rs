//! Query-aware projection edges (build-time only).
//!
//! A post-pass over a fully built [`GraphLayersBuilder`] that rewrites the
//! *level-0* link list of every point, using a set of "training queries" that
//! are representative of the queries the index will actually be asked at search
//! time.
//!
//! The idea (RoarGraph / OOD-DiskANN call it "projection"): two points that are
//! repeatedly retrieved *together* by the same query should be linked, even when
//! they are far apart in the corpus geometry. Corpus-geometry edges (what plain
//! HNSW builds) cannot connect them, so the beam never walks from one to the
//! other and out-of-distribution queries lose recall.
//!
//! The pass is:
//!
//! * **A** — for every training query, its exact top-`proj_topn` points.
//! * **B** — invert that: for every point, the training queries that retrieved
//!   it, sub-sampled to at most `proj_maxq` with a per-point seeded RNG.
//! * **C** — for every point `u`, count how often every other point co-occurs in
//!   those queries' top lists, keep the `proj_cands` most frequent, and select at
//!   most `proj_m` of them with the relative-neighbourhood ("not closer than
//!   base") rule so the projected edges stay diverse.
//! * **D** — new level-0 list of `u` = selected edges (in selection order), then
//!   `u`'s original level-0 links in their original order, skipping duplicates,
//!   truncated to `m0`.
//!
//! Points that no training query retrieved keep their list unchanged. Levels
//! `>= 1`, the entry points and the search code are untouched; the degree cap is
//! respected, so search cost is unchanged by construction.
//!
//! Nothing here is metric-specific: Qdrant scores are "higher is better" for
//! every distance, and the pass only ever compares scores.

use std::cmp::Reverse;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rayon::prelude::*;

use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::index::hnsw_index::graph_layers::GraphLayersBase;
use crate::index::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::index::hnsw_index::point_scorer::FilteredScorer;

/// How many training queries share one pass over the corpus in step A.
/// Larger values amortise the corpus reads, at the cost of more live scorers.
const QUERY_BATCH: usize = 64;

/// How many points are scored at once in step A. Sized so that the vectors of a
/// chunk stay in cache while all `QUERY_BATCH` scorers run over it.
const POINT_CHUNK: usize = 256;

/// How many points one rayon task rewrites in steps B–D.
const REWRITE_CHUNK: usize = 512;

/// Parameters of the projection post-pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionParams {
    /// Exact top-N points of each training query that define co-retrieval.
    pub proj_topn: usize,
    /// Maximum number of retrieving training queries sampled per point.
    pub proj_maxq: usize,
    /// Maximum number of co-retrieval candidates considered per point.
    pub proj_cands: usize,
    /// Maximum number of projected edges added to a point.
    pub proj_m: usize,
    /// Seed of the per-point training-query sub-sampling.
    pub proj_seed: u64,
}

impl Default for ProjectionParams {
    fn default() -> Self {
        Self {
            proj_topn: 100,
            proj_maxq: 64,
            proj_cands: 200,
            proj_m: 16,
            proj_seed: 0,
        }
    }
}

/// What the post-pass did.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProjectionStats {
    /// Number of points in the graph (including ones left untouched).
    pub num_points: usize,
    /// Number of training queries used.
    pub num_train_queries: usize,
    /// Points that at least one training query retrieved (i.e. were rewritten).
    pub points_with_candidates: usize,
    /// Total number of projected edges placed.
    pub projected_edges: usize,
    /// Total number of level-0 links after the pass.
    pub level0_links: usize,
    /// Wall time of step A (exact top-N of every training query).
    pub top_lists_duration: Duration,
    /// Wall time of steps B–D (inverse index, candidates, selection, rewrite).
    pub rewrite_duration: Duration,
}

impl ProjectionStats {
    /// Mean number of projected edges per point, averaged over *all* points.
    pub fn mean_projected_edges(&self) -> f64 {
        if self.num_points == 0 {
            return 0.0;
        }
        self.projected_edges as f64 / self.num_points as f64
    }

    /// Mean level-0 degree after the pass, averaged over *all* points.
    pub fn mean_level0_degree(&self) -> f64 {
        if self.num_points == 0 {
            return 0.0;
        }
        self.level0_links as f64 / self.num_points as f64
    }

    /// Total wall time of the post-pass.
    pub fn total_duration(&self) -> Duration {
        self.top_lists_duration + self.rewrite_duration
    }
}

/// Run the whole projection post-pass against an already built graph.
///
/// Must be called *after* every point has been inserted with
/// [`GraphLayersBuilder::link_new_point`] and *before*
/// [`GraphLayersBuilder::into_graph_layers`].
///
/// * `make_query_scorer(i)` must return a scorer for the `i`-th training query
///   (an external vector, not a stored point).
/// * `make_internal_scorer()` must return a scorer that can score two stored
///   points against each other; one is created per rayon task. Its query vector
///   is irrelevant — only [`FilteredScorer::score_internal`] is used.
///
/// Both closures are called from several threads, so the work is distributed
/// over the ambient rayon pool; install a pool around the call to control it.
pub fn add_query_aware_projection_edges<'a, FQ, FI>(
    builder: &GraphLayersBuilder,
    params: &ProjectionParams,
    num_train_queries: usize,
    make_query_scorer: FQ,
    make_internal_scorer: FI,
    is_stopped: &AtomicBool,
) -> OperationResult<ProjectionStats>
where
    FQ: Fn(usize) -> OperationResult<FilteredScorer<'a>> + Sync,
    FI: Fn() -> OperationResult<FilteredScorer<'a>> + Sync,
{
    let num_points = builder.num_points();
    if num_points == 0 || num_train_queries == 0 || params.proj_m == 0 {
        return Ok(ProjectionStats {
            num_points,
            num_train_queries,
            level0_links: count_level0_links(builder),
            ..Default::default()
        });
    }
    let points = eligible_points(builder, &make_internal_scorer)?;
    let topn = params.proj_topn.min(points.len());
    if topn == 0 {
        return Ok(ProjectionStats {
            num_points,
            num_train_queries,
            level0_links: count_level0_links(builder),
            ..Default::default()
        });
    }

    let timer = Instant::now();
    let top_lists = compute_top_lists(
        &points,
        num_train_queries,
        topn,
        &make_query_scorer,
        is_stopped,
    )?;
    let top_lists_duration = timer.elapsed();

    let mut stats = add_query_aware_projection_edges_from_lists(
        builder,
        params,
        &top_lists,
        topn,
        make_internal_scorer,
        is_stopped,
    )?;
    stats.top_lists_duration = top_lists_duration;
    Ok(stats)
}

/// Steps B–D only: run the post-pass with *precomputed* top lists.
///
/// `top_lists` is a flat `[num_train_queries, topn]` array of point ids, best
/// first, from [`exact_top_lists`], [`approximate_top_lists`] or any other
/// source. `top_lists_duration` in the returned stats is zero; the caller knows
/// how long its lists took.
pub fn add_query_aware_projection_edges_from_lists<'a, FI>(
    builder: &GraphLayersBuilder,
    params: &ProjectionParams,
    top_lists: &[PointOffsetType],
    topn: usize,
    make_internal_scorer: FI,
    is_stopped: &AtomicBool,
) -> OperationResult<ProjectionStats>
where
    FI: Fn() -> OperationResult<FilteredScorer<'a>> + Sync,
{
    let num_points = builder.num_points();
    if topn == 0 || top_lists.len() % topn != 0 {
        return Err(OperationError::service_error(format!(
            "query-aware projection: top lists of {} ids are not a multiple of topn={topn}",
            top_lists.len()
        )));
    }
    let num_train_queries = top_lists.len() / topn;
    let mut stats = ProjectionStats {
        num_points,
        num_train_queries,
        ..Default::default()
    };
    if num_points == 0 || num_train_queries == 0 || params.proj_m == 0 {
        stats.level0_links = count_level0_links(builder);
        return Ok(stats);
    }

    let timer = Instant::now();
    let rewrite = rewrite_level0_links(
        builder,
        top_lists,
        topn,
        params,
        &make_internal_scorer,
        is_stopped,
    )?;
    stats.rewrite_duration = timer.elapsed();

    stats.points_with_candidates = rewrite.points_with_candidates;
    stats.projected_edges = rewrite.projected_edges;
    stats.level0_links = rewrite.level0_links;
    Ok(stats)
}

/// Points that may take part in the projection at all (pass the scorer's filter).
fn eligible_points<'a, FI>(
    builder: &GraphLayersBuilder,
    make_internal_scorer: &FI,
) -> OperationResult<Vec<PointOffsetType>>
where
    FI: Fn() -> OperationResult<FilteredScorer<'a>>,
{
    let scorer = make_internal_scorer()?;
    Ok((0..builder.num_points() as PointOffsetType)
        .filter(|&p| scorer.filters().check_vector(p))
        .collect())
}

/// Step A, exact: the exact top-`topn` points of every training query by a full
/// scan of the corpus. Flat `[num_train_queries, topn]`, best first.
pub fn exact_top_lists<'a, FQ, FI>(
    builder: &GraphLayersBuilder,
    num_train_queries: usize,
    topn: usize,
    make_query_scorer: &FQ,
    make_internal_scorer: &FI,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<PointOffsetType>>
where
    FQ: Fn(usize) -> OperationResult<FilteredScorer<'a>> + Sync,
    FI: Fn() -> OperationResult<FilteredScorer<'a>> + Sync,
{
    let points = eligible_points(builder, make_internal_scorer)?;
    let topn = topn.min(points.len());
    compute_top_lists(
        &points,
        num_train_queries,
        topn,
        make_query_scorer,
        is_stopped,
    )
}

/// Step A, approximate: the top-`topn` points of every training query found by
/// an HNSW search *on the graph being built* (greedy descent from the entry
/// point, then a level-0 beam of width `max(ef, topn)`), instead of the exact
/// scan. Costs `O(ef · degree)` scored points per query rather than
/// `O(num_points)`; the lists are only as good as the raw graph is for these
/// queries. Flat `[num_train_queries, topn]`, best first.
pub fn approximate_top_lists<'a, FQ>(
    builder: &GraphLayersBuilder,
    num_train_queries: usize,
    topn: usize,
    ef: usize,
    make_query_scorer: &FQ,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<PointOffsetType>>
where
    FQ: Fn(usize) -> OperationResult<FilteredScorer<'a>> + Sync,
{
    let total = num_train_queries.checked_mul(topn).ok_or_else(|| {
        OperationError::service_error("query-aware projection: top-list size overflow")
    })?;
    if u32::try_from(total).is_err() {
        return Err(OperationError::service_error(
            "query-aware projection: too many training queries for the inverse index",
        ));
    }
    let ef = ef.max(topn);
    let mut top_lists = vec![0 as PointOffsetType; total];

    top_lists
        .par_chunks_mut(topn)
        .enumerate()
        .try_for_each(|(query_idx, row)| -> OperationResult<()> {
            check_process_stopped(is_stopped)?;
            let mut scorer = make_query_scorer(query_idx)?;
            let entry = builder
                .get_entry_points()
                .get_entry_point(|p| scorer.filters().check_vector(p))
                .ok_or_else(|| {
                    OperationError::service_error(
                        "query-aware projection: graph has no entry point",
                    )
                })?;
            let level_entry =
                builder.search_entry(entry.point_id, entry.level, 0, &mut scorer, is_stopped)?;
            let nearest = builder.search_on_level(level_entry, 0, ef, &mut scorer, is_stopped)?;
            let mut filled = 0;
            for (j, scored) in nearest.into_iter_sorted().take(topn).enumerate() {
                row[j] = scored.idx;
                filled = j + 1;
            }
            if filled < topn {
                return Err(OperationError::service_error(format!(
                    "query-aware projection: approximate search returned {filled} < topn={topn} points"
                )));
            }
            Ok(())
        })?;

    Ok(top_lists)
}

fn count_level0_links(builder: &GraphLayersBuilder) -> usize {
    let mut links = Vec::new();
    let mut total = 0;
    for point_id in 0..builder.num_points() as PointOffsetType {
        builder.level0_links(point_id, &mut links);
        total += links.len();
    }
    total
}

/// Step A: exact top-`topn` points of every training query, best first.
///
/// Returns a `num_train_queries * topn` flat array of point ids.
fn compute_top_lists<'a, FQ>(
    points: &[PointOffsetType],
    num_train_queries: usize,
    topn: usize,
    make_query_scorer: &FQ,
    is_stopped: &AtomicBool,
) -> OperationResult<Vec<PointOffsetType>>
where
    FQ: Fn(usize) -> OperationResult<FilteredScorer<'a>> + Sync,
{
    let total = num_train_queries.checked_mul(topn).ok_or_else(|| {
        OperationError::service_error("query-aware projection: top-list size overflow")
    })?;
    if u32::try_from(total).is_err() {
        return Err(OperationError::service_error(
            "query-aware projection: too many training queries for the inverse index",
        ));
    }
    let mut top_lists = vec![0 as PointOffsetType; total];

    top_lists
        .par_chunks_mut(QUERY_BATCH * topn)
        .enumerate()
        .try_for_each(|(batch_idx, out)| -> OperationResult<()> {
            let first_query = batch_idx * QUERY_BATCH;
            let batch_len = out.len() / topn;

            let mut scorers = Vec::with_capacity(batch_len);
            for i in 0..batch_len {
                scorers.push(make_query_scorer(first_query + i)?);
            }
            let mut heaps: Vec<FixedLengthPriorityQueue<ScoredPointOffset>> = (0..batch_len)
                .map(|_| FixedLengthPriorityQueue::new(topn))
                .collect();

            for chunk in points.chunks(POINT_CHUNK) {
                check_process_stopped(is_stopped)?;
                for (scorer, heap) in scorers.iter_mut().zip(heaps.iter_mut()) {
                    for scored in scorer.score_points_unfiltered(chunk) {
                        heap.push(scored);
                    }
                }
            }

            for (i, heap) in heaps.into_iter().enumerate() {
                for (j, scored) in heap.into_iter_sorted().enumerate() {
                    out[i * topn + j] = scored.idx;
                }
            }
            Ok(())
        })?;

    Ok(top_lists)
}

struct RewriteStats {
    points_with_candidates: usize,
    projected_edges: usize,
    level0_links: usize,
}

/// Steps B–D: build the inverse index, select the projected edges and rewrite
/// the level-0 link lists in place.
///
/// `top_lists` is the flat `[num_queries, topn]` array produced by step A. Every
/// point is touched by exactly one task, and a task only reads its own point's
/// links, so the in-place rewrite is safe and order-independent.
fn rewrite_level0_links<'a, FI>(
    builder: &GraphLayersBuilder,
    top_lists: &[PointOffsetType],
    topn: usize,
    params: &ProjectionParams,
    make_internal_scorer: &FI,
    is_stopped: &AtomicBool,
) -> OperationResult<RewriteStats>
where
    FI: Fn() -> OperationResult<FilteredScorer<'a>> + Sync,
{
    let num_points = builder.num_points();
    let m0 = builder.hnsw_m().m0;

    // Step B: inverse index point -> training queries that retrieved it, as CSR.
    let mut starts = vec![0u32; num_points + 1];
    for &point_id in top_lists {
        starts[point_id as usize + 1] += 1;
    }
    for i in 0..num_points {
        starts[i + 1] += starts[i];
    }
    let mut cursor = starts[..num_points].to_vec();
    let mut queries_of_point = vec![0u32; top_lists.len()];
    for (query_idx, row) in top_lists.chunks(topn).enumerate() {
        for &point_id in row {
            let slot = &mut cursor[point_id as usize];
            queries_of_point[*slot as usize] = query_idx as u32;
            *slot += 1;
        }
    }

    let ranges: Vec<(usize, usize)> = (0..num_points)
        .step_by(REWRITE_CHUNK)
        .map(|from| (from, (from + REWRITE_CHUNK).min(num_points)))
        .collect();

    let partials = ranges
        .par_iter()
        .map(|&(from, to)| -> OperationResult<RewriteStats> {
            let scorer = make_internal_scorer()?;
            let mut local = RewriteStats {
                points_with_candidates: 0,
                projected_edges: 0,
                level0_links: 0,
            };
            let mut sampled: Vec<u32> = Vec::new();
            let mut occurrences: Vec<PointOffsetType> = Vec::new();
            let mut candidates: Vec<(u32, PointOffsetType)> = Vec::new();
            let mut selected: Vec<PointOffsetType> = Vec::new();
            let mut original: Vec<PointOffsetType> = Vec::new();
            let mut new_links: Vec<PointOffsetType> = Vec::new();

            for point_id in from..to {
                check_process_stopped(is_stopped)?;
                let u = point_id as PointOffsetType;
                builder.level0_links(u, &mut original);

                let retrieving =
                    &queries_of_point[starts[point_id] as usize..starts[point_id + 1] as usize];
                if retrieving.is_empty() {
                    // No training query wants this point: keep its list as is.
                    local.level0_links += original.len();
                    continue;
                }
                local.points_with_candidates += 1;

                // Sub-sample the retrieving queries with a per-point RNG, so the
                // result does not depend on how the points are split over threads.
                sampled.clear();
                sampled.extend_from_slice(retrieving);
                if sampled.len() > params.proj_maxq {
                    let mut rng = StdRng::seed_from_u64(mix_seed(params.proj_seed, u));
                    for i in 0..params.proj_maxq {
                        let j = rng.random_range(i..sampled.len());
                        sampled.swap(i, j);
                    }
                    sampled.truncate(params.proj_maxq);
                }

                // Step C: count co-retrieved points, keep the most frequent ones.
                occurrences.clear();
                for &query_idx in &sampled {
                    let offset = query_idx as usize * topn;
                    occurrences.extend_from_slice(&top_lists[offset..offset + topn]);
                }
                occurrences.sort_unstable();
                candidates.clear();
                let mut i = 0;
                while i < occurrences.len() {
                    let value = occurrences[i];
                    let mut j = i + 1;
                    while j < occurrences.len() && occurrences[j] == value {
                        j += 1;
                    }
                    if value != u {
                        candidates.push(((j - i) as u32, value));
                    }
                    i = j;
                }
                // Most co-retrieved first; ties broken by point id for determinism.
                candidates.sort_unstable_by_key(|&(count, id)| (Reverse(count), id));
                candidates.truncate(params.proj_cands);

                // Relative-neighbourhood pruning: keep `c` only when no already
                // selected `s` is closer to `c` than `u` is.
                selected.clear();
                for &(_, c) in &candidates {
                    if selected.len() >= params.proj_m {
                        break;
                    }
                    let c_to_u = scorer.score_internal(c, u);
                    let redundant = selected
                        .iter()
                        .any(|&s| scorer.score_internal(s, c) >= c_to_u);
                    if !redundant {
                        selected.push(c);
                    }
                }

                // Step D: projected edges first, then the original ones.
                new_links.clear();
                new_links.extend_from_slice(&selected);
                new_links.truncate(m0);
                for &link in &original {
                    if new_links.len() >= m0 {
                        break;
                    }
                    if !new_links.contains(&link) {
                        new_links.push(link);
                    }
                }

                local.projected_edges += selected.len().min(m0);
                local.level0_links += new_links.len();
                builder.set_level0_links(u, new_links.iter().copied());
            }
            Ok(local)
        })
        .collect::<OperationResult<Vec<_>>>()?;

    Ok(partials.into_iter().fold(
        RewriteStats {
            points_with_candidates: 0,
            projected_edges: 0,
            level0_links: 0,
        },
        |mut acc, part| {
            acc.points_with_candidates += part.points_with_candidates;
            acc.projected_edges += part.projected_edges;
            acc.level0_links += part.level0_links;
            acc
        },
    ))
}

/// Derive a per-point RNG seed, so that sub-sampling is reproducible for a given
/// `proj_seed` no matter how the points are distributed over threads.
fn mix_seed(seed: u64, point_id: PointOffsetType) -> u64 {
    // SplitMix64 finalizer.
    let mut z = seed ^ (u64::from(point_id).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use common::bitvec::BitVec;
    use common::counter::hardware_counter::HardwareCounterCell;

    use super::*;
    use crate::data_types::vectors::{QueryVector, VectorRef};
    use crate::index::hnsw_index::HnswM;
    use crate::types::Distance;
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;

    /// Unit vector at `deg` degrees; dot(a, b) = cos(angle between them), which
    /// makes every score in the test readable by eye.
    fn unit(deg: f32) -> Vec<f32> {
        let rad = deg.to_radians();
        vec![rad.cos(), rad.sin()]
    }

    /// Six points on the unit circle plus one that no query ever retrieves.
    ///
    /// For `u = 0` (0°) the candidate order is `[1 (10°), 2 (20°), 3 (190°), 4 (90°)]`:
    /// * 1 is selected first,
    /// * 2 is pruned  (dot(1,2) = cos 10° > dot(0,2) = cos 20°),
    /// * 3 is kept    (dot(1,3) = cos 180° < dot(0,3) = cos 190°),
    /// * 4 is pruned  (dot(1,4) = cos 80° > dot(0,4) = cos 90° = 0).
    fn fixture_vectors() -> Vec<Vec<f32>> {
        vec![
            unit(0.0),
            unit(10.0),
            unit(20.0),
            unit(190.0),
            unit(90.0),
            unit(270.0),
            unit(45.0),
        ]
    }

    fn fixture_links() -> Vec<Vec<PointOffsetType>> {
        vec![
            vec![4, 1, 5], // point 0: 1 is also a projected edge -> deduplicated
            vec![0, 2],
            vec![0, 1],
            vec![0],
            vec![0],
            vec![0],
            vec![0, 1], // point 6: no training query retrieves it
        ]
    }

    /// `topn = 4`; point 6 appears in no list, point 0 is retrieved by q0 and q1.
    fn fixture_top_lists() -> (Vec<PointOffsetType>, usize) {
        (vec![0, 1, 2, 3, /**/ 0, 1, 2, 4, /**/ 5, 4, 3, 1], 4)
    }

    fn build_fixture(m0: usize) -> GraphLayersBuilder {
        let links = fixture_links();
        let mut builder = GraphLayersBuilder::new(links.len(), HnswM::new(2, m0), 4, 1, true);
        for point_id in 0..links.len() as PointOffsetType {
            builder.set_levels(point_id, 0);
        }
        for (point_id, point_links) in links.iter().enumerate() {
            builder.add_new_point(point_id as PointOffsetType, vec![point_links.clone()]);
        }
        builder
    }

    fn run(builder: &GraphLayersBuilder, params: &ProjectionParams) -> RewriteStats {
        let vectors = fixture_vectors();
        let mut storage = new_volatile_dense_vector_storage(2, Distance::Dot);
        let hw = HardwareCounterCell::new();
        for (point_id, vector) in vectors.iter().enumerate() {
            storage
                .insert_vector(
                    point_id as PointOffsetType,
                    VectorRef::from(&vector[..]),
                    &hw,
                )
                .unwrap();
        }
        let deleted = BitVec::repeat(false, vectors.len());
        let (top_lists, topn) = fixture_top_lists();
        rewrite_level0_links(
            builder,
            &top_lists,
            topn,
            params,
            &|| {
                Ok(FilteredScorer::new_for_test(
                    QueryVector::from(vectors[0].clone()),
                    &storage,
                    &deleted,
                ))
            },
            &AtomicBool::new(false),
        )
        .unwrap()
    }

    fn links_of(builder: &GraphLayersBuilder, point_id: PointOffsetType) -> Vec<PointOffsetType> {
        let mut links = Vec::new();
        builder.level0_links(point_id, &mut links);
        links
    }

    #[test]
    fn projection_selects_rng_pruned_edges_and_keeps_original_order() {
        let builder = build_fixture(4);
        let stats = run(
            &builder,
            &ProjectionParams {
                proj_m: 4,
                ..Default::default()
            },
        );

        // Projected [1, 3] first, then the original [4, 1, 5] with 1 deduplicated.
        assert_eq!(links_of(&builder, 0), vec![1, 3, 4, 5]);
        // Point 6 is in no top list, so it keeps its original links.
        assert_eq!(links_of(&builder, 6), vec![0, 1]);
        assert_eq!(stats.points_with_candidates, 6);
    }

    #[test]
    fn projection_truncates_to_m0() {
        let builder = build_fixture(3);
        run(
            &builder,
            &ProjectionParams {
                proj_m: 4,
                ..Default::default()
            },
        );
        assert_eq!(links_of(&builder, 0), vec![1, 3, 4]);
    }

    #[test]
    fn projection_respects_proj_m() {
        let builder = build_fixture(4);
        run(
            &builder,
            &ProjectionParams {
                proj_m: 1,
                ..Default::default()
            },
        );
        // Only the first projected edge survives; the rest is the original list.
        assert_eq!(links_of(&builder, 0), vec![1, 4, 5]);
    }

    #[test]
    fn projection_without_rng_rule_would_keep_the_count_order() {
        // Sanity check of the fixture itself: the pruning rule is what removes
        // point 2, not the candidate ordering.
        let vectors = fixture_vectors();
        let dot =
            |a: usize, b: usize| vectors[a][0] * vectors[b][0] + vectors[a][1] * vectors[b][1];
        assert!(dot(1, 2) > dot(0, 2));
        assert!(dot(1, 3) < dot(0, 3));
        assert!(dot(1, 4) > dot(0, 4));
    }

    #[test]
    fn sub_sampling_is_deterministic_for_a_seed() {
        let params = ProjectionParams {
            proj_m: 4,
            proj_maxq: 1,
            ..Default::default()
        };
        let first = build_fixture(4);
        run(&first, &params);
        let second = build_fixture(4);
        run(&second, &params);
        for point_id in 0..7 {
            assert_eq!(links_of(&first, point_id), links_of(&second, point_id));
        }
    }

    #[test]
    fn zero_proj_m_is_a_no_op() {
        let builder = build_fixture(4);
        run(
            &builder,
            &ProjectionParams {
                proj_m: 0,
                ..Default::default()
            },
        );
        for (point_id, original) in fixture_links().iter().enumerate() {
            assert_eq!(&links_of(&builder, point_id as PointOffsetType), original);
        }
    }
}
