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
//!   `u`'s original level-0 links nearest first (so the cut drops the farthest
//!   ones), skipping duplicates, truncated to `m0`.
//! * **E** — reachability repair (Hua et al. 2025, NGFix with the neighbourhood
//!   set equal to the whole top list): for every training query in turn, its
//!   top-`proj_topn` points minus the excluded ones must all reach each other
//!   through level-0 links inside that set; while some pair cannot, the closest
//!   unreached pair gets a link, placed at the front of the source's list, which
//!   is cut back to `m0`. A point holds at most `repair_max_per_point` such links.
//!
//! `excluded_targets` (step C) are points that never receive a projected edge:
//! attention sinks. A key retrieved by a large share of queries but the best key
//! for few of them otherwise becomes a hub of thousands of in-links, and a beam
//! that lands next to it stalls there. The caller scores excluded points directly
//! at query time. Points that no training query retrieved keep their list
//! unchanged. Levels `>= 1`, the entry points and the search code are untouched;
//! the degree cap is respected, so search cost is unchanged by construction.
//!
//! Recipe decided 2026-09-20 on measured data (kv-search
//! `docs/2026-09-19-L15H3-projection-findings.md` §3g-§3j): sorted cut, sinks
//! excluded, repair on with cap 32. Rejected there, and not kept in the code: an
//! in-degree cap on targets, a rank gate on who may link to a sink, reserved slots
//! for original links, a frequency-based sink detector (blind on the training
//! queries of L15H3), repair of the rank-1 pairs only, and repair without the
//! co-retrieval edges.
//!
//! Nothing here is metric-specific: Qdrant scores are "higher is better" for
//! every distance, and the pass only ever compares scores.

use std::cmp::Reverse;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use fs_err as fs;
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

/// Environment variable that turns the provenance dump of the pass on, by naming the directory to
/// write it into. Unset (the normal case) means no dump and no cost — see
/// [`write_projection_dump`] for the format.
pub const PROJECTION_DUMP_DIR_ENV: &str = "QDRANT_HNSW_PROJECTION_DUMP_DIR";

/// Parameters of the projection post-pass.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Internal offsets of the points that never receive a projected edge (step C) and are
    /// left out of the repair's neighbourhoods (step E). Sorted ascending; empty = none.
    pub excluded_targets: Vec<PointOffsetType>,
    /// Step E: run the reachability repair, with this many repair links per point at most.
    /// `None` skips the repair.
    pub repair_max_per_point: Option<usize>,
}

impl Default for ProjectionParams {
    fn default() -> Self {
        Self {
            proj_topn: 100,
            proj_maxq: 64,
            proj_cands: 200,
            proj_m: 16,
            proj_seed: 0,
            excluded_targets: Vec::new(),
            repair_max_per_point: Some(32),
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
    if num_points == 0
        || num_train_queries == 0
        || (params.proj_m == 0 && params.repair_max_per_point.is_none())
    {
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
    let run_coretrieval = params.proj_m > 0;
    if num_points == 0
        || num_train_queries == 0
        || (!run_coretrieval && params.repair_max_per_point.is_none())
    {
        stats.level0_links = count_level0_links(builder);
        return Ok(stats);
    }

    let mut excluded_targets = params.excluded_targets.clone();
    excluded_targets.sort_unstable();
    excluded_targets.dedup();
    if !excluded_targets.is_empty() {
        log::info!(
            "HNSW query-aware projection: {} points excluded as projected targets: {:?}",
            excluded_targets.len(),
            excluded_targets
        );
    }
    let dump_dir = std::env::var_os(PROJECTION_DUMP_DIR_ENV).map(PathBuf::from);
    if let Some(dir) = &dump_dir {
        // Part of the provenance dump: the excluded points, one internal offset per line, so the
        // query side can score the very same points directly.
        fs::create_dir_all(dir)?;
        let text: String = excluded_targets.iter().map(|p| format!("{p}\n")).collect();
        fs::write(dir.join("excluded_targets.txt"), text)?;
    }

    let timer = Instant::now();
    if run_coretrieval {
        let rewrite = rewrite_level0_links(
            builder,
            top_lists,
            topn,
            params,
            &excluded_targets,
            &make_internal_scorer,
            is_stopped,
        )?;
        stats.points_with_candidates = rewrite.points_with_candidates;
        stats.projected_edges = rewrite.projected_edges;
        stats.level0_links = rewrite.level0_links;
    } else if let Some(dir) = &dump_dir {
        // Repair only: the co-retrieval pass added nothing, so its provenance dump is one empty
        // row per point. Written so a caller that requires the dump still finds it.
        let empty: Vec<Vec<PointOffsetType>> = vec![Vec::new(); num_points];
        write_projection_dump(dir, empty.iter())?;
    }
    if let Some(max_per_point) = params.repair_max_per_point {
        let added = reachability_repair(
            builder,
            top_lists,
            topn,
            max_per_point,
            !run_coretrieval,
            &excluded_targets,
            &make_internal_scorer,
            is_stopped,
        )?;
        stats.projected_edges += added;
        stats.level0_links = count_level0_links(builder);
    }
    stats.rewrite_duration = timer.elapsed();
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

/// What one rewrite task returns: its share of the stats, and — only when the provenance dump is
/// on — one row of projected links per point of its chunk, in point order.
/// The third element is only filled by the THROWAWAY two-phase (in-degree cap) path: the selected
/// targets of every rewritten point of the chunk, to be composed and written after the cap.
type RewritePartial = (RewriteStats, Vec<Vec<PointOffsetType>>);

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
    excluded_targets: &[PointOffsetType],
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

    // Provenance dump (off unless `QDRANT_HNSW_PROJECTION_DUMP_DIR` is set): which level-0 links
    // this pass added, per point — added, not merely placed, so a selected target the builder had
    // already linked is left out (see the push below and `write_projection_dump`). Only the lookup
    // happens on the default path — no allocation, no per-point bookkeeping — so a normal build
    // pays nothing for it.
    let dump_dir = std::env::var_os(PROJECTION_DUMP_DIR_ENV).map(PathBuf::from);

    let partials = ranges
        .par_iter()
        .map(|&(from, to)| -> OperationResult<RewritePartial> {
            let scorer = make_internal_scorer()?;
            let mut local = RewriteStats {
                points_with_candidates: 0,
                projected_edges: 0,
                level0_links: 0,
            };
            // One row per point of this chunk, in point order; stays empty when not dumping.
            let mut dumped: Vec<Vec<PointOffsetType>> = Vec::new();
            if dump_dir.is_some() {
                dumped.reserve(to - from);
            }
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
                    if dump_dir.is_some() {
                        dumped.push(Vec::new());
                    }
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
                    // `excluded_targets`: attention sinks never become targets (module docs).
                    if value != u && excluded_targets.binary_search(&value).is_err() {
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
                compose_links(u, &selected, &mut original, m0, &scorer, &mut new_links);

                local.projected_edges += selected.len().min(m0);
                local.level0_links += new_links.len();
                if dump_dir.is_some() {
                    // Exactly the links the pass ADDED: the ones it placed (`selected`, in
                    // selection order, cut to the degree cap the way step D cuts it) minus the
                    // ones the builder had already linked. `selected` is chosen from co-retrieval
                    // candidates with no exclusion of the existing list -- which is precisely why
                    // step D above has to de-duplicate against `original` -- so a target the
                    // builder had already chosen would otherwise be recorded here as projected
                    // provenance it is not. `original` is the list as it was read at the top of
                    // this iteration, before `set_level0_links` below overwrites it.
                    dumped.push(
                        selected[..selected.len().min(m0)]
                            .iter()
                            .filter(|c| !original.contains(c))
                            .copied()
                            .collect(),
                    );
                }
                builder.set_level0_links(u, new_links.iter().copied());
            }
            Ok((local, dumped))
        })
        .collect::<OperationResult<Vec<_>>>()?;

    if let Some(dir) = &dump_dir {
        // `ranges` covers `0..num_points` in order and `par_iter().collect()` keeps that order, so
        // concatenating the chunks yields one row per point, point id ascending.
        write_projection_dump(dir, partials.iter().flat_map(|(_, rows)| rows.iter()))?;
    }

    Ok(partials.into_iter().map(|(stats, _)| stats).fold(
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

/// Write the provenance dump of one projection pass: which level-0 links the pass added, per
/// point, as a CSR pair of flat little-endian arrays (what `numpy.fromfile` reads).
///
/// | file | dtype | length | meaning |
/// |---|---|---|---|
/// | `projected_offsets.u64` | `uint64` | `points + 1` | CSR row index: point `v`'s projected links are `projected_links[offsets[v]..offsets[v + 1]]` |
/// | `projected_links.u32` | `uint32` | the last offset | the projected link targets, as **internal** point ids, in selection order |
///
/// "Added" is meant literally: a row holds the projected links that were **not already builder
/// links** of that point. A selected target the builder had already linked is not listed — the
/// pass moved it to the front of the list but did not create it, so counting it as projected
/// provenance would over-attribute. A point no training query retrieved has an empty row, and so
/// does a point every one of whose selected targets it was already linked to. The ids are internal
/// (segment point offsets), the same ids the graph's own link lists use, so a reader joins them to
/// token positions through the segment's id map.
///
/// `rows` must yield exactly one row per point, point id ascending.
fn write_projection_dump<'r, I>(dir: &Path, rows: I) -> OperationResult<()>
where
    I: Iterator<Item = &'r Vec<PointOffsetType>>,
{
    fs::create_dir_all(dir)?;
    let mut offsets: Vec<u8> = vec![0u8; size_of::<u64>()];
    let mut links: Vec<u8> = Vec::new();
    let mut offset = 0u64;
    for row in rows {
        offset += row.len() as u64;
        offsets.extend_from_slice(&offset.to_le_bytes());
        for &link in row {
            links.extend_from_slice(&link.to_le_bytes());
        }
    }
    fs::write(dir.join("projected_offsets.u64"), &offsets)?;
    fs::write(dir.join("projected_links.u32"), &links)?;
    Ok(())
}

/// Step E: reachability repair, Hua et al. 2025's NGFix with `K_h = N_q = topn`, run
/// sequentially over the training queries so that every query sees the edges the earlier ones
/// added.
///
/// For one training query: its top-`topn` points minus the excluded targets (the sinks) are the
/// nodes; the level-0 links between them are the edges (the paper's `NG_{S,q}` induced subgraph);
/// [`plan_reach_edges`] decides which directed edges to add so that every node reaches every
/// other node inside that subgraph, closest pair first with the closure updated after each
/// addition. Each added edge `s -> t` is placed at the front of `s`'s level-0 list, which is
/// truncated to `m0` (so the last link is evicted: the farthest original when the list is
/// nearest-first). A point holds at most `max_per_point` repair edges; a pair whose source is
/// full is skipped and the next closest pair is tried instead, which is the paper's degree cap
/// without its EH-based pruning.
///
/// `sort_on_first_touch`: when step D did not run (repair without co-retrieval edges) nothing has
/// ordered the lists yet, so a point's list is sorted nearest first the first time the repair
/// touches it, and the eviction still drops the farthest link.
///
/// Returns the number of edges added. Sequential and deterministic.
fn reachability_repair<'a, FI>(
    builder: &GraphLayersBuilder,
    top_lists: &[PointOffsetType],
    topn: usize,
    max_per_point: usize,
    sort_on_first_touch: bool,
    excluded: &[PointOffsetType],
    make_internal_scorer: &FI,
    is_stopped: &AtomicBool,
) -> OperationResult<usize>
where
    FI: Fn() -> OperationResult<FilteredScorer<'a>> + Sync,
{
    if topn > 128 {
        return Err(OperationError::service_error(format!(
            "reachability repair: topn={topn} exceeds the 128-node bitset"
        )));
    }
    let num_points = builder.num_points();
    let m0 = builder.hnsw_m().m0;
    let scorer = make_internal_scorer()?;
    let started = Instant::now();

    let mut reach_count = vec![0u8; num_points];
    let mut touched = vec![false; num_points];

    let mut nodes: Vec<PointOffsetType> = Vec::with_capacity(topn);
    let mut links: Vec<PointOffsetType> = Vec::new();
    let mut adj: Vec<u128> = Vec::with_capacity(topn);
    let mut scores: Vec<f32> = Vec::new();
    let (mut queries_with_defects, mut edges_added, mut skipped_full, mut evicted) =
        (0usize, 0usize, 0usize, 0usize);
    let mut per_query_added: Vec<u32> = Vec::with_capacity(top_lists.len() / topn);

    for (qi, row) in top_lists.chunks(topn).enumerate() {
        if qi % 1024 == 0 {
            check_process_stopped(is_stopped)?;
        }
        // nodes in rank order, sinks dropped
        nodes.clear();
        nodes.extend(
            row.iter()
                .copied()
                .filter(|p| excluded.binary_search(p).is_err()),
        );
        let n = nodes.len();
        if n < 2 {
            per_query_added.push(0);
            continue;
        }
        // rank lookup: node id -> index; nodes are few, a sorted pair list beats a hash map
        let mut index: Vec<(PointOffsetType, u8)> = nodes
            .iter()
            .enumerate()
            .map(|(i, &p)| (p, i as u8))
            .collect();
        index.sort_unstable();
        let rank_of = |p: PointOffsetType| -> Option<usize> {
            index
                .binary_search_by_key(&p, |&(id, _)| id)
                .ok()
                .map(|k| index[k].1 as usize)
        };
        // induced adjacency
        adj.clear();
        for &u in &nodes {
            builder.level0_links(u, &mut links);
            let mut bits: u128 = 0;
            for &l in &links {
                if let Some(j) = rank_of(l) {
                    bits |= 1u128 << j;
                }
            }
            adj.push(bits);
        }
        // pairwise scores, lazily: only computed when the closure is incomplete
        let plan = plan_reach_edges(
            &adj,
            |i, j| {
                if scores.is_empty() {
                    scores.resize(n * n, f32::NAN);
                }
                let k = i * n + j;
                if scores[k].is_nan() {
                    let sc = scorer.score_internal(nodes[i], nodes[j]);
                    scores[k] = sc;
                    scores[j * n + i] = sc;
                }
                scores[k]
            },
            |i| (reach_count[nodes[i] as usize] as usize) < max_per_point,
        );
        scores.clear();
        if plan.added.is_empty() && plan.skipped == 0 {
            per_query_added.push(0);
            continue;
        }
        queries_with_defects += 1;
        skipped_full += plan.skipped;
        per_query_added.push(plan.added.len() as u32);
        for &(si, ti) in &plan.added {
            let (s, t) = (nodes[si], nodes[ti]);
            builder.level0_links(s, &mut links);
            if sort_on_first_touch && !touched[s as usize] {
                let mut scored: Vec<(f32, PointOffsetType)> = links
                    .iter()
                    .map(|&l| (scorer.score_internal(s, l), l))
                    .collect();
                scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
                links.clear();
                links.extend(scored.into_iter().map(|(_, l)| l));
            }
            touched[s as usize] = true;
            if links.contains(&t) {
                // already linked outside the induced view (cannot happen: adj came from the same
                // list), keep the count honest anyway
                continue;
            }
            links.insert(0, t);
            if links.len() > m0 {
                links.truncate(m0);
                evicted += 1;
            }
            builder.set_level0_links(s, links.iter().copied());
            reach_count[s as usize] = reach_count[s as usize].saturating_add(1);
            edges_added += 1;
        }
    }
    let points_at_cap = reach_count
        .iter()
        .filter(|&&c| c as usize >= max_per_point)
        .count();
    let points_with_repair = reach_count.iter().filter(|&&c| c > 0).count();
    per_query_added.sort_unstable();
    let pct = |p: f64| per_query_added[((per_query_added.len() - 1) as f64 * p) as usize];
    log::info!(
        "HNSW query-aware projection: reachability repair (max {} per point): \
         {} queries, {} with defects, {} edges added, {} evictions, {} pairs skipped for a full source, \
         {} points hold repair edges, {} at the cap; edges per query p50 {} p90 {} p99 {} max {}; {:.1}s",
        max_per_point,
        per_query_added.len(),
        queries_with_defects,
        edges_added,
        evicted,
        skipped_full,
        points_with_repair,
        points_at_cap,
        pct(0.5),
        pct(0.9),
        pct(0.99),
        per_query_added.last().copied().unwrap_or(0),
        started.elapsed().as_secs_f64()
    );
    Ok(edges_added)
}

/// What [`plan_reach_edges`] decided for one query.
struct ReachPlan {
    /// Directed edges `(source index, target index)` to add, in the order they were chosen.
    added: Vec<(usize, usize)>,
    /// Pairs that were needed but whose source was full; the closure stays incomplete for them.
    skipped: usize,
}

/// The pure core of the reachability repair for one query: given the induced adjacency (`adj[i]`
/// bit `j` set when node `i` links to node `j`), decide which edges to add.
///
/// Computes the transitive closure, then repeats: among the ordered pairs `(i, j)` that are not
/// connected, take the closest by `score` (higher = closer) whose source `can_add`; add it and
/// update the closure (every node that reaches `i` now reaches everything `j` reaches). A needed
/// pair whose every candidate source is full is counted in `skipped`. At most `2 (n - 1)` edges
/// are added (the paper's Theorem 4).
fn plan_reach_edges(
    adj: &[u128],
    mut score: impl FnMut(usize, usize) -> f32,
    mut can_add: impl FnMut(usize) -> bool,
) -> ReachPlan {
    let n = adj.len();
    let full: u128 = if n == 128 {
        u128::MAX
    } else {
        (1u128 << n) - 1
    };
    // closure: reach[i] bit j = i reaches j (including itself)
    let mut reach: Vec<u128> = adj
        .iter()
        .enumerate()
        .map(|(i, &b)| b | (1u128 << i))
        .collect();
    for k in 0..n {
        let rk = reach[k];
        for i in 0..n {
            if reach[i] & (1u128 << k) != 0 {
                reach[i] |= rk;
            }
        }
    }
    let mut plan = ReachPlan {
        added: Vec::new(),
        skipped: 0,
    };
    loop {
        // the set of unsatisfied pairs
        let mut best: Option<(f32, usize, usize)> = None;
        let mut any_unsatisfied = false;
        for i in 0..n {
            let missing = full & !reach[i];
            if missing == 0 {
                continue;
            }
            any_unsatisfied = true;
            if !can_add(i) {
                continue;
            }
            let mut tg = missing;
            while tg != 0 {
                let j = tg.trailing_zeros() as usize;
                tg &= tg - 1;
                let sc = score(i, j);
                if best.is_none_or(|b| sc > b.0 || (sc == b.0 && (i, j) < (b.1, b.2))) {
                    best = Some((sc, i, j));
                }
            }
        }
        if !any_unsatisfied {
            break;
        }
        let Some((_, s, t)) = best else {
            // something is unsatisfied but no source can take an edge
            plan.skipped += (0..n)
                .map(|i| (full & !reach[i]).count_ones() as usize)
                .sum::<usize>();
            break;
        };
        // The chosen source is the closest to *some* missing target: add s -> t and update the
        // closure.
        plan.added.push((s, t));
        let rt = reach[t];
        for i in 0..n {
            if reach[i] & (1u128 << s) != 0 {
                reach[i] |= rt | (1u128 << t);
            }
        }
        if plan.added.len() > 2 * n {
            // cannot happen (Theorem 4); guard against a logic error looping forever
            break;
        }
    }
    plan
}

/// Step D: `new_links` = the selected targets (selection order), then `u`'s original links
/// nearest first (score to `u` descending, ties by id; `original` is reordered in place), skipping
/// duplicates, truncated to `m0`. The order matters only at the cut: a builder's list is roughly
/// nearest-first already, but a list read back from a stored graph is in id order, and cutting
/// that dropped arbitrary links (measured: 0.011-0.015 mass-weighted recall on L15H3).
fn compose_links(
    u: PointOffsetType,
    selected: &[PointOffsetType],
    original: &mut Vec<PointOffsetType>,
    m0: usize,
    scorer: &FilteredScorer<'_>,
    new_links: &mut Vec<PointOffsetType>,
) {
    if original.len() > 1 {
        let mut scored: Vec<(f32, PointOffsetType)> = original
            .iter()
            .map(|&l| (scorer.score_internal(u, l), l))
            .collect();
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        original.clear();
        original.extend(scored.into_iter().map(|(_, l)| l));
    }
    new_links.clear();
    new_links.extend_from_slice(selected);
    new_links.truncate(m0);
    for &link in original.iter() {
        if new_links.len() >= m0 {
            break;
        }
        if !new_links.contains(&link) {
            new_links.push(link);
        }
    }
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
mod reach_tests {
    use super::*;

    /// Nodes 0..4 in rank order; 0 -> 1 -> 2 linked, 3 isolated. Score = -|i - j| (closer ranks
    /// closer). The first edge chosen is the closest unreached pair with a source that can add:
    /// 1 -> 0, 2 -> 1, 2 -> 3 and 3 -> 2 tie on score, and the tie goes to the smallest
    /// (source, target).
    #[test]
    fn first_edge_is_the_closest_unreached_pair() {
        let adj = vec![0b0010u128, 0b0100, 0b0000, 0b0000];
        let plan = plan_reach_edges(&adj, |i, j| -((i as f32 - j as f32).abs()), |_| true);
        assert_eq!(plan.added[0], (1, 0));
        assert_eq!(plan.skipped, 0);
    }

    /// Same graph: the planner must terminate with every pair connected and at most
    /// 2(n-1) = 6 edges.
    #[test]
    fn all_pairs_terminates_fully_connected() {
        let adj = vec![0b0010u128, 0b0100, 0b0000, 0b0000];
        let plan = plan_reach_edges(&adj, |i, j| -((i as f32 - j as f32).abs()), |_| true);
        assert!(plan.added.len() <= 6, "{:?}", plan.added);
        assert_eq!(plan.skipped, 0);
        // replay the plan on the adjacency and check the closure is complete
        let mut a = adj.clone();
        for &(s, t) in &plan.added {
            a[s] |= 1u128 << t;
        }
        let n = a.len();
        let mut reach: Vec<u128> = a
            .iter()
            .enumerate()
            .map(|(i, &b)| b | (1u128 << i))
            .collect();
        for k in 0..n {
            let rk = reach[k];
            for i in 0..n {
                if reach[i] & (1u128 << k) != 0 {
                    reach[i] |= rk;
                }
            }
        }
        assert!(reach.iter().all(|&r| r == 0b1111), "{reach:?}");
    }

    /// A full source is skipped: only node 0 may add edges, so every added edge starts at 0.
    #[test]
    fn a_full_source_is_skipped_for_the_next_closest() {
        let adj = vec![0b0010u128, 0b0100, 0b0000, 0b0000];
        let plan = plan_reach_edges(&adj, |i, j| -((i as f32 - j as f32).abs()), |i| i == 0);
        assert_eq!(plan.added[0], (0, 3));
        assert!(plan.added.iter().all(|&(s, _)| s == 0), "{:?}", plan.added);
        // 0 now reaches everything; the pairs with another source stay unsatisfied
        assert!(plan.skipped > 0);
    }

    /// Nothing can add: the missing pairs are reported as skipped and the planner stops.
    #[test]
    fn no_source_available_reports_skipped() {
        let adj = vec![0b0010u128, 0b0100, 0b0000, 0b0000];
        let plan = plan_reach_edges(&adj, |_, _| 0.0, |_| false);
        assert!(plan.added.is_empty());
        // 0 misses 3; 1 misses 0, 3; 2 misses 0, 1, 3; 3 misses 0, 1, 2
        assert_eq!(plan.skipped, 1 + 2 + 3 + 3);
    }

    /// Already connected: nothing to do.
    #[test]
    fn connected_graph_needs_nothing() {
        let adj = vec![0b0010u128, 0b0100, 0b1000, 0b0001];
        let plan = plan_reach_edges(&adj, |_, _| 0.0, |_| true);
        assert!(plan.added.is_empty());
        assert_eq!(plan.skipped, 0);
    }
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
    /// Point 0's original links `[4, 1, 5]` sort nearest first to `[1 (10°), 4 (90°), 5 (260°)]`.
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
            unit(260.0),
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

    /// Steps B-E through the public entry point, with the repair off unless the caller asked
    /// for it explicitly (the tests below pin step D's output, which the repair would rewrite).
    fn run(builder: &GraphLayersBuilder, params: &ProjectionParams) -> ProjectionStats {
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
        add_query_aware_projection_edges_from_lists(
            builder,
            params,
            &top_lists,
            topn,
            || {
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

    fn no_repair(params: ProjectionParams) -> ProjectionParams {
        ProjectionParams {
            repair_max_per_point: None,
            ..params
        }
    }

    /// Induced reachability inside one top list after the pass: does every node reach every
    /// other node walking only level-0 links between the list's nodes?
    fn fully_reachable(builder: &GraphLayersBuilder, row: &[PointOffsetType]) -> bool {
        let n = row.len();
        let mut adj = vec![0u128; n];
        let mut links = Vec::new();
        for (i, &u) in row.iter().enumerate() {
            builder.level0_links(u, &mut links);
            for &l in &links {
                if let Some(j) = row.iter().position(|&p| p == l) {
                    adj[i] |= 1 << j;
                }
            }
        }
        let plan = plan_reach_edges(&adj, |_, _| 0.0, |_| false);
        plan.skipped == 0
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
            &no_repair(ProjectionParams {
                proj_m: 4,
                ..Default::default()
            }),
        );

        // Projected [1, 3] first, then the original [4, 1, 5] with 1 deduplicated.
        assert_eq!(links_of(&builder, 0), vec![1, 3, 4, 5]);
        // Point 6 is in no top list, so it keeps its original links.
        assert_eq!(links_of(&builder, 6), vec![0, 1]);
        assert_eq!(stats.points_with_candidates, 6);
    }

    /// Point 1 is excluded: it is never selected as a target (point 0's first projected edge
    /// becomes 2, the next most co-retrieved candidate, instead of 1), but it keeps its place
    /// among point 0's original links, and it still takes part as a source (its own list is
    /// rewritten; for this fixture the rewrite happens to reproduce `[0, 2]`).
    #[test]
    fn excluded_targets_are_never_selected_but_keep_their_links() {
        let builder = build_fixture(4);
        let stats = run(
            &builder,
            &no_repair(ProjectionParams {
                proj_m: 4,
                excluded_targets: vec![1],
                ..Default::default()
            }),
        );
        let links = links_of(&builder, 0);
        assert_eq!(links[0], 2, "{links:?}");
        assert!(links[1..].contains(&1), "{links:?}");
        assert_eq!(stats.points_with_candidates, 6);
    }

    /// Step E: with the repair on and room in the lists (m0 = 8), every training query's top
    /// list is fully reachable through its own induced links afterwards; without it, the
    /// fixture's third list (`[5, 4, 3, 1]`, whose points only link to 0) is not.
    #[test]
    fn repair_makes_every_top_list_reachable() {
        let (top_lists, topn) = fixture_top_lists();
        let without = build_fixture(8);
        run(
            &without,
            &no_repair(ProjectionParams {
                proj_m: 4,
                ..Default::default()
            }),
        );
        assert!(!fully_reachable(&without, &top_lists[2 * topn..3 * topn]));

        let with = build_fixture(8);
        let stats = run(
            &with,
            &ProjectionParams {
                proj_m: 4,
                repair_max_per_point: Some(32),
                ..Default::default()
            },
        );
        for row in top_lists.chunks(topn) {
            assert!(fully_reachable(&with, row), "{row:?}");
        }
        assert!(stats.projected_edges > 0);
    }

    /// An excluded point is left out of the repair's neighbourhoods: with point 1 excluded the
    /// repair never adds a link into it, and its own list is not touched by the repair.
    #[test]
    fn repair_leaves_excluded_points_alone() {
        let builder = build_fixture(8);
        run(
            &builder,
            &ProjectionParams {
                proj_m: 0,
                excluded_targets: vec![1],
                repair_max_per_point: Some(32),
                ..Default::default()
            },
        );
        assert_eq!(links_of(&builder, 1), vec![0, 2]);
        let (top_lists, topn) = fixture_top_lists();
        for row in top_lists.chunks(topn) {
            let rest: Vec<PointOffsetType> = row.iter().copied().filter(|&p| p != 1).collect();
            assert!(fully_reachable(&builder, &rest), "{rest:?}");
        }
    }

    #[test]
    fn projection_truncates_to_m0() {
        let builder = build_fixture(3);
        run(
            &builder,
            &no_repair(ProjectionParams {
                proj_m: 4,
                ..Default::default()
            }),
        );
        assert_eq!(links_of(&builder, 0), vec![1, 3, 4]);
    }

    #[test]
    fn projection_respects_proj_m() {
        let builder = build_fixture(4);
        run(
            &builder,
            &no_repair(ProjectionParams {
                proj_m: 1,
                ..Default::default()
            }),
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
        let params = no_repair(ProjectionParams {
            proj_m: 4,
            proj_maxq: 1,
            ..Default::default()
        });
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
            &no_repair(ProjectionParams {
                proj_m: 0,
                ..Default::default()
            }),
        );
        for (point_id, original) in fixture_links().iter().enumerate() {
            assert_eq!(&links_of(&builder, point_id as PointOffsetType), original);
        }
    }
}
