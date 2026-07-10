mod generators;

use std::collections::BTreeSet;

use ahash::AHashSet;
use api::rest::RecommendStrategy;
use generators::{
    random_direction, random_distinct_ids, random_distinct_points, random_existing_ids, random_num,
    random_partial_named_vectors, random_payload, random_payload_key, random_payload_keys,
    random_point, random_prefetch, random_query_for_name, random_recommend_strategy,
    random_scroll_filter, random_tag, random_update_mode, random_url_prefix_probe,
    random_vector_name, random_vector_name_subset, random_with_payload, random_with_vector,
    upsert_fallback,
};
use rand::distr::weighted::WeightedIndex;
use rand::prelude::Distribution;
use rand::seq::{IndexedRandom, IteratorRandom};
use rand::{Rng, RngExt};
use segment::data_types::index::{KeywordIndexParams, KeywordIndexType};
use segment::data_types::order_by::Direction;
use segment::data_types::vector_name_config::{
    DenseVectorConfig, SparseVectorConfig, VectorNameConfig,
};
use segment::json_path::JsonPath;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HasIdCondition, HasVectorCondition, Match,
    MultiVectorConfig, Payload, PayloadFieldSchema, PayloadSchemaParams, PayloadSchemaType,
    PointIdType, VectorNameBuf, VectorStorageDatatype, WithPayloadInterface, WithVector,
};
use segment::vector_storage::turbo::turbo_storage_roundtrip;
use sparse::common::sparse_vector::SparseVector;

use super::{ALL_CANDIDATES, Model, ModelEntry, VectorKind, VectorValue, kind_of};
use crate::operations::point_ops::UpdateMode;

/// Operations driven against both the live `Collection` and the model.
#[derive(Debug, Clone)]
pub(super) enum Op {
    Upsert(PointIdType, NamedVectors, Payload),
    UpsertBatch(Vec<(PointIdType, NamedVectors, Payload)>),
    Delete(Vec<PointIdType>),
    DeleteByFilter(i64),
    SetPayload(Vec<PointIdType>, Payload),
    OverwritePayload(Vec<PointIdType>, Payload),
    DeletePayload(Vec<PointIdType>, Vec<JsonPath>),
    ClearPayload(Vec<PointIdType>),
    CreateIndex(JsonPath, PayloadFieldSchema),
    DropIndex(JsonPath),
    /// Retrieve verification: IDs sampled uniformly from `0..id_pool`. Most IDs won't be in
    /// the model — the assertion exercises engine⇄model agreement on missing IDs plus the
    /// occasional full per-id check on a hit.
    RetrieveRandom(Vec<PointIdType>),
    /// Retrieve verification: IDs sampled from the model so every requested ID exercises
    /// the full per-id vectors+payload equality check.
    RetrieveExisting(Vec<PointIdType>),
    /// Retrieve verification exercising the `with_payload`/`with_vector` *selectors* (rather than
    /// the plain `Bool` forms the other retrieve ops use). IDs are sampled from the model; the
    /// engine's returned payload/vectors must equal the model entry filtered by the same selector
    /// (payload via `PayloadSelector::process`, vectors via the requested name subset).
    RetrieveSelective {
        ids: Vec<PointIdType>,
        with_payload: WithPayloadInterface,
        with_vector: WithVector,
    },
    CountByNum(i64),
    /// Verification op: nearest-neighbor search. `exact=true` does a brute-force scan;
    /// `exact=false` goes through the HNSW (dense) or sparse-index path.
    /// We don't compare scores (float-flaky), only result size and id membership in the model.
    Search {
        vector_name: VectorNameBuf,
        query: VectorValue,
        limit: usize,
        exact: bool,
        filter_num: Option<i64>,
        filter_url_prefix: Option<String>,
    },
    /// Verification op: nearest-neighbor search via the Query API (`ScoringQuery::Vector(Nearest)`),
    /// the unified entrypoint that also backs prefetch/fusion. We exercise only the plain Nearest
    /// shape here for basic coverage. Like `Search`, scores are float-flaky so we check only result
    /// size bounds and id membership in the model.
    Query {
        vector_name: VectorNameBuf,
        query: VectorValue,
        limit: usize,
        exact: bool,
        filter_num: Option<i64>,
        filter_url_prefix: Option<String>,
    },
    /// Upsert that only applies to a point if the existing payload matches the filter
    /// (semantics depend on `mode`).
    UpsertConditional {
        points: Vec<(PointIdType, NamedVectors, Payload)>,
        condition_num: i64,
        mode: UpdateMode,
    },
    /// Replace a subset of named vectors on existing points; the rest stay untouched.
    /// `condition_num=None` updates blindly; `Some(n)` only updates points where `num == n`.
    /// Ids are sampled from the model so the no-filter path doesn't hit `PointIdError`.
    UpdateVectors {
        /// Each entry's map contains only the names being replaced on that point.
        points: Vec<(PointIdType, NamedVectors)>,
        condition_num: Option<i64>,
    },
    /// Drop specific named vectors from existing points. The point keeps payload + the
    /// remaining vectors. Ids are sampled from the model.
    DeleteVectors {
        ids: Vec<PointIdType>,
        names: Vec<VectorNameBuf>,
    },
    /// Same as `DeleteVectors` but driven by a `num == X` filter instead of an id list.
    ///
    /// NOTE: this is structurally similar to `DeleteByFilter` (which is currently disabled
    /// due to https://github.com/qdrant/qdrant/issues/9575). If post-reload count
    /// mismatches appear here, the same WAL-replay nondeterminism is the prime suspect.
    DeleteVectorsByFilter {
        num: i64,
        names: Vec<VectorNameBuf>,
    },
    /// Verification op: scroll all points matching `num == X`, compare id set to model.
    ScrollFilteredByNum(i64),
    /// Verification op: count points matching `tag == X`, compare to model count.
    CountByTag(String),
    /// Verification op: count points whose `url` starts with the given prefix.
    CountByUrlPrefix(String),
    /// Verification op: scroll all points matching `tag == X`, compare id set to model.
    ScrollFilteredByTag(String),
    /// Verification op: scroll all points whose `url` starts with the given prefix.
    ScrollFilteredByUrlPrefix(String),
    /// Verification op: scroll ordered by `num` (requires the eager `num` index).
    /// Asserts monotonic ordering and that the returned set covers every point with a `num`.
    ScrollOrdered(Direction),
    /// Verification op: recommend by point IDs. Positives and negatives are sampled from the
    /// model so they exist in the collection AND have the chosen `vector_name` populated
    /// (otherwise the engine errors with e.g. "Positive vectors should not be empty").
    Recommend {
        positive: Vec<PointIdType>,
        negative: Vec<PointIdType>,
        limit: usize,
        strategy: RecommendStrategy,
        vector_name: VectorNameBuf,
    },
    /// Add a new named vector to the collection schema. Existing points have no value for the
    /// new name until a later `UpdateVectors` fills it in. The name is drawn from the static
    /// pool (see `ALL_CANDIDATES`) and must currently be inactive.
    CreateVectorName {
        name: VectorNameBuf,
        config: VectorNameConfig,
    },
    /// Remove a named vector from the collection schema. All points lose any value for that
    /// name. At least one name must remain active; the random selector skips this op when
    /// the active set has only one entry.
    DeleteVectorName(VectorNameBuf),
    /// Merge `payload` into every point matching `num == X` (filter-driven `SetPayload`).
    SetPayloadByFilter {
        num: i64,
        payload: Payload,
    },
    /// Replace the whole payload of every point matching `num == X` (filter-driven
    /// `OverwritePayload`).
    OverwritePayloadByFilter {
        num: i64,
        payload: Payload,
    },
    /// Delete `keys` from the payload of every point matching `num == X` (filter-driven
    /// `DeletePayload`).
    DeletePayloadByFilter {
        num: i64,
        keys: Vec<JsonPath>,
    },
    /// Clear all payload from every point matching `num == X` (filter-driven `ClearPayload`).
    ClearPayloadByFilter(i64),
    /// Verification op: facet (count points per distinct value) on an always-indexed, facetable
    /// field (`num` or `b`), optionally restricted to `num == X`. Run with `exact=true` and a
    /// limit above the field's cardinality, so the full per-value count map is compared to the
    /// model exactly.
    Facet {
        key: &'static str,
        filter_num: Option<i64>,
        filter_url_prefix: Option<String>,
    },
    /// Set-payload scoped to a nested key path (`SetPayloadOp.key`): the engine inserts
    /// `payload` *under* `key` for each listed point (`merge_by_key` / `JsonPath::value_set`),
    /// rather than merging it at the payload root like plain `SetPayload`. Ids are sampled from
    /// the model so every target exists. `key` is a single top-level field name drawn from the
    /// fixed payload schema (e.g. `g`), so the assignment overwrites that field with `payload`.
    SetPayloadByKey {
        ids: Vec<PointIdType>,
        payload: Payload,
        key: JsonPath,
    },
    /// Verification op: scroll the (optionally filtered) collection in pages of `limit`, following
    /// `next_page_offset` until exhausted. Exercises the `offset` cursor and a real (non-`usize::MAX`)
    /// `limit` — the other scroll ops always read everything in one page. Asserts: each page holds
    /// at most `limit` points, no id repeats across pages, and the union of all pages equals the
    /// model's expected id set for the chosen filter.
    ScrollPaged {
        limit: usize,
        filter: ScrollFilter,
    },
    /// Verification op: a fused multi-source Query (`prefetch` + RRF/DBSF fusion) — the query-API
    /// shape the plain `Query` op never reaches. Each prefetch is an independent Nearest source
    /// (its own vector name, limit and optional `num` filter); the outer query fuses their union
    /// and an optional outer `num` filter applies on top. Fusion ranking is score-based and
    /// approximate, so — like `Search`/`Query` — we check only id membership (every result is some
    /// prefetch's candidate and passes the outer filter) and a result-size upper bound, never
    /// scores or order. One prefetch level only (no nesting).
    QueryFusion {
        prefetches: Vec<Prefetch>,
        fusion: FusionKind,
        limit: usize,
        filter_num: Option<i64>,
        filter_url_prefix: Option<String>,
    },
    /// Take a snapshot of the collection *in the background, concurrently with the ongoing workload*:
    /// the run loop spawns `create_snapshot` on a task and keeps applying ops while it runs, then
    /// discards the archive (no recovery). Exercises the snapshot-creation path under concurrent
    /// writes — the check is that it succeeds and doesn't corrupt the live collection (the ongoing
    /// verification ops + final reload catch that). Handled in the run loop (not `apply`) because it
    /// spawns a task against shared collection state.
    CreateSnapshot,
}

/// A single prefetch source for `QueryFusion`: a Nearest sub-query over one vector name, with its
/// own limit and optional `num` filter.
#[derive(Debug, Clone)]
pub(super) struct Prefetch {
    pub(super) vector_name: VectorNameBuf,
    pub(super) query: VectorValue,
    pub(super) limit: usize,
    pub(super) filter_num: Option<i64>,
}

/// Fusion method for `QueryFusion` (the engine's `api::rest::Fusion` isn't `Clone`, which `Op`
/// requires). Maps to `FusionInternal` at apply time.
#[derive(Debug, Clone, Copy)]
pub(super) enum FusionKind {
    Rrf,
    Dbsf,
}

/// Filter selector for `ScrollPaged`. Beyond the payload filters the other scroll verifiers
/// support, this also covers a `has_id` matcher (restrict to an explicit point-id set).
#[derive(Debug, Clone)]
pub(super) enum ScrollFilter {
    None,
    Num(i64),
    Tag(String),
    /// `has_id` matcher: only points whose id is in this set. The set mixes ids that exist in the
    /// model with ids drawn from the id pool that may not, so the matcher meaningfully restricts.
    HasId(Vec<PointIdType>),
    /// `has_vector` matcher: only points that have a value for the given named vector. The
    /// populated vector set varies per point (DeleteVectors / partial UpdateVectors), so this
    /// restricts to a model-checkable subset.
    HasVector(VectorNameBuf),
    /// `url` prefix matcher: only points whose `url` payload starts with the given prefix.
    UrlPrefix(String),
}

/// Per-point named-vector payload — used by Upsert/UpsertBatch/UpdateVectors/UpsertConditional.
pub(super) type NamedVectors = std::collections::BTreeMap<VectorNameBuf, VectorValue>;

/// Per-run workload selection in the sense of Groce et al., "Swarm Testing" (ISSTA 2012):
/// rather than enabling every op in every run under one fixed distribution, each run draws a
/// random *subset* of ops to enable (omitted ops get weight 0). Omitting features for a whole
/// run surfaces bugs the uniform mix suppresses — e.g. a run that never deletes lets segments
/// grow and stresses reload, while a delete-heavy run stresses tombstones and `deleted_mask`.
///
/// The config is drawn from the seeded rng once before the op loop, so a seed reproduces both
/// the swarm config and the op stream. The enabled set is logged to the trace as a `Swarm` line.
pub(super) struct Swarm {
    weights: [u32; Self::N],
}

impl Swarm {
    const N: usize = 38;

    /// Op names, aligned 1:1 with `BASE` and the `match` arms in `Op::random`.
    const NAMES: [&'static str; Self::N] = [
        "Upsert",
        "UpsertBatch",
        "Delete",
        "DeleteByFilter",
        "SetPayload",
        "OverwritePayload",
        "DeletePayload",
        "ClearPayload",
        "CreateIndex",
        "DropIndex",
        "RetrieveRandom",
        "CountByNum",
        "Search",
        "UpsertConditional",
        "UpdateVectors",
        "DeleteVectors",
        "DeleteVectorsByFilter",
        "ScrollFilteredByNum",
        "CountByTag",
        "ScrollFilteredByTag",
        "ScrollOrdered",
        "Recommend",
        "CreateVectorName",
        "DeleteVectorName",
        "RetrieveExisting",
        "SetPayloadByFilter",
        "OverwritePayloadByFilter",
        "DeletePayloadByFilter",
        "ClearPayloadByFilter",
        "Facet",
        "SetPayloadByKey",
        "RetrieveSelective",
        "ScrollPaged",
        "Query",
        "QueryFusion",
        "CountByUrlPrefix",
        "ScrollFilteredByUrlPrefix",
        "CreateSnapshot",
    ];

    /// Each op's *natural* relative weight — the default distribution before swarm masking.
    /// Whether an op actually fires is decided by `random()` with `FORCE_ON`/`FORCE_OFF`; a
    /// `FORCE_OFF` op keeps its natural weight here (never read while disabled), so re-enabling
    /// is just removing it from `FORCE_OFF`.
    //
    // Currently in `FORCE_OFF` (known-broken):
    // - CreateVectorName / DeleteVectorName (22, 23): (1) proxy-segment schema race (optimizer-on)
    //   → "missing / Not existing vector name"; (2) DeleteVectorName vs. storage incoherence
    //   (fires without the optimizer too) — `delete_named_vector` updates `CollectionParams` but
    //   doesn't reconcile on-disk segment storage or the WAL, so reload drops WAL-replayed points
    //   or re-exposes purged vector data. Re-enable once both are fixed.
    const BASE: [u32; Self::N] = [
        35, // Upsert
        15, // UpsertBatch
        12, // Delete
        3,  // DeleteByFilter
        8,  // SetPayload
        5,  // OverwritePayload
        5,  // DeletePayload
        4,  // ClearPayload
        3,  // CreateIndex
        2,  // DropIndex
        4,  // RetrieveRandom
        4,  // CountByNum
        6,  // Search
        5,  // UpsertConditional
        5,  // UpdateVectors
        4,  // DeleteVectors
        3,  // DeleteVectorsByFilter
        5,  // ScrollFilteredByNum
        4,  // CountByTag
        4,  // ScrollFilteredByTag
        4,  // ScrollOrdered
        4,  // Recommend
        1,  // CreateVectorName
        1,  // DeleteVectorName
        8,  // RetrieveExisting
        4,  // SetPayloadByFilter
        3,  // OverwritePayloadByFilter
        3,  // DeletePayloadByFilter
        3,  // ClearPayloadByFilter
        4,  // Facet
        4,  // SetPayloadByKey
        4,  // RetrieveSelective
        4,  // ScrollPaged
        6,  // Query
        5,  // QueryFusion
        4,  // CountByUrlPrefix
        4,  // ScrollFilteredByUrlPrefix
        // Background snapshot: cheap to *start* (the loop just spawns it), but it does real IO on a
        // blocking thread, so keep the weight modest.
        2, // CreateSnapshot
    ];

    /// Indices kept enabled in every swarm config: without a way to insert points the run can't
    /// make progress and most ops degrade to no-ops.
    const FORCE_ON: [usize; 1] = [0]; // Upsert

    /// Indices kept disabled in every swarm config: known-broken ops (see `BASE`).
    // DeleteByFilter (3) was here for https://github.com/qdrant/qdrant/issues/9575 — re-enabled
    // once filter ops are resolved to concrete ids before the WAL append.
    const FORCE_OFF: [usize; 2] = [22, 23]; // CreateVectorName, DeleteVectorName

    /// Index of the snapshot op (`CreateSnapshot`), masked off when `disable_snapshots` is set.
    /// Must stay aligned with `NAMES`/`BASE`.
    const SNAPSHOT_OPS: [usize; 1] = [37];

    /// Draw a per-run config: each non-forced op is included with probability 0.5 (keeping its
    /// base weight); omitted ops get weight 0.
    ///
    /// With `enable_force_off`, the known-broken `FORCE_OFF` ops are promoted to forced-on instead
    /// (enabled in every config) so a run can deliberately exercise them — see the CLI flag of the
    /// same name. They never draw `random_bool` either way (forced-off or forced-on), so the
    /// rng-draw count is identical regardless of the flag and a given seed reproduces the same op
    /// stream for the non-broken ops.
    ///
    /// With `disable_snapshots`, the snapshot ops are forced to weight 0. The masking is applied
    /// *after* the draw loop so each op (snapshots included) still draws its `random_bool` — the
    /// rng-draw count is unchanged, so toggling the flag doesn't shift the other ops' swarm draws.
    pub(super) fn random(
        rng: &mut impl Rng,
        enable_force_off: bool,
        disable_snapshots: bool,
    ) -> Self {
        let mut weights = Self::BASE;
        for (i, w) in weights.iter_mut().enumerate() {
            let forced_off = Self::FORCE_OFF.contains(&i);
            let forced_on = Self::FORCE_ON.contains(&i) || (enable_force_off && forced_off);
            // Short-circuit order matters: `random_bool` is only drawn for non-forced ops, so
            // the rng-draw count stays fixed (one per swarmable op) regardless of the forced
            // sets — keeping a given seed reproducible.
            let disable =
                (forced_off && !enable_force_off) || (!forced_on && !rng.random_bool(0.5));
            if disable {
                *w = 0;
            }
        }
        if disable_snapshots {
            for i in Self::SNAPSHOT_OPS {
                weights[i] = 0;
            }
        }
        Self { weights }
    }

    /// Names of the ops enabled (nonzero weight) in this config, for the trace `Swarm` line.
    pub(super) fn enabled_ops(&self) -> Vec<&'static str> {
        Self::NAMES
            .iter()
            .zip(self.weights)
            .filter(|(_, w)| *w > 0)
            .map(|(name, _)| *name)
            .collect()
    }

    fn distribution(&self) -> WeightedIndex<u32> {
        // FORCE_ON guarantees a nonzero total weight.
        WeightedIndex::new(self.weights).expect("swarm config has at least one enabled op")
    }
}

impl Op {
    pub(super) fn random(
        rng: &mut impl Rng,
        model: &Model,
        active: &BTreeSet<VectorNameBuf>,
        id_pool: u64,
        swarm: &Swarm,
    ) -> Self {
        // The op at index N below MUST line up with `Swarm::BASE`/`Swarm::NAMES` at the same
        // index. Adding a new op? Append at the END of all three — never insert in the middle,
        // or every arm after the insertion silently fires the wrong handler.
        let workload = swarm.distribution();

        match workload.sample(rng) {
            0 => {
                let (id, vecs, payload) = random_point(rng, active, id_pool);
                Op::Upsert(id, vecs, payload)
            }
            1 => Op::UpsertBatch(random_distinct_points(rng, active, 2..=5, id_pool)),
            2 => Op::Delete(random_distinct_ids(rng, 1..=3, id_pool)),
            3 => Op::DeleteByFilter(random_num(rng)),
            4 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::SetPayload(ids, random_payload(rng))
            }
            5 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::OverwritePayload(ids, random_payload(rng))
            }
            6 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::DeletePayload(ids, random_payload_keys(rng))
            }
            7 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::ClearPayload(ids)
            }
            8 => {
                // Randomly toggle one of two keyword indices, preserving coverage of both the
                // plain keyword-equality path (`tag`) and the prefix path (`url`):
                // - `tag`: plain keyword index, exercised by exact match ops;
                // - `url`: prefix-enabled keyword index, exercised by prefix match ops.
                // When the index is absent, the corresponding filters fall back to full-scan;
                // when present, the index path is exercised across segment variants.
                let (field, schema) = if rng.random_bool(0.5) {
                    (
                        "tag".parse().unwrap(),
                        PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
                    )
                } else {
                    ("url".parse().unwrap(), url_prefix_index_schema())
                };
                Op::CreateIndex(field, schema)
            }
            9 => {
                let field = if rng.random_bool(0.5) { "tag" } else { "url" };
                Op::DropIndex(field.parse().unwrap())
            }
            10 => Op::RetrieveRandom(random_distinct_ids(rng, 3..=10, id_pool)),
            11 => Op::CountByNum(random_num(rng)),
            12 => {
                let vector_name = random_vector_name(rng, active);
                let query = random_query_for_name(rng, &vector_name);
                Op::Search {
                    vector_name,
                    query,
                    limit: rng.random_range(1..=10),
                    exact: rng.random_bool(0.5),
                    filter_num: rng.random_bool(0.5).then(|| random_num(rng)),
                    filter_url_prefix: rng
                        .random_bool(0.5)
                        .then(|| random_url_prefix_probe(rng).to_string()),
                }
            }
            13 => Op::UpsertConditional {
                points: random_distinct_points(rng, active, 1..=3, id_pool),
                condition_num: random_num(rng),
                mode: random_update_mode(rng),
            },
            14 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                let mut points = Vec::with_capacity(ids.len());
                for id in ids {
                    points.push((id, random_partial_named_vectors(rng, active)));
                }
                let condition_num = rng.random_bool(0.5).then(|| random_num(rng));
                Op::UpdateVectors {
                    points,
                    condition_num,
                }
            }
            15 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::DeleteVectors {
                    ids,
                    names: random_vector_name_subset(rng, active),
                }
            }
            16 => Op::DeleteVectorsByFilter {
                num: random_num(rng),
                names: random_vector_name_subset(rng, active),
            },
            17 => Op::ScrollFilteredByNum(random_num(rng)),
            18 => Op::CountByTag(random_tag(rng).to_string()),
            19 => Op::ScrollFilteredByTag(random_tag(rng).to_string()),
            20 => Op::ScrollOrdered(random_direction(rng)),
            21 => {
                // Positives + negatives must both have the chosen vector populated, else the
                // engine errors (e.g. "Positive vectors should not be empty with `average`").
                let vector_name = random_vector_name(rng, active);
                let eligible: Vec<PointIdType> = model
                    .iter()
                    .filter(|(_, entry)| entry.vectors.contains_key(&vector_name))
                    .map(|(id, _)| *id)
                    .collect();
                if eligible.is_empty() {
                    return upsert_fallback(rng, active, id_pool);
                }
                let n_pos = rng.random_range(1..=3).min(eligible.len());
                let positive: Vec<PointIdType> = eligible.iter().copied().sample(rng, n_pos);
                let pos_set: AHashSet<PointIdType> = positive.iter().copied().collect();
                let n_neg = rng.random_range(0..=2);
                let negative: Vec<PointIdType> = eligible
                    .iter()
                    .copied()
                    .filter(|id| !pos_set.contains(id))
                    .sample(rng, n_neg);
                Op::Recommend {
                    positive,
                    negative,
                    limit: rng.random_range(1..=5),
                    strategy: random_recommend_strategy(rng),
                    vector_name,
                }
            }
            22 => {
                // CreateVectorName: pick a candidate currently absent from the active set.
                // If all candidates are active, fall back to a regular upsert.
                let inactive: Vec<&'static super::VectorCandidate> = ALL_CANDIDATES
                    .iter()
                    .filter(|c| !active.contains(c.name))
                    .collect();
                if inactive.is_empty() {
                    return upsert_fallback(rng, active, id_pool);
                }
                let pick = inactive.choose(rng).unwrap();
                let config = match pick.kind {
                    VectorKind::Dense(dim) => VectorNameConfig::dense(DenseVectorConfig {
                        size: dim as usize,
                        distance: Distance::Dot,
                        multivector_config: None,
                        datatype: None,
                    }),
                    VectorKind::Sparse => VectorNameConfig::sparse(SparseVectorConfig {
                        modifier: None,
                        datatype: None,
                    }),
                    VectorKind::MultiDense(dim) => VectorNameConfig::dense(DenseVectorConfig {
                        size: dim as usize,
                        distance: Distance::Dot,
                        multivector_config: Some(MultiVectorConfig::default()),
                        datatype: None,
                    }),
                    VectorKind::DenseTurbo(dim) => VectorNameConfig::dense(DenseVectorConfig {
                        size: dim as usize,
                        distance: Distance::Dot,
                        multivector_config: None,
                        datatype: Some(VectorStorageDatatype::Turbo4),
                    }),
                };
                Op::CreateVectorName {
                    name: pick.name.to_string(),
                    config,
                }
            }
            23 => {
                // DeleteVectorName: at least one name must remain active.
                if active.len() < 2 {
                    return upsert_fallback(rng, active, id_pool);
                }
                let name = active.iter().choose(rng).unwrap().clone();
                Op::DeleteVectorName(name)
            }
            24 => {
                let Some(ids) = random_existing_ids(rng, model, 10) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::RetrieveExisting(ids)
            }
            25 => Op::SetPayloadByFilter {
                num: random_num(rng),
                payload: random_payload(rng),
            },
            26 => Op::OverwritePayloadByFilter {
                num: random_num(rng),
                payload: random_payload(rng),
            },
            27 => Op::DeletePayloadByFilter {
                num: random_num(rng),
                keys: random_payload_keys(rng),
            },
            28 => Op::ClearPayloadByFilter(random_num(rng)),
            29 => Op::Facet {
                // Always-indexed, facetable fields (`num`: Integer, `b`: Bool).
                key: ["num", "b"].choose(rng).copied().unwrap(),
                filter_num: rng.random_bool(0.5).then(|| random_num(rng)),
                filter_url_prefix: rng
                    .random_bool(0.5)
                    .then(|| random_url_prefix_probe(rng).to_string()),
            },
            30 => {
                let Some(ids) = random_existing_ids(rng, model, 3) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::SetPayloadByKey {
                    ids,
                    payload: random_payload(rng),
                    key: random_payload_key(rng),
                }
            }
            31 => {
                let Some(ids) = random_existing_ids(rng, model, 10) else {
                    return upsert_fallback(rng, active, id_pool);
                };
                Op::RetrieveSelective {
                    ids,
                    with_payload: random_with_payload(rng),
                    with_vector: random_with_vector(rng, active),
                }
            }
            32 => Op::ScrollPaged {
                // Small page size relative to id_pool so multi-page pagination actually happens.
                limit: rng.random_range(1..=20),
                filter: random_scroll_filter(rng, active, id_pool),
            },
            33 => {
                let vector_name = random_vector_name(rng, active);
                let query = random_query_for_name(rng, &vector_name);
                Op::Query {
                    vector_name,
                    query,
                    limit: rng.random_range(1..=10),
                    exact: rng.random_bool(0.5),
                    filter_num: rng.random_bool(0.5).then(|| random_num(rng)),
                    filter_url_prefix: rng
                        .random_bool(0.5)
                        .then(|| random_url_prefix_probe(rng).to_string()),
                }
            }
            34 => {
                // 1-3 independent Nearest prefetch sources, fused by RRF or DBSF. Vector names are
                // drawn per prefetch so sources can mix metrics/kinds; an optional outer filter
                // applies on top of the fused union.
                let n_prefetch = rng.random_range(1..=3);
                let prefetches = (0..n_prefetch)
                    .map(|_| random_prefetch(rng, active))
                    .collect();
                let fusion = if rng.random_bool(0.5) {
                    FusionKind::Rrf
                } else {
                    FusionKind::Dbsf
                };
                Op::QueryFusion {
                    prefetches,
                    fusion,
                    limit: rng.random_range(1..=10),
                    filter_num: rng.random_bool(0.5).then(|| random_num(rng)),
                    filter_url_prefix: rng
                        .random_bool(0.5)
                        .then(|| random_url_prefix_probe(rng).to_string()),
                }
            }
            35 => Op::CountByUrlPrefix(random_url_prefix_probe(rng).to_string()),
            36 => Op::ScrollFilteredByUrlPrefix(random_url_prefix_probe(rng).to_string()),
            37 => Op::CreateSnapshot,
            n => panic!("unexpected op index {n}"),
        }
    }

    /// Variant name only, no payload — suitable for the progress bar message
    /// (the full `{:?}` is hundreds of characters per op).
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Op::Upsert(..) => "Upsert",
            Op::UpsertBatch(_) => "UpsertBatch",
            Op::Delete(_) => "Delete",
            Op::DeleteByFilter(_) => "DeleteByFilter",
            Op::SetPayload(..) => "SetPayload",
            Op::OverwritePayload(..) => "OverwritePayload",
            Op::DeletePayload(..) => "DeletePayload",
            Op::ClearPayload(_) => "ClearPayload",
            Op::CreateIndex(..) => "CreateIndex",
            Op::DropIndex(_) => "DropIndex",
            Op::RetrieveRandom(_) => "RetrieveRandom",
            Op::RetrieveExisting(_) => "RetrieveExisting",
            Op::CountByNum(_) => "CountByNum",
            Op::Search { .. } => "Search",
            Op::Query { .. } => "Query",
            Op::UpsertConditional { .. } => "UpsertConditional",
            Op::UpdateVectors { .. } => "UpdateVectors",
            Op::DeleteVectors { .. } => "DeleteVectors",
            Op::DeleteVectorsByFilter { .. } => "DeleteVectorsByFilter",
            Op::ScrollFilteredByNum(_) => "ScrollFilteredByNum",
            Op::CountByTag(_) => "CountByTag",
            Op::CountByUrlPrefix(_) => "CountByUrlPrefix",
            Op::ScrollFilteredByTag(_) => "ScrollFilteredByTag",
            Op::ScrollFilteredByUrlPrefix(_) => "ScrollFilteredByUrlPrefix",
            Op::ScrollOrdered(_) => "ScrollOrdered",
            Op::Recommend { .. } => "Recommend",
            Op::CreateVectorName { .. } => "CreateVectorName",
            Op::DeleteVectorName(_) => "DeleteVectorName",
            Op::SetPayloadByFilter { .. } => "SetPayloadByFilter",
            Op::OverwritePayloadByFilter { .. } => "OverwritePayloadByFilter",
            Op::DeletePayloadByFilter { .. } => "DeletePayloadByFilter",
            Op::ClearPayloadByFilter(_) => "ClearPayloadByFilter",
            Op::Facet { .. } => "Facet",
            Op::SetPayloadByKey { .. } => "SetPayloadByKey",
            Op::RetrieveSelective { .. } => "RetrieveSelective",
            Op::ScrollPaged { .. } => "ScrollPaged",
            Op::QueryFusion { .. } => "QueryFusion",
            Op::CreateSnapshot => "CreateSnapshot",
        }
    }
}

// ───── shared filter constructors + predicates ─────────────────────────────

fn url_prefix_index_schema() -> PayloadFieldSchema {
    PayloadFieldSchema::FieldParams(PayloadSchemaParams::Keyword(KeywordIndexParams {
        r#type: KeywordIndexType::Keyword,
        prefix: Some(true),
        ..Default::default()
    }))
}

pub(super) fn match_url_prefix_filter(prefix: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        "url".parse().unwrap(),
        Match::new_prefix(prefix),
    )))
}

/// Optional combined filter for read ops that may restrict by `num` and/or `url` prefix.
pub(super) fn optional_read_filter(
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
) -> Option<Filter> {
    Filter::merge_opts(
        filter_num.map(match_num_filter),
        filter_url_prefix.map(match_url_prefix_filter),
    )
}

pub(super) fn match_num_filter(num: i64) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        "num".parse().unwrap(),
        Match::from(num),
    )))
}

pub(super) fn match_tag_filter(tag: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        "tag".parse().unwrap(),
        Match::from(tag.to_string()),
    )))
}

pub(super) fn match_has_id_filter(ids: &[PointIdType]) -> Filter {
    Filter::new_must(Condition::HasId(
        ids.iter().copied().collect::<HasIdCondition>(),
    ))
}

pub(super) fn match_has_vector_filter(name: &str) -> Filter {
    Filter::new_must(Condition::HasVector(HasVectorCondition::from(
        name.to_string(),
    )))
}

pub(super) fn num_matches(payload: &Payload, target: i64) -> bool {
    payload
        .0
        .get("num")
        .and_then(|v| v.as_i64())
        .is_some_and(|n| n == target)
}

pub(super) fn tag_matches(payload: &Payload, target: &str) -> bool {
    payload
        .0
        .get("tag")
        .and_then(|v| v.as_str())
        .is_some_and(|t| t == target)
}

pub(super) fn url_prefix_matches(payload: &Payload, prefix: &str) -> bool {
    payload
        .0
        .get("url")
        .and_then(|v| v.as_str())
        .is_some_and(|url| url.starts_with(prefix))
}

pub(super) fn passes_read_filters(
    payload: &Payload,
    filter_num: Option<i64>,
    filter_url_prefix: Option<&str>,
) -> bool {
    filter_num.is_none_or(|n| num_matches(payload, n))
        && filter_url_prefix.is_none_or(|p| url_prefix_matches(payload, p))
}

pub(super) fn has_num(payload: &Payload) -> bool {
    payload.0.get("num").and_then(|v| v.as_i64()).is_some()
}

/// Build a new `ModelEntry` from a fresh upsert.
pub(super) fn model_entry_from(vecs: &NamedVectors, payload: &Payload) -> ModelEntry {
    ModelEntry {
        vectors: vecs
            .iter()
            .map(|(name, value)| (name.clone(), model_vector(name, value)))
            .collect(),
        payload: payload.clone(),
    }
}

/// Predicted engine read-back for `value` stored under `name`. Turbo4-backed dense
/// vectors are lossy: the engine stores 4-bit quantized codes and returns the
/// dequantized vector, so the model must record that round-trip instead of the inserted
/// value. The round-trip is deterministic (fixed rotation seeds), shared across
/// segments and reloads. The engine still receives the original vector: the round-trip
/// is not idempotent (re-quantizing a read-back shifts the stored norm), so
/// canonicalizing at generation time would not converge.
pub(super) fn model_vector(name: &str, value: &VectorValue) -> VectorValue {
    match (kind_of(name), value) {
        (VectorKind::DenseTurbo(_), VectorValue::Dense(v)) => {
            // Every fixture vector uses Dot (see `fixture::fixture` and the
            // CreateVectorName generator arm above).
            VectorValue::Dense(turbo_storage_roundtrip(v, Distance::Dot))
        }
        (
            VectorKind::Dense(_) | VectorKind::Sparse | VectorKind::MultiDense(_),
            VectorValue::Dense(_) | VectorValue::Sparse(_) | VectorValue::MultiDense(_),
        ) => value.clone(),
        (VectorKind::DenseTurbo(_), VectorValue::Sparse(_) | VectorValue::MultiDense(_)) => {
            panic!("model_vector: non-dense value for Turbo4 name `{name}`: {value:?}")
        }
    }
}

/// Compare a returned dense vector against the model's prediction for `name`.
/// Exact for full-precision names. Turbo4 read-backs are compared with a tiny
/// relative tolerance: a copy-on-write point move re-quantizes the dequantized
/// read-back, and although the codes and the centroid norm are reproduced, the
/// re-measured stored norm passes through two f64 rotation round-trips and can
/// land a few ulps off, uniformly rescaling the read-back at ulp scale. The
/// budget of 16 ulps (relative) absorbs that wobble even accumulated across
/// repeated moves, while real divergences (wrong codes, stale vector, a bad
/// norm divisor) are orders of magnitude larger. Purely relative on purpose:
/// an absolute floor would accept sign flips of near-zero components.
pub(super) fn dense_matches(name: &str, actual: &[f32], expected: &[f32]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    match kind_of(name) {
        VectorKind::DenseTurbo(_) => actual.iter().zip(expected).all(|(&a, &e)| {
            let tol = 16.0 * f32::EPSILON * f32::max(a.abs(), e.abs());
            (a - e).abs() <= tol
        }),
        VectorKind::Dense(_) | VectorKind::Sparse | VectorKind::MultiDense(_) => actual == expected,
    }
}

/// Human-readable breakdown of a dense mismatch for panic messages: per-component
/// deltas plus a uniform-scale probe. A uniform engine/model ratio across all
/// components is the signature of a re-quantization rescale (e.g. the Turbo4
/// copy-on-write degradation, where read-backs come back scaled by `cn/sqrt(d)`),
/// as opposed to per-component noise or a stale/wrong vector.
pub(super) fn dense_diff(actual: &[f32], expected: &[f32]) -> String {
    if actual.len() != expected.len() {
        return format!(
            "length mismatch: engine {} vs model {}",
            actual.len(),
            expected.len(),
        );
    }
    let diffs: Vec<f32> = actual.iter().zip(expected).map(|(&a, &e)| a - e).collect();
    let max_abs_diff = diffs.iter().fold(0.0f32, |m, d| m.max(d.abs()));
    let ratios: Vec<f32> = actual
        .iter()
        .zip(expected)
        .map(|(&a, &e)| if e.abs() > 1e-12 { a / e } else { f32::NAN })
        .collect();
    // Judge uniformity on the finite ratios only: a near-zero expected component
    // yields a NaN ratio, and a genuinely uniform rescale should still be labeled
    // as such when one component sits at zero.
    let finite: Vec<f32> = ratios.iter().copied().filter(|r| r.is_finite()).collect();
    let uniform = !finite.is_empty() && finite.iter().all(|r| (r - finite[0]).abs() < 1e-5);
    let scale_note = if uniform {
        format!(
            "UNIFORM engine/model scale {:.6} (single rescale)",
            finite[0]
        )
    } else {
        "non-uniform ratios (per-component divergence)".to_string()
    };
    format!("max_abs_diff={max_abs_diff:e}; diffs={diffs:?}; ratios={ratios:?}; {scale_note}")
}

/// Sort sparse indices ascending and drop entries with zero value (mirrors the engine's
/// canonicalization on read — see `lib/sparse/src/common/sparse_vector.rs`).
pub(super) fn canonical_sparse(sv: &SparseVector) -> SparseVector {
    let mut pairs: Vec<(u32, f32)> = sv
        .indices
        .iter()
        .zip(sv.values.iter())
        .filter(|(_, v)| **v != 0.0)
        .map(|(i, v)| (*i, *v))
        .collect();
    pairs.sort_by_key(|&(i, _)| i);
    SparseVector {
        indices: pairs.iter().map(|(i, _)| *i).collect(),
        values: pairs.iter().map(|(_, v)| *v).collect(),
    }
}
