use std::collections::BTreeSet;

use ahash::AHashSet;
use api::rest::RecommendStrategy;
use rand::seq::{IndexedRandom, IteratorRandom};
use rand::{Rng, RngExt};
use segment::data_types::order_by::Direction;
use segment::json_path::JsonPath;
use segment::types::{
    ExtendedPointId, Payload, PayloadSelector, PayloadSelectorExclude, PayloadSelectorInclude,
    PointIdType, VectorNameBuf, WithPayloadInterface, WithVector,
};
use serde_json::{Map, Value};
use sparse::common::sparse_vector::SparseVector;

use super::super::{Model, VectorKind, VectorValue, candidate_of};
use super::{NamedVectors, Op, Prefetch, ScrollFilter, canonical_sparse};
use crate::operations::point_ops::UpdateMode;
use crate::operations::types::Datatype;

// ───── payload value pools ────────────────────────────────────────────────
//
// Each non-numeric field is sampled from a small discrete pool. Pools are kept small enough
// that filter ranges have plenty of "clearly in" and "clearly out" points to avoid floating-
// point boundary flakes between the engine and the model.

const TAGS: [&str; 3] = ["a", "b", "c"];
const FLOATS: [f64; 5] = [0.0, 0.25, 0.5, 0.75, 1.0];
const DATETIMES: [&str; 5] = [
    "2023-01-15T00:00:00Z",
    "2023-04-15T00:00:00Z",
    "2023-07-15T00:00:00Z",
    "2023-10-15T00:00:00Z",
    "2023-12-31T00:00:00Z",
];
const TEXTS: [&str; 4] = [
    "alpha beta gamma",
    "alpha delta",
    "beta epsilon",
    "gamma delta zeta",
];
// Identifier-like strings with shared prefixes — exercises keyword prefix index + filter.
const URLS: [&str; 6] = [
    "https://qdrant.tech",
    "https://qdrant.tech/docs",
    "https://qdrant.tech/blog",
    "https://example.com",
    "http://example.com",
    "ftp://files.example.com",
];
const URL_PREFIX_PROBES: [&str; 8] = [
    "https://qdrant.",
    "https://",
    "http",
    "ftp://",
    "https://example.com",
    "nonexistent",
    "", // matches every point that has `url`
    "qdrant",
];
// Geo coords live in a small EU-shaped rectangle, snapped to 0.1 degree.
const GEO_LAT_LO: f64 = 40.0;
const GEO_LAT_HI: f64 = 50.0;
const GEO_LON_LO: f64 = -10.0;
const GEO_LON_HI: f64 = 10.0;

// Sparse vector generation parameters.
const SPARSE_DIM_SPACE: u32 = 32; // indices are drawn from 0..SPARSE_DIM_SPACE
const SPARSE_NNZ_MIN: usize = 1; // at least one nonzero so the engine has something to index
const SPARSE_NNZ_MAX: usize = 6;

pub(super) fn random_tag(rng: &mut impl Rng) -> &'static str {
    TAGS.choose(rng).unwrap()
}

pub(super) fn random_url(rng: &mut impl Rng) -> &'static str {
    URLS.choose(rng).unwrap()
}

pub(super) fn random_url_prefix_probe(rng: &mut impl Rng) -> &'static str {
    URL_PREFIX_PROBES.choose(rng).unwrap()
}

fn random_id(rng: &mut impl Rng, id_pool: u64) -> PointIdType {
    ExtendedPointId::NumId(rng.random_range(0..id_pool))
}

pub(super) fn random_num(rng: &mut impl Rng) -> i64 {
    rng.random_range(0..100i64)
}

pub(super) fn random_distinct_ids(
    rng: &mut impl Rng,
    range: std::ops::RangeInclusive<usize>,
    id_pool: u64,
) -> Vec<PointIdType> {
    let n = rng.random_range(range);
    let mut seen = AHashSet::new();
    let mut ids = Vec::with_capacity(n);
    while ids.len() < n {
        let id = random_id(rng, id_pool);
        if seen.insert(id) {
            ids.push(id);
        }
    }
    ids
}

/// Sample up to `max` ids from the model. Returns `None` if the model is empty so the caller
/// can fall back to an Upsert (see `upsert_fallback`).
pub(super) fn random_existing_ids(
    rng: &mut impl Rng,
    model: &Model,
    max: usize,
) -> Option<Vec<PointIdType>> {
    if model.is_empty() {
        return None;
    }
    let n = rng.random_range(1..=max).min(model.len());
    Some(model.keys().copied().sample(rng, n))
}

/// When an op needs existing ids but the model is empty, replace it with a simple Upsert so
/// the workload still makes progress.
pub(super) fn upsert_fallback(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
    id_pool: u64,
) -> Op {
    let (id, vecs, payload) = random_point(rng, active, id_pool);
    Op::Upsert(id, vecs, payload)
}

pub(super) fn random_payload_keys(rng: &mut impl Rng) -> Vec<JsonPath> {
    let all_keys = ["num", "tag", "url", "f", "b", "d", "g", "t"];
    let mut keys: Vec<JsonPath> = all_keys
        .iter()
        .filter(|_| rng.random_bool(0.4))
        .map(|k| k.parse().unwrap())
        .collect();
    // Guarantee at least one key so DeletePayload always has work to do.
    if keys.is_empty() {
        let pick = all_keys.choose(rng).unwrap();
        keys.push(pick.parse().unwrap());
    }
    keys
}

/// A single top-level payload field path to scope a keyed `SetPayload` under (`SetPayloadOp.key`).
/// Restricted to one of the fixed schema's top-level fields so the engine's `merge_by_key`
/// (which assigns the source payload as the value at this path) maps to a simple field overwrite
/// the model can mirror via `Payload::merge_by_key`.
pub(super) fn random_payload_key(rng: &mut impl Rng) -> JsonPath {
    let all_keys = ["num", "tag", "url", "f", "b", "d", "g", "t"];
    all_keys.choose(rng).unwrap().parse().unwrap()
}

/// A `with_payload` selector exercising every `WithPayloadInterface` form (`Bool`, `Fields`,
/// `Selector::Include`, `Selector::Exclude`). Keys are drawn from the fixed top-level payload
/// schema so the model can mirror the engine's `PayloadSelector::process` exactly.
pub(super) fn random_with_payload(rng: &mut impl Rng) -> WithPayloadInterface {
    // 1-3 distinct top-level keys for the field/selector variants.
    let n = rng.random_range(1..=3);
    let keys: Vec<JsonPath> = ["num", "tag", "url", "f", "b", "d", "g", "t"]
        .iter()
        .copied()
        .sample(rng, n)
        .into_iter()
        .map(|k| k.parse().unwrap())
        .collect();
    match rng.random_range(0..5) {
        0 => WithPayloadInterface::Bool(true),
        1 => WithPayloadInterface::Bool(false),
        2 => WithPayloadInterface::Fields(keys),
        3 => WithPayloadInterface::Selector(PayloadSelector::Include(PayloadSelectorInclude::new(
            keys,
        ))),
        4 => WithPayloadInterface::Selector(PayloadSelector::Exclude(PayloadSelectorExclude::new(
            keys,
        ))),
        _ => unreachable!(),
    }
}

/// A `with_vector` selector exercising both `WithVector::Bool` forms and
/// `WithVector::Selector(names)` over a subset of currently-active vector names.
pub(super) fn random_with_vector(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
) -> WithVector {
    match rng.random_range(0..3) {
        0 => WithVector::Bool(true),
        1 => WithVector::Bool(false),
        2 => WithVector::Selector(random_vector_name_subset(rng, active)),
        _ => unreachable!(),
    }
}

/// A filter selector for paginated scroll: no filter, `num == X`, `tag == X`, a `has_id`
/// matcher, or a `has_vector` matcher over a currently-active vector name.
pub(super) fn random_scroll_filter(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
    id_pool: u64,
) -> ScrollFilter {
    match rng.random_range(0..6) {
        0 => ScrollFilter::None,
        1 => ScrollFilter::Num(random_num(rng)),
        2 => ScrollFilter::Tag(random_tag(rng).to_string()),
        // `has_id` over 1-15 ids drawn from the pool — some present, some not — so the matcher
        // restricts to a known, model-checkable set. Clamp the count to `id_pool` so
        // `random_distinct_ids` can't spin forever trying to draw more distinct ids than exist
        // (relevant only for tiny `--id-pool` values; the default pool is far above 15).
        3 => ScrollFilter::HasId(random_distinct_ids(
            rng,
            1..=(id_pool as usize).min(15),
            id_pool,
        )),
        // `has_vector` over an active name: points keep all active vectors on upsert but
        // `DeleteVectors`/`UpdateVectors`/`CreateVectorName` make the populated set vary per point,
        // so this meaningfully restricts.
        4 => ScrollFilter::HasVector(random_vector_name(rng, active)),
        5 => ScrollFilter::UrlPrefix(random_url_prefix_probe(rng).to_string()),
        _ => unreachable!(),
    }
}

// ───── vector generators ────────────────────────────────────────────────────

/// Uniform components in `0.0..1.0`, except Uint8-backed names: their storage truncates
/// each component with `x as u8`, so a unit-range draw would collapse every vector to
/// zeros. Draw from the full byte range instead (the model records the truncated
/// read-back, see `model_vector`).
fn random_dense_vec(rng: &mut impl Rng, dim: u64, datatype: Option<Datatype>) -> Vec<f32> {
    let upper = match datatype {
        Some(Datatype::Uint8) => 256.0,
        None | Some(Datatype::Float32 | Datatype::Float16 | Datatype::Turbo4) => 1.0,
    };
    (0..dim).map(|_| rng.random_range(0.0..upper)).collect()
}

fn random_sparse_vector(rng: &mut impl Rng) -> SparseVector {
    let nnz = rng.random_range(SPARSE_NNZ_MIN..=SPARSE_NNZ_MAX);
    let mut indices: Vec<u32> = (0..SPARSE_DIM_SPACE).sample(rng, nnz);
    indices.sort_unstable();
    // Use a small discrete value pool so values round-trip exactly (no FP equality risk).
    let values: Vec<f32> = (0..nnz)
        .map(|_| *[0.25_f32, 0.5, 0.75, 1.0].choose(rng).unwrap())
        .collect();
    canonical_sparse(&SparseVector { indices, values })
}

/// All currently-active named vectors populated — used by initial Upsert/Batch/Conditional.
fn random_named_vectors(rng: &mut impl Rng, active: &BTreeSet<VectorNameBuf>) -> NamedVectors {
    let mut map = NamedVectors::default();
    for name in active {
        map.insert(name.clone(), random_vector_for_name(rng, name));
    }
    map
}

/// A partial named-vector map (1-2 of the currently-active names) — used by UpdateVectors.
pub(super) fn random_partial_named_vectors(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
) -> NamedVectors {
    let names = random_vector_name_subset(rng, active);
    let mut map = NamedVectors::default();
    for name in names {
        let value = random_vector_for_name(rng, &name);
        map.insert(name, value);
    }
    map
}

/// Build a random vector matching the kind metadata associated with `name`.
fn random_vector_for_name(rng: &mut impl Rng, name: &str) -> VectorValue {
    let candidate = candidate_of(name);
    match candidate.kind {
        VectorKind::Dense(dim) => {
            VectorValue::Dense(random_dense_vec(rng, dim, candidate.datatype))
        }
        VectorKind::Sparse => VectorValue::Sparse(random_sparse_vector(rng)),
        VectorKind::MultiDense(dim) => {
            VectorValue::MultiDense(random_multi_dense(rng, dim, candidate.datatype))
        }
    }
}

/// 2-4 rows of `dim`-wide dense vectors. Row count varies per point — exercises the
/// engine's variable-length matrix storage.
fn random_multi_dense(rng: &mut impl Rng, dim: u64, datatype: Option<Datatype>) -> Vec<Vec<f32>> {
    let rows = rng.random_range(2..=4);
    (0..rows)
        .map(|_| random_dense_vec(rng, dim, datatype))
        .collect()
}

/// Random 1-2 distinct names from the active set (callers gate on `active` being non-empty).
pub(super) fn random_vector_name_subset(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
) -> Vec<VectorNameBuf> {
    let n = rng.random_range(1..=2).min(active.len());
    active.iter().cloned().sample(rng, n)
}

pub(super) fn random_vector_name(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
) -> VectorNameBuf {
    active.iter().choose(rng).cloned().expect("active is empty")
}

/// Build a search query appropriate for the chosen vector name.
pub(super) fn random_query_for_name(rng: &mut impl Rng, name: &str) -> VectorValue {
    random_vector_for_name(rng, name)
}

/// A single prefetch source for `QueryFusion`: a Nearest sub-query over a random active vector
/// name, with its own limit and optional `num` filter.
pub(super) fn random_prefetch(rng: &mut impl Rng, active: &BTreeSet<VectorNameBuf>) -> Prefetch {
    let vector_name = random_vector_name(rng, active);
    let query = random_query_for_name(rng, &vector_name);
    Prefetch {
        vector_name,
        query,
        limit: rng.random_range(1..=10),
        filter_num: rng.random_bool(0.5).then(|| random_num(rng)),
    }
}

pub(super) fn random_payload(rng: &mut impl Rng) -> Payload {
    let mut map = Map::new();
    map.insert("num".to_string(), Value::from(rng.random_range(0..100i64)));
    if rng.random_bool(0.5) {
        map.insert("tag".to_string(), Value::from(random_tag(rng)));
    }
    // Always-on fields — payload mutation ops (Set/Overwrite/Delete/Clear) can still drop them.
    map.insert("f".to_string(), Value::from(*FLOATS.choose(rng).unwrap()));
    map.insert("b".to_string(), Value::from(rng.random_bool(0.5)));
    map.insert(
        "d".to_string(),
        Value::from(*DATETIMES.choose(rng).unwrap()),
    );
    let (lat, lon) = random_geo_coords(rng);
    let mut geo = Map::new();
    geo.insert("lat".to_string(), Value::from(lat));
    geo.insert("lon".to_string(), Value::from(lon));
    map.insert("g".to_string(), Value::Object(geo));
    map.insert("t".to_string(), Value::from(*TEXTS.choose(rng).unwrap()));
    map.insert("url".to_string(), Value::from(random_url(rng)));
    Payload(map)
}

fn random_geo_coords(rng: &mut impl Rng) -> (f64, f64) {
    // 0.1-degree snapping: ≈11km separation between adjacent positions, well above any
    // Haversine FP imprecision a radius filter could land on.
    let lat = (rng.random_range(GEO_LAT_LO..GEO_LAT_HI) * 10.0).round() / 10.0;
    let lon = (rng.random_range(GEO_LON_LO..GEO_LON_HI) * 10.0).round() / 10.0;
    (lat, lon)
}

pub(super) fn random_point(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
    id_pool: u64,
) -> (PointIdType, NamedVectors, Payload) {
    (
        random_id(rng, id_pool),
        random_named_vectors(rng, active),
        random_payload(rng),
    )
}

pub(super) fn random_distinct_points(
    rng: &mut impl Rng,
    active: &BTreeSet<VectorNameBuf>,
    range: std::ops::RangeInclusive<usize>,
    id_pool: u64,
) -> Vec<(PointIdType, NamedVectors, Payload)> {
    let n = rng.random_range(range);
    let mut seen = AHashSet::new();
    let mut points = Vec::with_capacity(n);
    while points.len() < n {
        let (id, vecs, payload) = random_point(rng, active, id_pool);
        if seen.insert(id) {
            points.push((id, vecs, payload));
        }
    }
    points
}

pub(super) fn random_direction(rng: &mut impl Rng) -> Direction {
    if rng.random_bool(0.5) {
        Direction::Asc
    } else {
        Direction::Desc
    }
}

pub(super) fn random_recommend_strategy(rng: &mut impl Rng) -> RecommendStrategy {
    match rng.random_range(0..3) {
        0 => RecommendStrategy::AverageVector,
        1 => RecommendStrategy::BestScore,
        2 => RecommendStrategy::SumScores,
        _ => unreachable!(),
    }
}

pub(super) fn random_update_mode(rng: &mut impl Rng) -> UpdateMode {
    match rng.random_range(0..3) {
        0 => UpdateMode::Upsert,
        1 => UpdateMode::InsertOnly,
        2 => UpdateMode::UpdateOnly,
        _ => unreachable!(),
    }
}
