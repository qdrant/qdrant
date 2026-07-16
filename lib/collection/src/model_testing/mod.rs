mod apply;
mod fixture;
mod op;
mod trace;
mod verify;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use segment::types::{Distance, Payload, PointIdType, VectorNameBuf};
use sparse::common::sparse_vector::SparseVector;
use tokio::task::JoinHandle;

use crate::collection::Collection;
use crate::operations::snapshot_ops::SnapshotDescription;
use crate::operations::types::{CollectionResult, Datatype};
use crate::shards::shard::PeerId;

const PEER_ID: PeerId = 1;
const COLLECTION_NAME: &str = "test";

/// Static metadata for every vector name the test might ever activate. All names but "u"
/// start active in the fixture (`initially_active`); inactive ones are reachable through
/// `Op::CreateVectorName`.
pub(super) const ALL_CANDIDATES: &[VectorCandidate] = &[
    VectorCandidate {
        name: "a",
        kind: VectorKind::Dense(4),
        datatype: None,
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    VectorCandidate {
        name: "b",
        kind: VectorKind::Dense(6),
        datatype: None,
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // Explicit `Some(Float32)` rather than the unset default: exercises schema configs that
    // spell the default datatype out (config serialization + any `None` vs `Some` comparison
    // logic), a path the `datatype: None` candidates never hit. Storage-wise identical to them.
    VectorCandidate {
        name: "c",
        kind: VectorKind::Dense(5),
        datatype: Some(Datatype::Float32),
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // Dense vector configured with HNSW `inline_storage` (original + quantized vectors stored
    // inside the HNSW index file) backed by scalar quantization. From the model's perspective
    // this behaves like any other dense vector; only the on-disk index layout differs.
    VectorCandidate {
        name: "i",
        kind: VectorKind::Dense(4),
        datatype: None,
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::Scalar),
        initially_active: true,
    },
    VectorCandidate {
        name: "s",
        kind: VectorKind::Sparse,
        datatype: None,
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    VectorCandidate {
        name: "u",
        kind: VectorKind::Sparse,
        datatype: None,
        distance: Distance::Dot,
        quantization: None,
        initially_active: false,
    },
    VectorCandidate {
        name: "m",
        kind: VectorKind::MultiDense(4),
        datatype: None,
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // TurboQuant 4-bit compressed storage, the primary quantized datatype, applied to
    // appendable segments too. Lossy: the model records the dequantized read-back
    // (see `model_vector`) and compares with a few-ulp tolerance (see `dense_matches`).
    VectorCandidate {
        name: "q",
        kind: VectorKind::Dense(8),
        datatype: Some(Datatype::Turbo4),
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // Half-precision storage. Lossy but deterministic and idempotent: the model records
    // the f32 -> f16 -> f32 round-trip and compares exactly.
    VectorCandidate {
        name: "h",
        kind: VectorKind::Dense(6),
        datatype: Some(Datatype::Float16),
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // Unsigned-byte storage. The engine truncates each component with `x as u8`; the
    // generator draws components from `0.0..256.0` so values don't collapse to 0/1,
    // and the model records the truncated read-back and compares exactly.
    VectorCandidate {
        name: "y",
        kind: VectorKind::Dense(4),
        datatype: Some(Datatype::Uint8),
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // Multi-vector variants of the lossy-but-idempotent datatypes: each stored row goes
    // through the same per-component round-trip as its dense counterpart ("h" / "y").
    VectorCandidate {
        name: "w",
        kind: VectorKind::MultiDense(5),
        datatype: Some(Datatype::Float16),
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    VectorCandidate {
        name: "z",
        kind: VectorKind::MultiDense(3),
        datatype: Some(Datatype::Uint8),
        distance: Distance::Dot,
        quantization: None,
        initially_active: true,
    },
    // Cosine candidates. The engine normalizes Cosine vectors once, at first ingestion
    // (`Metric::preprocess`, dispatched per storage datatype); every later move
    // (optimizer rebuilds, copy-on-write point moves, `upsert_moved_point`) transfers the
    // stored form as raw bytes without re-preprocessing, so the model records
    // normalize-then-datatype-round-trip and compares exactly (Turbo4: usual tolerance).
    VectorCandidate {
        name: "e",
        kind: VectorKind::Dense(5),
        datatype: None,
        distance: Distance::Cosine,
        quantization: None,
        initially_active: true,
    },
    // Per-row normalization: each stored row of the matrix is preprocessed like a dense
    // Cosine vector.
    VectorCandidate {
        name: "n",
        kind: VectorKind::MultiDense(4),
        datatype: None,
        distance: Distance::Cosine,
        quantization: None,
        initially_active: true,
    },
    // Ordering coverage: normalization happens in f32 first, then the f16 storage
    // round-trip. The f16-rounded unit vector is stored as-is (raw-byte moves), so the
    // prediction stays exact even though its norm is no longer 1.0 within f16 precision.
    VectorCandidate {
        name: "x",
        kind: VectorKind::Dense(6),
        datatype: Some(Datatype::Float16),
        distance: Distance::Cosine,
        quantization: None,
        initially_active: true,
    },
    // TurboQuant's dedicated Cosine mode: the l2 length is not stored (forced to 1.0,
    // recovered via the centroid-norm recompute at dequantize), plus zero-vector guards.
    // Same padding-free dim as "q" so the CoW re-quantization fixed point holds.
    VectorCandidate {
        name: "o",
        kind: VectorKind::Dense(8),
        datatype: Some(Datatype::Turbo4),
        distance: Distance::Cosine,
        quantization: None,
        initially_active: true,
    },
    // Euclid / Manhattan candidates. Both metrics' ingestion preprocess is an identity
    // (like Dot), so the model's read-back prediction needs no distance-specific logic;
    // their coverage value is on the engine side: `Order::SmallBetter` comparator paths
    // in search/HNSW that Dot and Cosine never exercise.
    VectorCandidate {
        name: "j",
        kind: VectorKind::Dense(6),
        datatype: None,
        distance: Distance::Euclid,
        quantization: None,
        initially_active: true,
    },
    VectorCandidate {
        name: "k",
        kind: VectorKind::Dense(4),
        datatype: None,
        distance: Distance::Manhattan,
        quantization: None,
        initially_active: true,
    },
    // Search-side quantization coverage: one candidate per remaining `QuantizationConfig`
    // variant ("i" covers Scalar). Original vectors are always kept alongside the quantized
    // codes, so read-back predictions are unaffected; the lossy quantized scoring only feeds
    // the membership-only Search/Query/Recommend checks. Scalar/Product quantize at HNSW
    // build time; Binary/Turbo also quantize appendable segments (`appendable_quantization`
    // feature flag, on by default).
    VectorCandidate {
        name: "p",
        kind: VectorKind::Dense(8),
        datatype: None,
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::Product),
        initially_active: true,
    },
    // Dim 6 is deliberately not byte-aligned: the packed 1-bit codes have trailing padding.
    VectorCandidate {
        name: "v",
        kind: VectorKind::Dense(6),
        datatype: None,
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::Binary),
        initially_active: true,
    },
    // TurboQuant as search-side quantization over regular Float32 storage, distinct from
    // the `Datatype::Turbo4` candidates ("q"/"o") where TQ *is* the storage.
    VectorCandidate {
        name: "r",
        kind: VectorKind::Dense(8),
        datatype: None,
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::Turbo),
        initially_active: true,
    },
    // Quantization x datatype combos. Live appendable inserts quantize the pristine f32
    // input; optimizer rebuilds and CoW moves re-quantize from the lossy storage
    // read-back, so the codes for the same point differ across those paths. Predictions
    // still only depend on the datatype side (quantization never changes the stored
    // vectors).
    VectorCandidate {
        name: "l",
        kind: VectorKind::Dense(6),
        datatype: Some(Datatype::Float16),
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::Binary),
        initially_active: true,
    },
    // Turbo4 storage re-quantized with TurboQuant, one candidate per branch of
    // `should_keep_source_rotated` (segment's `quantized_vectors.rs`). Default bits: the
    // secondary TQ keeps the source codes in their rotated space and reuses the storage's
    // `Unpadded` rotation for queries. Same padding-free dim 8 as "q" so the CoW
    // re-quantization fixed point holds on the storage side.
    VectorCandidate {
        name: "d",
        kind: VectorKind::Dense(8),
        datatype: Some(Datatype::Turbo4),
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::Turbo),
        initially_active: true,
    },
    // `Bits1_5` requires a `Padded` rotation, so the same predicate takes the rotate-back
    // branch: source codes are dequantized to the original space and re-rotated.
    VectorCandidate {
        name: "g",
        kind: VectorKind::Dense(8),
        datatype: Some(Datatype::Turbo4),
        distance: Distance::Dot,
        quantization: Some(QuantizationKind::TurboBits1_5),
        initially_active: true,
    },
];

/// Reject kind/datatype/distance combinations the model cannot predict yet, at run startup
/// rather than as a false-divergence soak panic hours into a run:
/// - MultiDense + Turbo4: `model_vector` predicts Turbo4 via the per-vector
///   `turbo_storage_roundtrip`; the engine's multivector quantization path differs, so
///   there is no prediction for it. Float16/Uint8 multi-dense are fine: each row takes
///   the same per-component round-trip as the dense case.
/// - Sparse + any datatype: the fixture's sparse arm ignores the field entirely
///   (sparse datatype lives in the sparse index config, which is not modeled).
/// - Sparse + non-Dot distance: sparse schemas have no distance parameter (the fixture's
///   sparse arm ignores the field), so anything but `Dot` would silently not apply.
/// - Turbo4 + Euclid/Manhattan: TQ's L1/L2 modes store vector lengths differently from
///   Dot/Cosine (dedicated `l2_length` field / scaling-factor-as-length), and the
///   copy-on-write re-quantization fixed point that `dense_matches`' ulp tolerance
///   relies on has only been soak-validated for Dot and Cosine.
/// - Quantization on non-dense kinds: the fixture only wires `quantization` into its
///   dense arm, so it would be silently dropped.
/// - Quantization on inactive candidates: `Op::CreateVectorName`'s `DenseVectorConfig`
///   carries no quantization, so the name would activate without it and the intended
///   coverage would silently vanish.
fn assert_candidates_predictable() {
    for c in ALL_CANDIDATES {
        let turbo4 = matches!(c.datatype, Some(Datatype::Turbo4));
        let supported = match c.kind {
            VectorKind::Dense(_) => {
                !turbo4 || matches!(c.distance, Distance::Dot | Distance::Cosine)
            }
            VectorKind::MultiDense(_) => !turbo4,
            VectorKind::Sparse => c.datatype.is_none() && c.distance == Distance::Dot,
        };
        assert!(
            supported,
            "unsupported kind/datatype/distance combination for `{}` (see comment above)",
            c.name
        );
        if c.quantization.is_some() {
            assert!(
                matches!(c.kind, VectorKind::Dense(_)) && c.initially_active,
                "quantization is dense-only and requires initially_active for `{}` (see comment above)",
                c.name
            );
        }
    }
}

/// Dense vector name configured with HNSW `inline_storage` + scalar quantization in the fixture.
pub(super) const INLINE_STORAGE_VECTOR: &str = "i";

pub(super) struct VectorCandidate {
    pub(super) name: &'static str,
    pub(super) kind: VectorKind,
    /// Storage datatype override; `None` leaves the schema field unset so the engine's
    /// default (Float32) resolution applies. Supported for `VectorKind::Dense` (all
    /// datatypes) and `VectorKind::MultiDense` (all but Turbo4); enforced by the
    /// startup check (`assert_candidates_predictable`).
    pub(super) datatype: Option<Datatype>,
    /// Distance metric for dense / multi-dense storage. Cosine vectors are normalized by
    /// the engine at first ingestion; `model_vector` mirrors that (per-datatype dispatch:
    /// the byte metric's Cosine preprocess is an identity, so Uint8 is stored
    /// un-normalized). Sparse candidates must use `Dot` — sparse schemas carry no
    /// distance and score as dot product by construction — enforced by
    /// `assert_candidates_predictable`.
    pub(super) distance: Distance,
    /// Search-side quantization attached to the schema (`quantization_config` on the vector
    /// params). Originals are always kept, so read-back predictions are unaffected; only the
    /// membership-only nearest checks see the approximate quantized scoring. Dense-only, and
    /// only on `initially_active` candidates; enforced by `assert_candidates_predictable`.
    pub(super) quantization: Option<QuantizationKind>,
    /// Whether the name is present in the collection schema at fixture time. Inactive
    /// names are only reachable through `Op::CreateVectorName`, which is FORCE_OFF by
    /// default, so a candidate gets default-soak coverage only when this is true.
    pub(super) initially_active: bool,
}

/// Which `QuantizationConfig` variant the fixture attaches to a dense candidate; the
/// concrete config (all engine-default fields) is materialized in
/// `fixture::quantization_config`.
#[derive(Copy, Clone, Debug)]
pub(super) enum QuantizationKind {
    /// Int8 scalar quantization.
    Scalar,
    /// Product quantization, x4 compression (1-dim chunks, 256 centroids each).
    Product,
    /// Binary quantization, default one-bit encoding.
    Binary,
    /// TurboQuant quantization of the search index (as opposed to `Datatype::Turbo4`,
    /// which makes TQ the storage itself).
    Turbo,
    /// TurboQuant with `Bits1_5` encoding, which requires a `Padded` rotation: over a
    /// Turbo4 storage source this forces the rotate-back branch of
    /// `should_keep_source_rotated`, where default-bits `Turbo` takes the keep-rotated one.
    TurboBits1_5,
}

#[derive(Copy, Clone, Debug)]
pub(super) enum VectorKind {
    Dense(u64),
    Sparse,
    /// ColBERT-style multi-vector: each point stores a matrix of `dim`-wide rows. Scoring
    /// uses MaxSim across query rows × stored rows.
    MultiDense(u64),
}

pub(super) fn candidate_of(name: &str) -> &'static VectorCandidate {
    ALL_CANDIDATES
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("unknown vector name: {name}"))
}

/// Every point id the workload can draw, precomputed at startup. A bounded, stable pool is
/// what gives the workload its reuse semantics (upserts overwrite live points, deletes and
/// retrieves hit them); fresh random UUIDs drawn per op would never repeat and lose all of
/// that.
struct IdSpace {
    ids: Vec<PointIdType>,
}

impl IdSpace {
    /// `uuid_fraction` of the `id_pool` slots (rounded) become UUID ids, the rest stay
    /// numeric (`NumId(0..n)`). The UUIDs are well-formed v4, built from seeded-rng bytes
    /// (`Builder::from_random_bytes`), so the same seed regenerates the identical pool.
    /// With `0.0` no rng draws are consumed and the pool is exactly `NumId(0..id_pool)`,
    /// reproducing the numeric-only op stream of builds without UUID support.
    fn new(rng: &mut impl Rng, id_pool: u64, uuid_fraction: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&uuid_fraction),
            "uuid_fraction must be in [0.0, 1.0], got {uuid_fraction}"
        );
        let uuid_count = (id_pool as f64 * uuid_fraction).round() as u64;
        let mut ids: Vec<PointIdType> = (0..id_pool - uuid_count).map(PointIdType::NumId).collect();
        for _ in 0..uuid_count {
            let uuid = uuid::Builder::from_random_bytes(rng.random()).into_uuid();
            ids.push(PointIdType::Uuid(uuid));
        }
        IdSpace { ids }
    }

    /// One uniform draw from the pool. Consumes a single `0..len` range draw, exactly like
    /// the `NumId(random_range(0..id_pool))` it replaced, so an all-numeric pool is
    /// rng-stream-identical to the pre-`IdSpace` workload.
    fn random_id(&self, rng: &mut impl Rng) -> PointIdType {
        let n = rng.random_range(0..self.ids.len() as u64);
        self.ids[n as usize]
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}

// BTreeMap for deterministic iteration order — several op generators sample from `model.keys()`
// with a seeded RNG, and reservoir sampling order is iteration-order-dependent. AHashMap's
// `RandomState` seeds with per-process entropy, which breaks workload determinism even for a
// fixed `--seed`. BTreeMap's natural ordering by `PointIdType` is stable across runs.
type Model = BTreeMap<PointIdType, ModelEntry>;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ModelEntry {
    pub(super) vectors: BTreeMap<VectorNameBuf, VectorValue>,
    pub(super) payload: Payload,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum VectorValue {
    Dense(Vec<f32>),
    /// Stored canonicalized: sorted by index, zero-valued entries dropped. The engine returns
    /// sparse vectors in this form, so storing them canonically makes round-trip equality work.
    Sparse(SparseVector),
    /// ColBERT-style multi-vector: a matrix of rows, each of the configured dim.
    MultiDense(Vec<Vec<f32>>),
}

/// Soak entrypoint — drives randomized operations against a fresh collection, continuously
/// verifying it against an in-memory model. Reproducible for a given `seed`.
///
/// The run stops at whichever bound applies: by default after `op_num` ops, or — when
/// `duration` is `Some` — after that much wall-clock time elapses (`op_num` is then ignored
/// as the stop condition). Either way an early Ctrl-C still ends it cleanly.
///
/// Storage is rooted at `storage_path`. Any pre-existing `collection/` and `snapshots/`
/// subdirectories are wiped at the start of each run; copy them out beforehand if you
/// need to preserve a previous run for post-mortem inspection.
///
/// `shutdown` lets the caller request an early stop (e.g. on Ctrl-C). The op loop
/// checks it before each iteration and exits cleanly when set; post-run verification
/// + summary + reload still run with whatever ops were applied.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    seed: u64,
    op_num: usize,
    shard_count: u32,
    id_pool: u64,
    uuid_id_fraction: f64,
    storage_path: &Path,
    disable_optimizer: bool,
    max_segment_size_kb: usize,
    indexing_threshold_kb: usize,
    flush_interval_sec: u64,
    restart_probability: f64,
    swarm_interval: usize,
    on_disk: bool,
    pre_restart_check: bool,
    enable_force_off: bool,
    disable_snapshots: bool,
    duration: Option<Duration>,
    shutdown: Arc<AtomicBool>,
) {
    assert_candidates_predictable();

    let (collection_dir, snapshots_dir, collection) = fixture::fixture(
        shard_count,
        storage_path,
        disable_optimizer,
        max_segment_size_kb,
        indexing_threshold_kb,
        flush_interval_sec,
        on_disk,
    )
    .await;
    // `Arc` so a background `CreateSnapshot` task can hold the collection alive while the main loop
    // keeps writing to it. Reassigned (restart / final reload) by replacing the `Arc`.
    let mut collection = Arc::new(collection);

    // Trace lives at the storage-path root (next to `collection/` and `snapshots/`) so the
    // fixture's per-run wipe of those subdirs leaves it alone; `File::create` truncates so each
    // run starts fresh. Schema is documented in `trace.rs`.
    let trace_path = storage_path.join("trace.log");
    let mut trace = trace::Trace::create(&trace_path)
        .unwrap_or_else(|e| panic!("failed to create trace at {}: {e}", trace_path.display()));
    trace.header(
        seed,
        op_num,
        shard_count,
        id_pool,
        uuid_id_fraction,
        disable_optimizer,
        max_segment_size_kb,
        indexing_threshold_kb,
        flush_interval_sec,
        restart_probability,
        swarm_interval,
        enable_force_off,
        disable_snapshots,
        duration,
    );

    let mut model: Model = Model::new();
    let mut active_names: BTreeSet<VectorNameBuf> = ALL_CANDIDATES
        .iter()
        .filter(|c| c.initially_active)
        .map(|c| c.name.to_string())
        .collect();
    // In-flight background snapshot task, if any (at most one at a time). A `CreateSnapshot` op
    // spawns it; the loop reaps it on completion each iteration and drains it before any
    // close+reopen. `None` when no snapshot is running. See [`drain_snapshot`].
    let mut pending_snapshot: Option<JoinHandle<CollectionResult<SnapshotDescription>>> = None;
    let rng = &mut SmallRng::seed_from_u64(seed);
    // Drawn from the seeded rng BEFORE the first swarm config so the pool is fixed for the
    // whole run. A nonzero `uuid_id_fraction` consumes rng draws here and thus shifts the
    // op stream vs. other fraction values for the same seed; `0.0` consumes none.
    let id_space = IdSpace::new(rng, id_pool, uuid_id_fraction);

    // Swarm-testing config (Groce et al., ISSTA 2012): disable a random subset of ops. Recomputed
    // every `swarm_interval` ops so one long run becomes a sequence of swarm sub-tests (broader
    // feature-interaction coverage than a single fixed config). Drawn from the seeded rng so it's
    // reproducible; each redraw is logged with its op tick so a failure is attributable to the
    // config that was live. (This consumes rng draws, so the op stream differs from a non-swarm
    // build for the same seed.)
    let mut swarm = op::Swarm::random(rng, enable_force_off, disable_snapshots);
    let initial_enabled = swarm.enabled_ops();
    trace.swarm(0, &initial_enabled);
    println!("model_testing: op:0 swarm -> {initial_enabled:?}");

    // Two stop modes. By default the run is bounded by `op_num` and the bar fills toward that
    // op count. With `--duration` the op count is unbounded, so the bar instead fills toward the
    // wall-clock deadline (length = total seconds, position = elapsed seconds): `{pos}/{len}` and
    // `eta` read in seconds and the bar still visibly advances to completion.
    let loop_start = Instant::now();
    let deadline = duration.map(|d| loop_start + d);
    let bar = match duration {
        // `max(1)` avoids a zero-length bar (which indicatif renders as already-complete) for a
        // sub-second `--duration`.
        Some(d) => {
            let b = ProgressBar::new(d.as_secs().max(1));
            b.set_style(
                ProgressStyle::default_bar()
                    .template("{msg:24} [{elapsed_precise}] {bar:40} {pos}s/{len}s (eta:{eta})")
                    .expect("Failed to create progress style"),
            );
            b
        }
        None => {
            let b = ProgressBar::new(op_num as u64);
            b.set_style(
                ProgressStyle::default_bar()
                    // Single line with a fixed-width bar: a multi-line (`{msg}\n…`) template plus a
                    // full-width `{wide_bar}` can't be repainted in place — the bar line hits the
                    // terminal-width auto-wrap boundary and occupies one more physical row than
                    // indicatif clears, so stale `{msg}` lines stick and smear together. A
                    // fixed-width `{bar}` never reaches that boundary; `{msg:24}` is width-padded so
                    // a changing op kind (longest is "OverwritePayloadByFilter", 24 chars) doesn't
                    // shift the bar left/right each frame.
                    .template(
                        "{msg:24} [{elapsed_precise}] {bar:40} {pos}/{len} ({per_sec}, eta:{eta})",
                    )
                    .expect("Failed to create progress style"),
            );
            b
        }
    };

    let mut applied = 0usize;
    let mut i = 0usize;
    // `total_optimized_points` lives on the local shard and resets to 0 on every
    // close+reopen. Track whether we ever saw optimization during this run so a
    // restart on the last iteration doesn't send us back to `wait_for_optimizer`
    // with a zero counter even though the optimizer already ran earlier.
    let mut optimizer_ran_during_run = false;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            log::info!("shutdown received at op:{i}, exiting loop");
            break;
        }
        // Stop condition: deadline reached in duration mode, op count reached otherwise.
        match deadline {
            Some(dl) => {
                if Instant::now() >= dl {
                    log::info!("duration elapsed at op:{i}, exiting loop");
                    break;
                }
            }
            None => {
                if i >= op_num {
                    break;
                }
            }
        }
        // Recompute the swarm config at each interval boundary (op 0 was drawn before the loop).
        if i > 0 && i.is_multiple_of(swarm_interval) {
            let prev = swarm.enabled_ops();
            swarm = op::Swarm::random(rng, enable_force_off, disable_snapshots);
            let next = swarm.enabled_ops();
            // Trace keeps the full enabled set (for reproducibility); the console shows only the
            // delta vs. the previous config — that's what changed.
            trace.swarm(i, &next);
            let enabled: Vec<&str> = next.iter().copied().filter(|n| !prev.contains(n)).collect();
            let disabled: Vec<&str> = prev.iter().copied().filter(|n| !next.contains(n)).collect();
            let kept: Vec<&str> = next.iter().copied().filter(|n| prev.contains(n)).collect();
            // `suspend` clears the bar, runs the closure, then redraws — without it the bar's next
            // repaint overwrites the last printed line (the `=kept` one). Plain `println!`
            // (stdout) inside keeps it visible when piped, unlike `bar.println` which routes
            // through the bar's stderr draw target and is swallowed on a non-TTY.
            bar.suspend(|| {
                println!("model_testing: op:{i} regen swarm Δ\n  +{enabled:?}\n  -{disabled:?}\n  ={kept:?}");
            });
        }
        // Roll for a mid-run restart BEFORE generating an op so an iteration is exactly one of
        // restart / apply (never both). The `> 0.0` short-circuits the rng draw at the default
        // value, so workload determinism is preserved for `--restart-probability 0` (which matches
        // the previous no-restart default exactly). (Snapshots are an ordinary `CreateSnapshot`
        // swarm op dispatched below — not a separate roll.)
        let do_restart = restart_probability > 0.0 && rng.random::<f64>() < restart_probability;
        if do_restart {
            log::debug!("op:{i} Restart");
            bar.set_message("Restart");
            let (pre_segments, pre_optimized) = verify::run_summary(&collection).await;
            if pre_optimized > 0 {
                optimizer_ran_during_run = true;
            }
            trace.restart(i, model.len(), pre_segments);
            // Verify the LIVE collection against the model *before* closing. This splits a
            // restart failure into its two possible causes: if this assert fires, the engine
            // diverged from the model while serving (an apply-path bug); if it passes but the
            // post-reload assert below fires, the divergence was introduced by close+reopen (a
            // durability / WAL-replay bug). Without this split, a "restart at op:N" panic can't
            // distinguish the two.
            //
            // Opt-in (`--pre-restart-check`, off by default): the full pre-close scroll is slow,
            // and reading the whole collection warms caches / forces lazy loads that can mask a
            // reload divergence — so the default cold close+reopen surfaces more bugs. Enable the
            // check when you need to attribute a failure to the apply path vs. the reload path.
            if pre_restart_check {
                let live_pre = verify::collect_model_from_collection(&collection).await;
                verify::assert_matches_model(
                    &live_pre,
                    &model,
                    &format!("pre-restart (live) at op:{i}"),
                );
            }
            // Close + reopen + full model verification. Does NOT force a flush — mirrors
            // what a real user gets from `stop_gracefully`, which is the path that exposes
            // the WAL-replay bug class.
            //
            // `Collection::load` (inside `reopen_collection`) draws its own
            // "Recovering collection" progress bar to stderr. Two independent indicatif bars on
            // the same stream fight over the cursor — our op bar keeps overwriting the recovery
            // bar, so it never visibly advances. Hide our bar for the duration of the reopen so
            // the recovery bar owns the terminal, then restore it. The leading newline drops the
            // recovery bar onto a fresh line (a fresh indicatif bar's first draw lands at the
            // cursor without clearing, so it would otherwise share our bar's last frame's line);
            // the trailing newline preserves the recovery bar's final frame in scrollback instead
            // of letting the op loop's next tick `\r`-overwrite it.
            bar.set_draw_target(ProgressDrawTarget::hidden());
            eprintln!();
            // Finish any background snapshot first: it holds an `Arc` clone of this collection, so
            // the `drop` below wouldn't actually close the collection (releasing its files) until
            // the task ends — and reopening the same dir with the old collection still open is
            // unsafe.
            drain_snapshot(&collection, &snapshots_dir, &mut pending_snapshot, i).await;
            // Newest-clocks recovery point must survive the close+reopen exactly; captured here
            // (snapshot drained, op loop idle) and compared after the reload below. See
            // [`verify::assert_clocks_match`] for why both mismatch directions are bugs.
            let pre_clocks = verify::collect_clock_ticks(&collection).await;
            collection.stop_gracefully().await;
            // `into_inner` makes the invariant checked, not assumed: if any background task still
            // holds an `Arc` clone here, panic loudly instead of reopening the same dir while the
            // old collection is silently kept alive.
            drop(
                Arc::into_inner(collection)
                    .expect("collection still referenced at restart (undrained background task?)"),
            );
            collection =
                Arc::new(fixture::reopen_collection(&collection_dir, &snapshots_dir).await);
            // `Collection::load` returns before tail-of-WAL ops queued to the
            // update worker have been applied — that's an intentional fast-start
            // feature. Wait for the queue to drain so the scroll below observes
            // all WAL-replayed state.
            verify::wait_for_pending_updates(&collection).await;
            let live = verify::collect_model_from_collection(&collection).await;
            verify::assert_matches_model(&live, &model, &format!("restart at op:{i}"));
            // Clock check AFTER the model check: a lost WAL tail trips both, and the model diff
            // (extra/missing ids) is the established postmortem signature for that class, so it
            // stays first and known failures keep their signature. A clocks-only mismatch (data
            // intact, clock state wrong) then surfaces distinctly here.
            let post_clocks = verify::collect_clock_ticks(&collection).await;
            verify::assert_clocks_match(&pre_clocks, &post_clocks, &format!("restart at op:{i}"));
            eprintln!();
            bar.set_draw_target(ProgressDrawTarget::stderr());
        } else {
            let op = op::Op::random(rng, &model, &active_names, &id_space, &swarm);
            log::debug!("op:{i} {op:?}");
            bar.set_message(op.kind());
            // Log BEFORE apply so a panic preserves the offending op in the trace.
            trace.op(i, &op);
            // CreateSnapshot needs run-loop context (it spawns a background task against shared
            // collection state), so it's dispatched here; everything else goes through `apply`.
            if matches!(op, op::Op::CreateSnapshot) {
                // Spawn a snapshot that runs *concurrently with the ongoing workload* (the loop
                // keeps applying ops below while it runs on a `spawn_blocking` thread). The archive
                // is discarded — we only assert the create succeeds and doesn't corrupt the live
                // collection (the verification ops + final reload catch that). At most one in
                // flight; a CreateSnapshot drawn while one is running is a no-op. The op draws no
                // rng, so the op stream stays reproducible even with snapshots enabled.
                if pending_snapshot.is_none() {
                    let temp_dir = tempfile::tempdir().expect("failed to create snapshot temp dir");
                    let snapshot_collection = Arc::clone(&collection);
                    pending_snapshot = Some(tokio::spawn(async move {
                        let res = snapshot_collection
                            .create_snapshot(temp_dir.path(), PEER_ID)
                            .await;
                        drop(temp_dir); // keep the staging dir alive until the archive is written
                        res
                    }));
                    log::debug!("op:{i} CreateSnapshot spawned");
                } else {
                    log::debug!("op:{i} CreateSnapshot skipped (one already in flight)");
                }
            } else {
                apply::apply(&collection, &mut model, &mut active_names, &op).await;
            }
        }
        // Reap a finished background snapshot (non-blocking check); panics if it errored.
        if pending_snapshot
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            drain_snapshot(&collection, &snapshots_dir, &mut pending_snapshot, i).await;
        }
        i += 1;
        applied += 1;
        // In duration mode the bar tracks elapsed wall-clock seconds (capped at the total so a
        // final op finishing just past the deadline doesn't overshoot the bar); otherwise it
        // tracks the op count.
        match deadline {
            Some(_) => {
                let total = duration.map_or(0, |d| d.as_secs().max(1));
                bar.set_position(loop_start.elapsed().as_secs().min(total));
            }
            None => bar.inc(1),
        }
    }
    bar.finish();

    let interrupted = shutdown.load(Ordering::Relaxed);
    if interrupted {
        eprintln!("model_testing: verifying live state...");
    }
    log::debug!("all ops applied, verifying live collection against model");
    let live = verify::collect_model_from_collection(&collection).await;
    let (live_extra, live_missing) = verify::id_diff(&live, &model);
    let (mut segments, mut optimized) = verify::run_summary(&collection).await;
    if optimized > 0 {
        optimizer_ran_during_run = true;
    }
    trace.live_verify(
        applied,
        model.len(),
        live.len(),
        segments,
        optimized,
        &live_extra,
        &live_missing,
    );
    verify::assert_matches_model(&live, &model, "live");

    // Confirm the segment optimizer actually fired — but skip on shutdown (short run may
    // not have triggered), when the optimizer is intentionally disabled, or when we
    // already observed optimization earlier in the run (counter resets on restart).
    if !interrupted && !disable_optimizer && !optimizer_ran_during_run {
        verify::wait_for_optimizer(&collection).await;
        (segments, optimized) = verify::run_summary(&collection).await;
    }

    println!(
        "model_testing: {applied} ops applied, {} live points, {segments} segments, {optimized} \
         optimized points",
        model.len(),
    );

    if interrupted {
        eprintln!("model_testing: reopening to verify reload...");
    }
    // Finish any background snapshot before closing (it holds an `Arc` clone — see the restart
    // path), then close and reopen and re-verify — mirrors gridstore tests.rs:488-516.
    drain_snapshot(&collection, &snapshots_dir, &mut pending_snapshot, applied).await;
    // Same clock-durability capture as the mid-run restart path, for the final reload.
    let pre_clocks = verify::collect_clock_ticks(&collection).await;
    collection.stop_gracefully().await;
    // Checked close-before-reopen, same as the mid-run restart path.
    drop(
        Arc::into_inner(collection)
            .expect("collection still referenced at final reload (undrained background task?)"),
    );
    let collection = fixture::reopen_collection(&collection_dir, &snapshots_dir).await;
    // Same reason as the mid-run restart above — drain deferred WAL ops before scrolling.
    verify::wait_for_pending_updates(&collection).await;

    let reloaded = verify::collect_model_from_collection(&collection).await;
    let (reload_extra, reload_missing) = verify::id_diff(&reloaded, &model);
    trace.reload_verify(
        applied,
        model.len(),
        reloaded.len(),
        &reload_extra,
        &reload_missing,
    );
    verify::assert_matches_model(&reloaded, &model, "reloaded");
    // After the model check, same as the restart path (see the comment there for ordering).
    let post_clocks = verify::collect_clock_ticks(&collection).await;
    verify::assert_clocks_match(&pre_clocks, &post_clocks, "reloaded");
}

/// Upper bound on how long we wait for a background snapshot to finish when draining it, so a hung
/// `create_snapshot` fails loudly instead of stalling the run forever.
const SNAPSHOT_HANG_TIMEOUT: Duration = Duration::from_secs(120);

/// Finish the in-flight background snapshot (if any): await the task, panic if `create_snapshot`
/// errored or the task panicked, then delete the archive (we don't recover it). Called per-iteration
/// once the task reports finished, and unconditionally before any close+reopen — the task holds an
/// `Arc` clone of the collection, so it must end before the collection is dropped and reopened.
async fn drain_snapshot(
    collection: &Collection,
    snapshots_dir: &Path,
    pending: &mut Option<JoinHandle<CollectionResult<SnapshotDescription>>>,
    tick: usize,
) {
    let Some(handle) = pending.take() else {
        return;
    };
    let description = tokio::time::timeout(SNAPSHOT_HANG_TIMEOUT, handle)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "background snapshot did not finish within {SNAPSHOT_HANG_TIMEOUT:?} (op:{tick})"
            )
        })
        // Re-raise the task's own panic payload so the original assertion message (not an opaque
        // `JoinError`) reaches the log; `run` never cancels the task, so a join error is a panic.
        .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
        .unwrap_or_else(|e| panic!("concurrent create_snapshot (op:{tick}) failed: {e:?}"));

    // The archive isn't recovered, so drop it to keep a long soak from accumulating full-collection
    // copies on disk.
    let path = snapshots_dir.join(&description.name);
    if let Ok(manager) = collection.get_snapshots_storage_manager()
        && let Err(e) = manager.delete_snapshot(&path).await
    {
        log::warn!("failed to delete snapshot {path:?} (op:{tick}): {e}");
    }
}

/// Skipped on Windows because it is too slow
#[cfg(not(target_os = "windows"))]
#[cfg(test)]
mod tests {
    use super::*;

    const OP_NUM: usize = 8_000;
    const ID_POOL: u64 = 500;
    // The non-snapshot gates run with snapshots off so they stay fast, deterministic baselines for
    // their own concern. `CreateSnapshot` does background snapshot IO that only adds wall-clock time
    // without testing what those gates target; the snapshot path has its own test
    // ([`harness_no_optimizer_snapshots`]). Passing `true` here also exercises the
    // `--disable-snapshots` masking path.
    const SNAPSHOTS_OFF: bool = true;
    const SNAPSHOTS_ON: bool = false;

    /// Owns a run's storage dir. On a clean drop the dir is deleted (normal `TempDir`
    /// behaviour); if the test is unwinding from a panic the dir is leaked instead and its
    /// path printed, so the trace + data survive for postmortem reproduction with the seed.
    struct StorageGuard {
        dir: Option<tempfile::TempDir>,
        name: &'static str,
        seed: u64,
    }

    impl StorageGuard {
        fn path(&self) -> &Path {
            self.dir
                .as_ref()
                .expect("storage dir present during run")
                .path()
        }
    }

    impl Drop for StorageGuard {
        fn drop(&mut self) {
            if std::thread::panicking()
                && let Some(dir) = self.dir.take()
            {
                let path = dir.keep(); // leak: do not delete on failure
                eprintln!(
                    "model_testing: {} FAILED (seed = {}), storage retained at {}",
                    self.name,
                    self.seed,
                    path.display(),
                );
            }
        }
    }

    /// Drives one seeded smoke run end to end with the boilerplate shared by every harness
    /// test: single shard, fixed segment/indexing/flush knobs, no swarm force-off, op-bounded.
    /// Only `disable_optimizer`, `restart_probability`, `disable_snapshots`, and `op_num` vary
    /// between tests.
    ///
    /// `name` is the calling test's name, echoed in both the seed line and the failure message
    /// so a failure in this shared helper is attributable to the right test.
    ///
    /// The seed is drawn fresh each run and printed up front so a CI failure is reproducible
    /// (nextest captures stdout and shows it on failure); on panic [`StorageGuard`] retains the
    /// storage dir.
    async fn smoke(
        name: &'static str,
        disable_optimizer: bool,
        restart_probability: f64,
        disable_snapshots: bool,
        op_num: usize,
    ) {
        let storage_dir = tempfile::tempdir().expect("failed to create temp dir");
        let seed = rand::rng().random();
        println!("model_testing: {name} seed = {seed}");
        let guard = StorageGuard {
            dir: Some(storage_dir),
            name,
            seed,
        };
        run(
            seed,
            op_num,
            1, // shard_count
            ID_POOL,
            0.5, // uuid_id_fraction: mixed numeric/UUID ids
            guard.path(),
            disable_optimizer,
            10, // max_segment_size_kb
            5,  // indexing_threshold_kb
            5,  // flush_interval_sec
            restart_probability,
            2500,  // swarm_interval: a few redraws within the run
            false, // on_disk
            false, // pre_restart_check
            false, // enable_force_off
            disable_snapshots,
            None, // duration: bounded by op_num
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    }

    /// End-to-end smoke run: [`OP_NUM`] seeded ops against a single-shard collection, including
    /// the per-op verification ops, the end-of-run live check, and the final close+reopen
    /// reload check. Catches harness rot (op/model drift, fixture breakage) on every
    /// `cargo test` run.
    ///
    /// Single shard, optimizer disabled, no mid-run restarts: keeps the run fast and
    /// steers clear of the known-unfixed multi-shard reload bug class, so this stays green as a
    /// regression gate for the harness itself rather than a bug-finder (that's the binary's job).
    /// Snapshot ops are disabled here (`SNAPSHOTS_OFF`); the snapshot path has its own test
    /// ([`harness_no_optimizer_snapshots`]).
    #[tokio::test(flavor = "multi_thread")]
    async fn harness_no_optimizer_no_restarts() {
        smoke(
            "harness_no_optimizer_no_restarts",
            true,
            0.0,
            SNAPSHOTS_OFF,
            OP_NUM,
        )
        .await;
    }

    /// Same configuration as [`harness_no_optimizer_no_restarts`], but with the optimizer
    /// enabled: background segment churn (merges, indexing) runs concurrently with the op
    /// stream and feeds into the final close+reopen reload check, with no mid-run restarts.
    ///
    /// This is the fourth cell of the {optimizer on/off} × {restarts on/off} matrix. Holding
    /// restarts off isolates the optimizer/reload interaction from the WAL-replay-during-restart
    /// path that [`harness`] also exercises: a failure here points at the optimizer or the final
    /// reload, not at mid-run restart recovery.
    ///
    /// Runs half the op count of the optimizer-off variants: optimizer churn without restarts
    /// makes this the slowest no-restart cell, so it's trimmed to stay under the CI slow-test
    /// threshold. The fresh seed each run plus many CI runs recover the per-run coverage lost
    /// to the shorter run.
    #[tokio::test(flavor = "multi_thread")]
    async fn harness_no_restarts() {
        // op_num halved: optimizer churn makes this the slowest no-restart variant; trim it so
        // it isn't marked SLOW by nextest (coverage is recovered across many seeded CI runs).
        smoke("harness_no_restarts", false, 0.0, SNAPSHOTS_OFF, OP_NUM / 2).await;
    }

    /// Same configuration as [`harness_no_optimizer_no_restarts`], plus seeded mid-run
    /// restarts (~20 expected at p=0.002 over 10k ops): each one is a cold close+reopen+full
    /// model verification, so the WAL-replay path is exercised throughout the run
    /// instead of only at the final reload check.
    ///
    /// Note: the restart roll consumes one rng draw per iteration, so even for the same
    /// seed the op stream differs from a no-restart run.
    #[tokio::test(flavor = "multi_thread")]
    async fn harness_no_optimizer() {
        smoke("harness_no_optimizer", true, 0.002, SNAPSHOTS_OFF, OP_NUM).await;
    }

    /// Same configuration as [`harness_no_optimizer`], but with the optimizer enabled:
    /// background segment churn (merges, indexing) runs concurrently with the op stream
    /// and the seeded mid-run restarts, so the WAL-replay path is exercised against an
    /// optimized on-disk layout rather than a static one.
    ///
    /// Single shard keeps this clear of the known-unfixed multi-shard reload bug class, so
    /// it stays green as a regression gate for the optimizer-on restart path.
    ///
    /// Runs a quarter of the op count of the other variants: optimizer churn plus mid-run
    /// restarts make this the slowest combination, so it's trimmed to keep its wall-clock under
    /// the others rather than dominate the suite. The fresh seed each run plus many CI runs
    /// recover the per-run coverage lost to the shorter run.
    #[tokio::test(flavor = "multi_thread")]
    async fn harness() {
        // op_num quartered: optimizer churn + restarts make this the slowest variant; trim it so
        // it isn't the suite's long pole (coverage is recovered across many seeded CI runs).
        smoke("harness", false, 0.002, SNAPSHOTS_OFF, OP_NUM / 4).await;
    }

    /// Snapshot-focused gate: snapshots enabled (`SNAPSHOTS_ON`) plus seeded restarts. Each
    /// `CreateSnapshot` op spawns a `create_snapshot` that runs in the background, concurrently with
    /// the ongoing workload; the loop reaps it on completion (panicking if it errored). The restarts
    /// exercise the drain-before-close path (a restart must finish any in-flight snapshot before it
    /// can close+reopen), and the end-of-run reload exercises drain-at-end. The implicit assertion
    /// is that concurrent snapshotting neither errors nor corrupts the live collection — caught by
    /// the ongoing verification ops, the per-restart checks, and the final reload.
    ///
    /// `CreateSnapshot` draws no rng, so (unlike `--restart`) snapshots don't perturb the op stream;
    /// a fixed seed is fully reproducible.
    #[tokio::test(flavor = "multi_thread")]
    async fn harness_no_optimizer_snapshots() {
        // restart_probability 0.001: exercise drain-before-restart
        smoke(
            "harness_no_optimizer_snapshots",
            true,
            0.001,
            SNAPSHOTS_ON,
            OP_NUM,
        )
        .await;
    }
}
