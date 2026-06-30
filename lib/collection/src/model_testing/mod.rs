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
use rand::{RngExt, SeedableRng};
use segment::types::{Payload, PointIdType, VectorNameBuf};
use sparse::common::sparse_vector::SparseVector;

use crate::shards::shard::PeerId;

const PEER_ID: PeerId = 1;
const COLLECTION_NAME: &str = "test";

/// Static metadata for every vector name the test might ever activate. Four names start
/// active in the fixture; the remaining two are exclusively reachable through
/// `Op::CreateVectorName`.
pub(super) const ALL_CANDIDATES: &[VectorCandidate] = &[
    VectorCandidate {
        name: "a",
        kind: VectorKind::Dense(4),
    },
    VectorCandidate {
        name: "b",
        kind: VectorKind::Dense(6),
    },
    VectorCandidate {
        name: "c",
        kind: VectorKind::Dense(5),
    },
    // Dense vector configured with HNSW `inline_storage` (original + quantized vectors stored
    // inside the HNSW index file) backed by scalar quantization. From the model's perspective
    // this behaves like any other dense vector; only the on-disk index layout differs.
    VectorCandidate {
        name: "i",
        kind: VectorKind::Dense(4),
    },
    VectorCandidate {
        name: "s",
        kind: VectorKind::Sparse,
    },
    VectorCandidate {
        name: "u",
        kind: VectorKind::Sparse,
    },
    VectorCandidate {
        name: "m",
        kind: VectorKind::MultiDense(4),
    },
];

/// Names present in the collection schema at fixture time.
pub(super) const INITIAL_ACTIVE: &[&str] = &["a", "b", "i", "s", "m"];

/// Dense vector name configured with HNSW `inline_storage` + scalar quantization in the fixture.
pub(super) const INLINE_STORAGE_VECTOR: &str = "i";

pub(super) struct VectorCandidate {
    pub(super) name: &'static str,
    pub(super) kind: VectorKind,
}

#[derive(Copy, Clone, Debug)]
pub(super) enum VectorKind {
    Dense(u64),
    Sparse,
    /// ColBERT-style multi-vector: each point stores a matrix of `dim`-wide rows. Scoring
    /// uses MaxSim across query rows × stored rows.
    MultiDense(u64),
}

pub(super) fn kind_of(name: &str) -> VectorKind {
    ALL_CANDIDATES
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.kind)
        .unwrap_or_else(|| panic!("unknown vector name: {name}"))
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
    duration: Option<Duration>,
    shutdown: Arc<AtomicBool>,
) {
    let (collection_dir, snapshots_dir, mut collection) = fixture::fixture(
        shard_count,
        storage_path,
        disable_optimizer,
        max_segment_size_kb,
        indexing_threshold_kb,
        flush_interval_sec,
        on_disk,
    )
    .await;

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
        disable_optimizer,
        max_segment_size_kb,
        indexing_threshold_kb,
        flush_interval_sec,
        restart_probability,
        swarm_interval,
        enable_force_off,
        duration,
    );

    let mut model: Model = Model::new();
    let mut active_names: BTreeSet<VectorNameBuf> =
        INITIAL_ACTIVE.iter().map(|s| s.to_string()).collect();
    let rng = &mut SmallRng::seed_from_u64(seed);

    // Swarm-testing config (Groce et al., ISSTA 2012): disable a random subset of ops. Recomputed
    // every `swarm_interval` ops so one long run becomes a sequence of swarm sub-tests (broader
    // feature-interaction coverage than a single fixed config). Drawn from the seeded rng so it's
    // reproducible; each redraw is logged with its op tick so a failure is attributable to the
    // config that was live. (This consumes rng draws, so the op stream differs from a non-swarm
    // build for the same seed.)
    let mut swarm = op::Swarm::random(rng, enable_force_off);
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
            swarm = op::Swarm::random(rng, enable_force_off);
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
        // Roll for a mid-run restart BEFORE generating an op so an iteration is either
        // a restart OR an apply (not both). The `> 0.0` short-circuits the rng draw at
        // the default value, so workload determinism is preserved for `--restart-probability 0`
        // (which matches the previous no-restart default exactly).
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
            collection.stop_gracefully().await;
            drop(collection);
            collection = fixture::reopen_collection(&collection_dir, &snapshots_dir).await;
            // `Collection::load` returns before tail-of-WAL ops queued to the
            // update worker have been applied — that's an intentional fast-start
            // feature. Wait for the queue to drain so the scroll below observes
            // all WAL-replayed state.
            verify::wait_for_pending_updates(&collection).await;
            let live = verify::collect_model_from_collection(&collection).await;
            verify::assert_matches_model(&live, &model, &format!("restart at op:{i}"));
            eprintln!();
            bar.set_draw_target(ProgressDrawTarget::stderr());
        } else {
            let op = op::Op::random(rng, &model, &active_names, id_pool, &swarm);
            log::debug!("op:{i} {op:?}");
            bar.set_message(op.kind());
            // Log BEFORE apply so a panic preserves the offending op in the trace.
            trace.op(i, &op);
            apply::apply(&collection, &mut model, &mut active_names, &op).await;
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
    // Close and reopen, then re-verify — mirrors gridstore tests.rs:488-516.
    collection.stop_gracefully().await;
    drop(collection);
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
}

/// Skipped on Windows because it is too slow
#[cfg(not(target_os = "windows"))]
#[cfg(test)]
mod tests {
    use super::*;

    const OP_NUM: usize = 8_000;
    const ID_POOL: u64 = 500;

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
    /// Only `disable_optimizer`, `restart_probability`, and `op_num` vary between tests.
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
            None,  // duration: bounded by op_num
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
    /// deterministic (no background segment churn) and steers clear of the known-unfixed
    /// multi-shard reload bug class, so this stays green as a regression gate for the
    /// harness itself rather than a bug-finder (that's the binary's job).
    #[tokio::test(flavor = "multi_thread")]
    async fn harness_no_optimizer_no_restarts() {
        smoke("harness_no_optimizer_no_restarts", true, 0.0, OP_NUM).await;
    }

    /// Same configuration as [`harness_no_optimizer_no_restarts`], but with the optimizer
    /// enabled: background segment churn (merges, indexing) runs concurrently with the op
    /// stream and feeds into the final close+reopen reload check, with no mid-run restarts.
    ///
    /// This is the fourth cell of the {optimizer on/off} × {restarts on/off} matrix. Holding
    /// restarts off isolates the optimizer/reload interaction from the WAL-replay-during-restart
    /// path that [`harness`] also exercises: a failure here points at the optimizer or the final
    /// reload, not at mid-run restart recovery.
    #[tokio::test(flavor = "multi_thread")]
    async fn harness_no_restarts() {
        smoke("harness_no_restarts", false, 0.0, OP_NUM).await;
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
        smoke("harness_no_optimizer", true, 0.002, OP_NUM).await;
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
        smoke("harness", false, 0.002, OP_NUM / 4).await;
    }
}
