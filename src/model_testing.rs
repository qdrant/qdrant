use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use collection::profiling::interface::init_requests_profile_collector;
use common::flags::{FeatureFlags, init_feature_flags};
use common::mmap::MULTI_MMAP_SUPPORT_CHECK_RESULT;
#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(
    not(target_env = "msvc"),
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Long-running soak test: random ops against a Collection, verified live against an in-memory model"
)]
struct Args {
    /// RNG seed — same seed reproduces the exact op sequence.
    #[clap(long, default_value_t = 0)]
    seed: u64,

    /// Number of randomized operations to apply. Ignored as the stop condition when
    /// `--duration` is set (the run is then bounded by wall-clock time instead).
    #[clap(long, default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..))]
    op_num: u64,

    /// Run continuously for this many wall-clock seconds instead of stopping after `--op-num` ops.
    /// When set, `--op-num` is ignored as the stop condition and the run ends at the deadline (or
    /// on Ctrl-C, whichever comes first). The post-run live verification + final close/reopen
    /// reload check run as usual on whatever ops were applied. Use this for time-boxed soak runs
    /// (e.g. an overnight or per-CI-slot budget) where the interesting variable is "how long"
    /// rather than "how many ops".
    #[clap(long, value_parser = clap::value_parser!(u64).range(1..))]
    duration_sec: Option<u64>,

    /// Number of shards in the test collection.
    #[clap(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    shard_count: u32,

    /// Size of the point ID space (ids are drawn uniformly from a pool of this many ids,
    /// precomputed at startup; see `--uuid-id-fraction` for the numeric/UUID split).
    #[clap(long, default_value_t = 500, value_parser = clap::value_parser!(u64).range(1..))]
    id_pool: u64,

    /// Fraction (0.0..=1.0) of the id pool backed by UUID point ids instead of numeric ones,
    /// exercising the UUID side of the id tracker, WAL round-tripping and shard routing. The
    /// UUID slots are well-formed v4 ids drawn once at startup from the seeded rng, so the
    /// pool is stable for the run, `--seed` reproduces it exactly, and ids keep their
    /// upsert-overwrite / delete-hits-live-point reuse semantics. 0.0 consumes no rng draws
    /// and reproduces the numeric-only op stream of builds without UUID support.
    #[clap(long, default_value_t = 0.5)]
    uuid_id_fraction: f64,

    /// Where to write the collection + snapshots data. Wiped at the start of each run;
    /// copy the directory out beforehand to preserve a previous run for post-mortem.
    #[clap(long, default_value = "./storage-model")]
    storage_path: PathBuf,

    /// Disable the segment optimizer entirely (sets `max_optimization_threads=0` in the
    /// fixture). Useful for isolating optimizer-race bugs: if a failure stops reproducing
    /// with this flag on, the bug depends on concurrent optimization.
    #[clap(long, default_value_t = false)]
    disable_optimizer: bool,

    /// Optimizer `max_segment_size` in KB. Smaller values produce more segments and more
    /// optimizer churn (more chances to trip races); larger values mimic production
    /// cadence. The fixture's default (10 KB) is well below production thresholds because
    /// the soak collection only holds a few MB total — at production defaults
    /// (~200 MB) the optimizer would never fire on this workload.
    #[clap(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    max_segment_size_kb: u64,

    /// Optimizer `indexing_threshold` in KB. Same rationale as `max_segment_size_kb`:
    /// scaled down from production (~20 MB) to fire on the soak's small workload.
    #[clap(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    indexing_threshold_kb: u64,

    /// Periodic flush worker interval in seconds. Lower values mean smaller unflushed-WAL
    /// windows (more production-realistic given the soak's high op rate); larger values
    /// stress the unflushed-WAL path and surface bugs like the `DeleteVectorName` replay
    /// issue where historical Upserts referencing a since-deleted vector name fail.
    #[clap(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    flush_interval_sec: u64,

    /// Per-iteration probability (0.0..=1.0) of restarting the collection mid-run:
    /// close + reopen + full model verification. Surfaces reload/WAL-replay bugs at the
    /// op where they're introduced rather than only at end-of-run. 0.0 (default) skips
    /// restarts entirely; small positive values (e.g. 0.005) are enough to expose the
    /// known engine bugs without dominating the run with restart overhead.
    #[clap(long, default_value_t = 0.0)]
    restart_probability: f64,

    /// Recompute the swarm config (the random subset of enabled ops) every N ops, turning one
    /// long run into a sequence of swarm sub-tests for broader feature-interaction coverage
    /// (Groce et al., "Swarm Testing"). Each redraw is logged to the trace. Set
    /// `>= op_num` for a single config for the whole run. Default 10000 — long enough for a
    /// config to build up meaningful state (e.g. a no-delete epoch grows segments) before the
    /// next redraw; smaller values trade that depth for more config breadth.
    #[clap(long, default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..))]
    swarm_interval: u64,

    /// Set `on_disk` on the dense vector params. Vectors are persisted to disk either way; this
    /// chooses mmap-on-demand storage (`VectorStorageType::Mmap`, reads hit the page cache/disk)
    /// over a RAM-resident copy (`InRamMmap`, mmap populated into memory). Required for
    /// `--async-scorer` to actually exercise io_uring: with an in-RAM copy, reads are served from
    /// memory and the async read path is bypassed even though the io_uring storage is opened.
    #[clap(long, default_value_t = false)]
    on_disk: bool,

    /// Enable the io_uring async scorer for mmap dense vector storage (Linux only). Falls back to
    /// plain mmap with a logged `failed to open io_uring based vector storage` error if io_uring is
    /// unavailable — check the log to confirm it actually engaged. Pair with `--on-disk` (otherwise
    /// reads come from the RAM-resident copy); has no effect on sparse or multivector storage.
    #[clap(long, default_value_t = false)]
    async_scorer: bool,

    /// Run the live pre-close verification on a mid-run restart (the full model check before
    /// `stop_gracefully`). Off by default: it's slow, and scrolling the whole collection first
    /// warms caches / forces lazy loads that can mask a reload divergence, so the default cold
    /// close+reopen surfaces more bugs. Enable it to attribute a `restart at op:N` failure to the
    /// apply path vs. the reload path.
    #[clap(long, default_value_t = false)]
    pre_restart_check: bool,

    /// Fix the number of Tokio worker threads instead of defaulting to the CPU core count.
    /// Doesn't make thread interleaving reproducible (work-stealing timing, OS scheduling and
    /// I/O readiness still vary), but pins the runtime shape so a `--seed` repro shares the same
    /// worker count across machines. Leave unset for full parallelism on soak runs.
    #[clap(long, value_parser = clap::value_parser!(u64).range(1..))]
    worker_threads: Option<u64>,

    /// Promote the always-disabled (`FORCE_OFF`) ops — DeleteByFilter, CreateVectorName,
    /// DeleteVectorName — to forced-on, so they're enabled in every swarm config and guaranteed
    /// to fire. These ops are masked off by default because they trip known engine bugs (see the
    /// `FORCE_OFF` comment in `op/mod.rs`); enable this to deliberately reproduce them. The rng-draw
    /// count is unchanged vs. the default, so the non-broken op stream stays reproducible per seed.
    #[clap(long, default_value_t = false)]
    enable_force_off: bool,

    /// Disable the `CreateSnapshot` op entirely — it's masked out of every swarm config. Off by
    /// default, so a normal soak takes snapshots in the background concurrently with the workload
    /// (the archive is discarded — this stresses snapshot *creation* under concurrent writes, not
    /// recovery). The op draws no rng, so `--seed` reproduces identically whether or not snapshots
    /// are enabled; disable them only to avoid the background snapshot IO. The rng-draw count for
    /// the swarm config is unchanged either way.
    #[clap(long, default_value_t = false)]
    disable_snapshots: bool,
}

fn main() {
    let args = Args::parse();

    // Built by hand (not `#[tokio::main]`) to seed Tokio's RNG off `--seed`, pinning in-poll
    // draws like `select!` branch selection. `rng_seed` needs `--cfg tokio_unstable`; without
    // it the binary still builds, just unseeded.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(worker_threads) = args.worker_threads {
        builder.worker_threads(worker_threads as usize);
    }
    #[cfg(tokio_unstable)]
    {
        builder.rng_seed(tokio::runtime::RngSeed::from_bytes(
            &args.seed.to_le_bytes(),
        ));
    }
    let runtime = builder.build().expect("failed to build tokio runtime");

    runtime.block_on(run_main(args));
}

async fn run_main(args: Args) {
    env_logger::init();
    init_feature_flags(FeatureFlags::default());
    let _ = MULTI_MMAP_SUPPORT_CHECK_RESULT.set(true);
    init_requests_profile_collector(tokio::runtime::Handle::current());

    // Ctrl-C handler: first signal flags shutdown so the run loop exits cleanly between
    // ops; a second Ctrl-C falls through to the default handler and terminates. Wired
    // BEFORE the run starts so an early signal still gets caught.
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let s = shutdown.clone();
        ctrlc::set_handler(move || {
            if s.swap(true, Ordering::Relaxed) {
                eprintln!("\nmodel_testing: second Ctrl-C, terminating");
                std::process::exit(130);
            } else {
                eprintln!("\nmodel_testing: shutdown requested, draining current op...");
            }
        })
        .expect("failed to install ctrl-c handler");
    }

    if !(0.0..=1.0).contains(&args.restart_probability) {
        eprintln!(
            "model_testing: --restart-probability must be in [0.0, 1.0], got {}",
            args.restart_probability,
        );
        std::process::exit(2);
    }
    if !(0.0..=1.0).contains(&args.uuid_id_fraction) {
        eprintln!(
            "model_testing: --uuid-id-fraction must be in [0.0, 1.0], got {}",
            args.uuid_id_fraction,
        );
        std::process::exit(2);
    }
    // Process-global flag, read when each on-disk dense vector storage is opened — set it before
    // the fixture builds any segments.
    segment::vector_storage::common::set_async_scorer(args.async_scorer);
    // Show the active stop condition: the duration when time-bounded, else the op count.
    let stop = match args.duration_sec {
        Some(s) => format!("duration_sec={s}"),
        None => format!("op_num={}", args.op_num),
    };
    println!(
        "model_testing: seed={} {stop} shard_count={} id_pool={} uuid_id_fraction={} \
         storage_path={} disable_optimizer={} max_segment_size_kb={} indexing_threshold_kb={} \
         flush_interval_sec={} restart_probability={} swarm_interval={} \
         on_disk={} async_scorer={} pre_restart_check={} enable_force_off={} \
         disable_snapshots={}",
        args.seed,
        args.shard_count,
        args.id_pool,
        args.uuid_id_fraction,
        args.storage_path.display(),
        args.disable_optimizer,
        args.max_segment_size_kb,
        args.indexing_threshold_kb,
        args.flush_interval_sec,
        args.restart_probability,
        args.swarm_interval,
        args.on_disk,
        args.async_scorer,
        args.pre_restart_check,
        args.enable_force_off,
        args.disable_snapshots,
    );
    let start = Instant::now();
    collection::model_testing::run(
        args.seed,
        args.op_num as usize,
        args.shard_count,
        args.id_pool,
        args.uuid_id_fraction,
        &args.storage_path,
        args.disable_optimizer,
        args.max_segment_size_kb as usize,
        args.indexing_threshold_kb as usize,
        args.flush_interval_sec,
        args.restart_probability,
        args.swarm_interval as usize,
        args.on_disk,
        args.pre_restart_check,
        args.enable_force_off,
        args.disable_snapshots,
        args.duration_sec.map(Duration::from_secs),
        shutdown,
    )
    .await;
    println!("model_testing: ok in {:.1?}", start.elapsed());
}
