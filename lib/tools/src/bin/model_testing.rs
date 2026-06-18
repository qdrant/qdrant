use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

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

    /// Number of randomized operations to apply.
    #[clap(long, default_value_t = 10_000, value_parser = clap::value_parser!(u64).range(1..))]
    op_num: u64,

    /// Number of shards in the test collection.
    #[clap(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    shard_count: u32,

    /// Size of the point ID space (ids are drawn uniformly from 0..id_pool).
    #[clap(long, default_value_t = 500, value_parser = clap::value_parser!(u64).range(1..))]
    id_pool: u64,

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

    /// Promote the always-disabled (`FORCE_OFF`) ops — DeleteByFilter, CreateVectorName,
    /// DeleteVectorName — to forced-on, so they're enabled in every swarm config and guaranteed
    /// to fire. These ops are masked off by default because they trip known engine bugs (see the
    /// `FORCE_OFF` comment in `op/mod.rs`); enable this to deliberately reproduce them. The rng-draw
    /// count is unchanged vs. the default, so the non-broken op stream stays reproducible per seed.
    #[clap(long, default_value_t = false)]
    enable_force_off: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::init();
    init_feature_flags(FeatureFlags::default());
    let _ = MULTI_MMAP_SUPPORT_CHECK_RESULT.set(true);
    init_requests_profile_collector(tokio::runtime::Handle::current());
    let args = Args::parse();

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
    // Process-global flag, read when each on-disk dense vector storage is opened — set it before
    // the fixture builds any segments.
    segment::vector_storage::common::set_async_scorer(args.async_scorer);
    println!(
        "model_testing: seed={} op_num={} shard_count={} id_pool={} storage_path={} \
         disable_optimizer={} max_segment_size_kb={} indexing_threshold_kb={} \
         flush_interval_sec={} restart_probability={} swarm_interval={} on_disk={} \
         async_scorer={} pre_restart_check={} enable_force_off={}",
        args.seed,
        args.op_num,
        args.shard_count,
        args.id_pool,
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
    );
    let start = Instant::now();
    collection::model_testing::run(
        args.seed,
        args.op_num as usize,
        args.shard_count,
        args.id_pool,
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
        shutdown,
    )
    .await;
    println!("model_testing: ok in {:.1?}", start.elapsed());
}
