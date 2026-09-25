//! Experimental binary that opens a [`ReadOnlyEdgeShard`] directly over object
//! storage (AWS S3 / S3-compatible, or Google Cloud Storage) — or directly over
//! a running Qdrant peer's `StorageRead` gRPC service — and runs a single read
//! request against it.
//!
//! The operation is selected with a sub-command:
//!
//! * `scroll` — paginate over points, optionally filtered.
//! * `search` — nearest-neighbour search for a query vector, optionally filtered.
//! * `search-sparse` — nearest-neighbour search for a sparse query vector,
//!   optionally filtered.
//!
//! All sub-commands accept an arbitrary payload filter as JSON via `--filter`
//! (curl `--data` style: a literal JSON string, `@file` to read from a file, or
//! `@-` to read from stdin), so any condition expressible in Qdrant's filter DSL
//! can be used without adding a dedicated flag. A `--filter-key`/`--filter-value`
//! shortcut is kept for the common "field equals value" case.
//!
//! The shard data is read through the `io_bridge_object_store` blob backend,
//! wrapped in a [`DiskCache`] so each remote block is fetched once and then
//! served from a local mirror directory. Segments are discovered from the
//! leader's segment manifest, and the shard config is derived from the segments
//! themselves, so no `edge_config.json` is required.
//!
//! Example — scroll with a JSON filter (RustFS/MinIO running locally):
//!
//! ```sh
//! cargo run -p edge-shard-query -- \
//!     --backend  aws \
//!     --endpoint http://localhost:9000 \
//!     --bucket   test-bucket \
//!     --region   us-east-1 \
//!     --access-key  rustfsadmin \
//!     --secret-key  rustfsadmin \
//!     --prefix   collection/0 \
//!     scroll \
//!     --filter '{"must":[{"key":"city","match":{"value":"London"}}]}' \
//!     --limit  20
//! ```
//!
//! Example — vector search reading the query vector from a file:
//!
//! ```sh
//! cargo run -p edge-shard-query -- \
//!     --backend gcs \
//!     --bucket  my-bucket \
//!     --gcs-service-account-path /path/to/key.json \
//!     --prefix  collection/0 \
//!     search \
//!     --vector @query.json \
//!     --limit  5
//! ```
//!
//! Example — sparse vector search (sparse vectors are named, so `--using` is
//! usually required):
//!
//! ```sh
//! cargo run -p edge-shard-query -- \
//!     --backend  aws \
//!     --endpoint http://localhost:9000 \
//!     --bucket   test-bucket \
//!     --prefix   collection/0 \
//!     search-sparse \
//!     --using  text \
//!     --vector '{"indices": [12, 700, 5301], "values": [0.4, 0.9, 0.2]}' \
//!     --limit  5
//! ```
//!
//! `--prefix` is the key prefix inside the bucket that points at the edge-shard
//! root — the directory that contains `edge_config.json` and `segments/`.
//!
//! Example — read a shard straight from a running Qdrant instance over its
//! `StorageRead` gRPC service (no object storage involved; the instance must
//! run with `QDRANT__FEATURE_FLAGS__WRITE_SEGMENT_MANIFEST=true` so the shard
//! has a segment manifest). The shard is addressed by `--collection` and
//! `--shard-id`; `--prefix` stays empty:
//!
//! ```sh
//! cargo run -p edge-shard-query -- \
//!     --backend    uio-grpc \
//!     --endpoint   http://localhost:6334 \
//!     --collection my_collection \
//!     --shard-id   0 \
//!     scroll \
//!     --limit 10
//! ```
//!
//! With `--live-reload <SECONDS>` the tool keeps running after the first
//! answer: every interval it [`live_reload`](ReadOnlyEdgeShard::live_reload)s the
//! shard from object storage, re-runs the same request, and prints the
//! difference against the previous results (`+` appeared, `-` disappeared,
//! `~` changed), so a leader writing to the same bucket can be observed live.
//! `--live-reload-key` is the same loop with each reload triggered manually by
//! pressing Enter instead of a timer — handy when stepping through a debug
//! scenario.

mod backend;
mod cli;
mod parse;
mod report;
mod request;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser as _;
use common::uio_trace;
use common::universal_io::DiskCache;
use edge::{EdgeConfig, ReadOnlyEdgeShard};
use io_bridge_object_store::{AsyncRead, BlobFile, ObjectStoreSource};
use io_bridge_uio_grpc::UioGrpcSource;
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;

use crate::backend::{build_aws_config, build_cached_fs, build_gcs_config, build_uio_config};
use crate::cli::{Backend, Cli, ConnectionArgs, ReloadTrigger};
use crate::report::{print_diff, print_io_stats};
use crate::request::PreparedRequest;

/// Open the read-only shard over backend `A` and dispatch the requested command.
///
/// Generic over the object-storage backend handle `A` (e.g.
/// `ObjectStoreSource<AmazonS3>` or `ObjectStoreSource<GoogleCloudStorage>`) so
/// the whole read path stays monomorphic per backend. `remote_config` is that
/// backend's connection config.
fn run<A>(cli: &Cli, prefix: &Path, cache_dir: &Path, remote_config: A::Config) -> Result<()>
where
    A: AsyncRead + Clone,
{
    // Segment data — and the segment manifest used for discovery — are read through a disk cache:
    // fetched from object storage once, then served from the local mirror directory afterwards.
    let cached_fs = build_cached_fs::<A>(remote_config, prefix, cache_dir)?;
    let stats = cached_fs.remote_fs().stats();
    log::info!("caching segment reads under {}", cache_dir.display());

    // Build the request before the open: the shard is opened for exactly this request, so the
    // request's load profile decides which segment components are warmed at all (unless disabled
    // with `--no-load-profile`). An omitted `--vector` stays a placeholder until the open
    // reveals the dimension (the profile only needs the vector name).
    let mut request = PreparedRequest::build(&cli.command)?;
    let load_profile = (!cli.connection.no_load_profile).then(|| request.load_profile());

    // No edge_config.json: `ReadOnlyEdgeShard` derives its config from the segments and discovers
    // them via the manifest. `prefix` is passed only as the shard's (logical) path label. A
    // caller-provided `--search-threads` overrides the derived search-pool size (segments never
    // carry `max_search_threads`, so this is the only way to set it here).
    let config = cli
        .connection
        .search_threads
        .map(|n| EdgeConfig::builder().max_search_threads(n).build());

    log::info!("Load profile: {load_profile:?}");

    let before_open = stats.snapshot();
    let shard = uio_trace::Phase::start("open")
        .in_scope(|| {
            ReadOnlyEdgeShard::<DiskCache<BlobFile<A>>>::open(
                cached_fs,
                prefix,
                config,
                load_profile,
            )
        })
        .context("failed to open read-only edge shard over object storage");
    print_io_stats("open", &stats.snapshot().delta_since(&before_open));
    let shard = shard?;
    log::info!("opened shard with {} segment(s)", shard.segments_count());

    let before_prepare = stats.snapshot();
    let result = uio_trace::Phase::start("prepare").in_scope(|| request.fill_random_vector(&shard));
    print_io_stats("prepare", &stats.snapshot().delta_since(&before_prepare));
    result?;

    let before_query = stats.snapshot();
    let result = uio_trace::Phase::start("query").in_scope(|| request.run(&shard));
    print_io_stats("query", &stats.snapshot().delta_since(&before_query));
    let (rows, next_offset) = result?;
    request.print_full(&rows, next_offset.as_ref())?;

    let Some(trigger) = ReloadTrigger::from_cli(cli) else {
        return Ok(());
    };

    // Live-reload loop: on every trigger (timer tick or Enter), live_reload the
    // shard from the backend, re-run the same request, and print the diff
    // against the previous run. Runs until interrupted (or stdin closes, in
    // key mode).
    let mut previous = rows;
    for iteration in 1u64.. {
        if !trigger.wait()? {
            log::info!("stdin closed; stopping live-reload");
            return Ok(());
        }

        let before_reload = stats.snapshot();
        let result = uio_trace::Phase::start("reload").in_scope(|| shard.live_reload());
        print_io_stats("reload", &stats.snapshot().delta_since(&before_reload));
        if let Err(err) = result {
            // The shard keeps serving its previous state; retry on the next trigger.
            log::error!("live_reload failed (will retry on next reload): {err}");
            continue;
        }
        log::info!(
            "live-reload #{iteration}: live_reloaded shard, {} segment(s)",
            shard.segments_count(),
        );

        let before_query = stats.snapshot();
        let result = uio_trace::Phase::start("query").in_scope(|| request.run(&shard));
        print_io_stats("query", &stats.snapshot().delta_since(&before_query));
        let (rows, _) = result?;
        println!("--- live_reload #{iteration}: diff vs previous results ---");
        print_diff(&previous, &rows)?;
        previous = rows;
    }
    unreachable!("live-reload loop only ends by interruption");
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    // `serverless_compatible` is private on `FeatureFlags`: set only through deserialization.
    let feature_flags: common::flags::FeatureFlags =
        serde_json::from_value(serde_json::json!({ "serverless_compatible": true }))
            .expect("serverless_compatible is a valid FeatureFlags field");
    common::flags::init_feature_flags(feature_flags);
    let cli = Cli::parse();
    let conn = &cli.connection;
    let _flush_trace = conn.uio_trace.as_ref().map(uio_trace::start).transpose()?;
    let prefix = PathBuf::from(&conn.prefix);
    let cache_dir = conn
        .cache_dir
        .clone()
        .unwrap_or_else(|| default_cache_dir(conn));

    log::info!(
        "opening read-only edge shard backend={:?} bucket={:?} collection={:?} prefix={:?}",
        conn.backend,
        conn.bucket,
        conn.collection,
        conn.prefix,
    );

    match conn.backend {
        Backend::Aws => {
            run::<ObjectStoreSource<AmazonS3>>(&cli, &prefix, &cache_dir, build_aws_config(conn)?)
        }
        Backend::Gcs => run::<ObjectStoreSource<GoogleCloudStorage>>(
            &cli,
            &prefix,
            &cache_dir,
            build_gcs_config(conn)?,
        ),
        Backend::UioGrpc => {
            run::<UioGrpcSource>(&cli, &prefix, &cache_dir, build_uio_config(conn)?)
        }
    }
}

/// Default local mirror directory. The mirror is keyed by `--prefix`-relative
/// remote paths, so for `uio-grpc` — whose prefix is empty, the shard being
/// addressed by `--collection`/`--shard-id` instead — the replica identity is
/// folded into the directory to keep two shards from colliding in one mirror.
fn default_cache_dir(conn: &ConnectionArgs) -> PathBuf {
    let base = std::env::temp_dir().join("edge-shard-query-cache");
    match conn.backend {
        Backend::Aws | Backend::Gcs => base,
        Backend::UioGrpc => base
            .join(conn.collection.as_deref().unwrap_or("default"))
            .join(conn.shard_id.to_string()),
    }
}
