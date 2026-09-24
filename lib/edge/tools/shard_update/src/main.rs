//! Experimental binary that opens an [`UpdateOnlyEdgeShard`] over a local
//! shard directory — or directly over object storage (AWS S3 / S3-compatible,
//! or Google Cloud Storage) — and runs a batch of random upserts against it.
//!
//! The counterpart of `edge-shard-query` for the write path. Point ids to
//! overwrite are taken from the command line; the *shape* of each generated
//! point is derived from the shard's own schema — every dense/sparse vector
//! named in the appendable segment's config, and one payload value per field
//! in its payload-index schema — so the batch is exactly what a real client
//! could have written.
//!
//! By default the tool dry-runs: it runs [`preview_batch`], which shares the
//! decision pipeline with the real apply, and logs what *would* happen —
//! which segments hold each point today, in which slots and at what versions,
//! which points would be stored/skipped, and which slots would be tombstoned.
//! Nothing is written.
//!
//! With `--apply` it calls [`apply_batch`] instead and writes for real:
//! appends to the write target and tombstones in every segment holding an
//! older copy — over object storage through the appendable
//! [`CachedBlobFile`], so the mutations are direct appends or server-side
//! rewrites per the store's capability, durable once the run reports `Ok`.
//! Adding `--interactive` keeps the session open: after each batch the tool
//! prompts on stdin for the next round's ids and applies them through the
//! writer `apply_batch` handed back — no shard re-open — with the op-num
//! incremented per round.
//!
//! [`apply_batch`]: UpdateOnlyEdgeShard::apply_batch
//!
//! Example — local shard directory:
//!
//! ```sh
//! cargo run -p edge-shard-update -- \
//!     --path  ./qdrant_storage/collections/benchmark/0 \
//!     --ids   1,2,42 \
//!     --op-num 20000
//! ```
//!
//! Example — shard on S3-compatible object storage (here: GCS via its S3
//! interoperability endpoint). Segments are discovered from the leader's
//! segment manifest, and reads go through a local disk cache, exactly like
//! `edge-shard-query`:
//!
//! ```sh
//! cargo run -p edge-shard-update -- \
//!     --backend aws \
//!     --bucket   qdrant-benchmark-snapshots \
//!     --endpoint https://storage.googleapis.com \
//!     --region   auto \
//!     --access-key xxxx \
//!     --secret-key xxxx \
//!     --prefix   serverless/shard-100k \
//!     --ids      1,2,42 \
//!     --op-num   20000
//! ```
//!
//! [`preview_batch`]: UpdateOnlyEdgeShard::preview_batch

mod apply;
mod backend;
mod cli;
mod dry_run;
mod generate;
mod parse;
mod schema;

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::Parser as _;
use common::universal_io::MmapFs;
use edge::{ManifestSegmentEnumerator, PointId, UpdateOnlyEdgeShard};
use io_bridge_object_store::{AsyncAppend, CachedBlobFs, ObjectStoreSource};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;

use crate::apply::apply_run;
use crate::backend::{build_aws_config, build_cached_fs, build_gcs_config};
use crate::cli::{Backend, Cli};
use crate::dry_run::dry_run;
use crate::parse::parse_point_id;
use crate::schema::read_schema;

/// Dry-run against a local shard directory: segments discovered by scanning
/// `segments/`, read over memory-mapped files.
fn run_local(cli: &Cli, ids: &[PointId]) -> Result<()> {
    let path = cli
        .connection
        .path
        .clone()
        .ok_or_else(|| anyhow!("--path is required for the local backend"))?;

    let shard = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(&path)
        .context("failed to open update-only edge shard")?;
    log::info!(
        "opened update-only shard with {} segment(s)",
        shard.segments_count()
    );

    let schema = read_schema(&shard, &common::universal_io::MmapFs, &path)?;
    if cli.apply {
        apply_run(shard, &schema, ids, cli.op_num, cli.seed, cli.interactive)
    } else {
        dry_run(&shard, &schema, ids, cli.op_num, cli.seed)
    }
}

/// Run against object storage: segments discovered from the leader's
/// segment manifest, reads through the local disk cache, appends (with
/// `--apply`) straight to the remote.
fn run_remote<A>(cli: &Cli, ids: &[PointId], remote_config: A::Config) -> Result<()>
where
    A: AsyncAppend + Clone,
    A::Config: Clone,
{
    let prefix = PathBuf::from(&cli.connection.prefix);
    let cache_dir = cli
        .connection
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("edge-shard-update-cache"));

    let cached_fs = build_cached_fs::<A>(remote_config, &prefix, &cache_dir)?;
    log::info!("caching segment reads under {}", cache_dir.display());

    let enumerator = ManifestSegmentEnumerator::new(cached_fs.clone(), &prefix);
    let shard =
        UpdateOnlyEdgeShard::<CachedBlobFs<A>>::open(cached_fs.clone(), &prefix, enumerator)
            .context("failed to open update-only edge shard over object storage")?;
    log::info!(
        "opened update-only shard with {} segment(s)",
        shard.segments_count()
    );

    let schema = read_schema(&shard, &cached_fs, &prefix)?;
    if cli.apply {
        apply_run(shard, &schema, ids, cli.op_num, cli.seed, cli.interactive)
    } else {
        dry_run(&shard, &schema, ids, cli.op_num, cli.seed)
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();

    let ids = cli
        .ids
        .iter()
        .map(|raw| parse_point_id(raw))
        .collect::<Result<Vec<_>>>()?;
    log::info!("dry-run upsert of {} random point(s): {ids:?}", ids.len());

    let conn = &cli.connection;
    match conn.backend {
        Backend::Local => run_local(&cli, &ids),
        Backend::Aws => {
            run_remote::<ObjectStoreSource<AmazonS3>>(&cli, &ids, build_aws_config(conn)?)
        }
        Backend::Gcs => {
            run_remote::<ObjectStoreSource<GoogleCloudStorage>>(&cli, &ids, build_gcs_config(conn)?)
        }
    }
}
