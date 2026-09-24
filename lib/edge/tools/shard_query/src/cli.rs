//! Command-line definitions: connection options and the read sub-commands.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use edge::Filter;

use crate::parse::{build_kv_filter, read_data_arg};

/// Storage backend to read the shard from.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Backend {
    /// AWS S3 or an S3-compatible store (MinIO, RustFS, LocalStack, ...).
    Aws,
    /// Google Cloud Storage.
    Gcs,
    /// A running Qdrant peer's `StorageRead` gRPC service (its public gRPC
    /// endpoint). The shard is addressed by `--collection`/`--shard-id`.
    UioGrpc,
}

#[derive(Parser, Debug)]
#[command(
    about = "Open a ReadOnlyEdgeShard over S3/GCS object storage and run a read request (scroll/search)"
)]
pub struct Cli {
    /// Object-storage connection and shard-location options. Must be given
    /// before the sub-command.
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Live-reload polling interval in seconds. When set, the tool keeps
    /// running after the first answer: every interval it live_reloads the shard
    /// from object storage, re-runs the same request, and prints the
    /// difference against the previous results (`+` appeared, `-` disappeared,
    /// `~` changed). Must be given before the sub-command.
    #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(1..))]
    pub live_reload: Option<u64>,

    /// Like `--live-reload`, but each reload is triggered manually by pressing
    /// Enter instead of on a timer — easier when stepping through a debug
    /// scenario. Not compatible with `@-` (stdin) request arguments.
    #[arg(long, conflicts_with = "live_reload")]
    pub live_reload_key: bool,

    /// Which read request to run against the shard.
    #[command(subcommand)]
    pub command: Command,
}

/// What triggers each live-reload iteration.
pub enum ReloadTrigger {
    /// Live-reload every interval (`--live-reload <SECONDS>`).
    Timer(Duration),
    /// Live-reload when the user presses Enter (`--live-reload-key`).
    Key,
}

impl ReloadTrigger {
    pub fn from_cli(cli: &Cli) -> Option<Self> {
        if cli.live_reload_key {
            // `conflicts_with` guarantees `--live-reload` is not also set.
            return Some(Self::Key);
        }
        cli.live_reload
            .map(|secs| Self::Timer(Duration::from_secs(secs)))
    }

    /// Block until the next reload should run. `false` means the trigger is
    /// exhausted (stdin closed) and the loop should end.
    pub fn wait(&self) -> Result<bool> {
        match self {
            Self::Timer(interval) => {
                std::thread::sleep(*interval);
                Ok(true)
            }
            Self::Key => {
                log::info!("press Enter to live-reload (Ctrl+C to quit)");
                let mut line = String::new();
                let read = std::io::stdin()
                    .read_line(&mut line)
                    .context("failed to read from stdin")?;
                Ok(read > 0)
            }
        }
    }
}

/// How to reach the object store and where the shard lives inside it. Shared by
/// every sub-command.
#[derive(ClapArgs, Debug)]
pub struct ConnectionArgs {
    /// Which object-storage backend to use.
    #[arg(long, value_enum, default_value = "aws")]
    pub backend: Backend,

    /// [AWS/GCS] Bucket name (without any scheme prefix). Required for the
    /// object-storage backends; unused by `uio-grpc`.
    #[arg(long, env = "BLOB_BUCKET")]
    pub bucket: Option<String>,

    /// [AWS] Custom S3 endpoint URL (set for MinIO / RustFS / LocalStack; omit for real AWS).
    /// [UIO] The Qdrant peer's public gRPC URL (e.g. `http://localhost:6334`); required.
    #[arg(long, env = "S3_ENDPOINT")]
    pub endpoint: Option<String>,

    /// [AWS] Region (e.g. `us-east-1`). Required for real AWS; optional for S3-compatible endpoints.
    #[arg(long, env = "S3_REGION")]
    pub region: Option<String>,

    /// [AWS] Access key id. If omitted, the AWS default credential chain is used.
    #[arg(long, env = "S3_ACCESS_KEY")]
    pub access_key: Option<String>,

    /// [AWS] Secret access key. Required when `--access-key` is given.
    #[arg(long, env = "S3_SECRET_KEY")]
    pub secret_key: Option<String>,

    /// [AWS] Optional session token for short-lived credentials.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    pub session_token: Option<String>,

    /// [AWS] Use S3 Express One Zone (directory buckets, named `*--x-s3`).
    #[arg(long, env = "S3_EXPRESS")]
    pub s3_express: bool,

    /// [GCS] Path to a service-account JSON key file. Takes precedence over
    /// `--gcs-service-account-key`; if neither is set, application default
    /// credentials (ADC) are used.
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_PATH")]
    pub gcs_service_account_path: Option<String>,

    /// [GCS] Inline service-account JSON key contents (instead of a path).
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_KEY")]
    pub gcs_service_account_key: Option<String>,

    /// [UIO] Collection name on the Qdrant peer; required.
    #[arg(long, env = "QDRANT_COLLECTION")]
    pub collection: Option<String>,

    /// [UIO] Shard id within the collection.
    #[arg(long, default_value_t = 0)]
    pub shard_id: u32,

    /// [UIO] Qdrant API key sent on every request. Omit for an unauthenticated peer.
    #[arg(long, env = "QDRANT_API_KEY")]
    pub api_key: Option<String>,

    /// [AWS/GCS] Key prefix inside the bucket pointing at the edge-shard root
    /// (contains `edge_config.json` and `segments/`). Empty means the bucket
    /// root. The `uio-grpc` backend addresses the shard root by
    /// `--collection`/`--shard-id`, so leave this empty there.
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// Local directory for the segment disk cache. Remote blocks are fetched
    /// from object storage once and mirrored here; later reads hit this
    /// directory instead. Defaults to a stable subdirectory of the system temp
    /// dir, so the cache persists across runs.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,

    /// Number of threads in the shard's search thread pool (used to read
    /// segments in parallel at open and to run searches). `0` derives the count
    /// from the number of available CPUs. Omit to use the default.
    #[arg(long)]
    pub search_threads: Option<usize>,

    /// Disable the request-derived load profile: warm every segment component
    /// per the persisted segment configs, like a long-lived deployment would,
    /// instead of parking the components this request won't touch cold.
    #[arg(long, default_value_t = false)]
    pub no_load_profile: bool,

    /// Record every network storage request and save into this file.
    #[arg(long)]
    pub uio_trace: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Paginate over points, optionally filtered.
    Scroll(ScrollArgs),
    /// Nearest-neighbour search for a query vector, optionally filtered.
    Search(SearchArgs),
    /// Nearest-neighbour search for a sparse query vector, optionally filtered.
    SearchSparse(SearchSparseArgs),
}

/// Filter and result options shared by every sub-command.
#[derive(ClapArgs, Debug)]
pub struct CommonReadArgs {
    /// Payload filter as JSON, in Qdrant's filter DSL (the `filter` field of a
    /// REST request). Accepts a literal JSON string, `@path` to read the JSON
    /// from a file, or `@-` to read it from stdin. Mutually exclusive with the
    /// `--filter-key`/`--filter-value` shortcut.
    #[arg(long, conflicts_with_all = ["filter_key", "filter_value"])]
    pub filter: Option<String>,

    /// Shortcut: payload field to filter on (e.g. `city`). Builds a single
    /// "field equals value" condition together with `--filter-value`.
    #[arg(long, requires = "filter_value")]
    pub filter_key: Option<String>,

    /// Shortcut: value the `--filter-key` field must equal. Parsed as an integer
    /// or boolean when it looks like one, otherwise as a string.
    #[arg(long, requires = "filter_key")]
    pub filter_value: Option<String>,

    /// Maximum number of points to return.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,

    /// Include vectors in the output.
    #[arg(long, default_value_t = false)]
    pub with_vectors: bool,
}

impl CommonReadArgs {
    /// Resolve the effective filter from either `--filter` (JSON) or the
    /// `--filter-key`/`--filter-value` shortcut. Returns `None` when neither is
    /// given (operate over all points). `clap` already guarantees the two
    /// sources are mutually exclusive and that key/value come as a pair.
    pub fn resolve_filter(&self) -> Result<Option<Filter>> {
        if let Some(raw) = &self.filter {
            let json = read_data_arg(raw)?;
            let filter = serde_json::from_str(&json)
                .with_context(|| format!("failed to parse --filter as a JSON filter: {json}"))?;
            return Ok(Some(filter));
        }
        build_kv_filter(self.filter_key.as_deref(), self.filter_value.as_deref())
    }
}

#[derive(ClapArgs, Debug)]
pub struct ScrollArgs {
    #[command(flatten)]
    pub common: CommonReadArgs,

    /// Start ID to read points from (resume a previous page). Accepts an integer
    /// id or a UUID string.
    #[arg(long)]
    pub offset: Option<String>,

    /// Order the records by this payload field instead of by id.
    #[arg(long)]
    pub order_by: Option<String>,
}

#[derive(ClapArgs, Debug)]
pub struct SearchArgs {
    #[command(flatten)]
    pub common: CommonReadArgs,

    /// Query vector. Accepts a JSON array (`[0.1, 0.2, ...]`), a comma-separated
    /// list (`0.1,0.2,...`), or `@path`/`@-` to read either form from a file or
    /// stdin. Omit to search with a random vector — its dimension is read from
    /// the shard config after the shard opens.
    #[arg(long)]
    pub vector: Option<String>,

    /// Name of the vector to search (for collections with named vectors). Omit
    /// to use the default/unnamed vector.
    #[arg(long)]
    pub using: Option<String>,

    /// Number of results to skip before collecting `--limit` results.
    #[arg(long, default_value_t = 0)]
    pub offset: usize,

    /// Only return results scoring at least this value.
    #[arg(long)]
    pub score_threshold: Option<f32>,

    /// HNSW `ef` search parameter (size of the beam). Larger is more accurate
    /// and slower.
    #[arg(long)]
    pub hnsw_ef: Option<usize>,

    /// Search exhaustively without the HNSW approximation (slow, exact).
    #[arg(long, default_value_t = false)]
    pub exact: bool,
}

#[derive(ClapArgs, Debug)]
pub struct SearchSparseArgs {
    #[command(flatten)]
    pub common: CommonReadArgs,

    /// Sparse query vector. Accepts a JSON object (`{"indices": [12, 700],
    /// "values": [0.4, 0.9]}`), a comma-separated list of `index:value` pairs
    /// (`12:0.4,700:0.9`), or `@path`/`@-` to read either form from a file or
    /// stdin. Indices don't have to be sorted.
    #[arg(long)]
    pub vector: String,

    /// Name of the sparse vector to search. Sparse vectors are named, so this
    /// is usually required; omit only if the shard stores its sparse vector
    /// under the default (empty) name.
    #[arg(long)]
    pub using: Option<String>,

    /// Number of results to skip before collecting `--limit` results.
    #[arg(long, default_value_t = 0)]
    pub offset: usize,

    /// Only return results scoring at least this value.
    #[arg(long)]
    pub score_threshold: Option<f32>,

    /// Search exhaustively without the sparse index (slow, exact).
    #[arg(long, default_value_t = false)]
    pub exact: bool,
}
