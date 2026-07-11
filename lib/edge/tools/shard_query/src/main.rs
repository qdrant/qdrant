//! Experimental binary that opens a [`ReadOnlyEdgeShard`] directly over object
//! storage (AWS S3 / S3-compatible, or Google Cloud Storage) — or directly over
//! a running Qdrant peer's `StorageRead` gRPC service — and runs a single read
//! request against it.
//!
//! The operation is selected with a sub-command:
//!
//! * `scroll` — paginate over points, optionally filtered.
//! * `search` — nearest-neighbour search for a query vector, optionally filtered.
//!
//! Both sub-commands accept an arbitrary payload filter as JSON via `--filter`
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
//! answer: every interval it [`refresh`](ReadOnlyEdgeShard::refresh)es the
//! shard from object storage, re-runs the same request, and prints the
//! difference against the previous results (`+` appeared, `-` disappeared,
//! `~` changed), so a leader writing to the same bucket can be observed live.
//! `--live-reload-key` is the same loop with each reload triggered manually by
//! pressing Enter instead of a timer — handy when stepping through a debug
//! scenario.

use std::collections::HashMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use common::universal_io::{
    DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
};
use edge::{
    Condition, EdgeConfig, EdgeShardRead, FieldCondition, Filter, JsonPath, LoadProfile, Match,
    NamedQuery, OrderByInterface, PointId, QueryEnum, ReadOnlyEdgeShard, Record, ScoredPoint,
    ScrollRequest, SearchParams, SearchRequest, ValueVariants, VectorInternal,
    WithPayloadInterface,
};
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
use io_bridge_object_store::{AsyncRead, BlobFile, ObjectStoreSource};
use io_bridge_uio_grpc::{UioGrpcConfig, UioGrpcSource};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;

/// Storage backend to read the shard from.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Backend {
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
struct Cli {
    /// Object-storage connection and shard-location options. Must be given
    /// before the sub-command.
    #[command(flatten)]
    connection: ConnectionArgs,

    /// Live-reload polling interval in seconds. When set, the tool keeps
    /// running after the first answer: every interval it refreshes the shard
    /// from object storage, re-runs the same request, and prints the
    /// difference against the previous results (`+` appeared, `-` disappeared,
    /// `~` changed). Must be given before the sub-command.
    #[arg(long, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(1..))]
    live_reload: Option<u64>,

    /// Like `--live-reload`, but each reload is triggered manually by pressing
    /// Enter instead of on a timer — easier when stepping through a debug
    /// scenario. Not compatible with `@-` (stdin) request arguments.
    #[arg(long, conflicts_with = "live_reload")]
    live_reload_key: bool,

    /// Which read request to run against the shard.
    #[command(subcommand)]
    command: Command,
}

/// What triggers each live-reload iteration.
enum ReloadTrigger {
    /// Refresh every interval (`--live-reload <SECONDS>`).
    Timer(Duration),
    /// Refresh when the user presses Enter (`--live-reload-key`).
    Key,
}

impl ReloadTrigger {
    fn from_cli(cli: &Cli) -> Option<Self> {
        if cli.live_reload_key {
            // `conflicts_with` guarantees `--live-reload` is not also set.
            return Some(Self::Key);
        }
        cli.live_reload
            .map(|secs| Self::Timer(Duration::from_secs(secs)))
    }

    /// Block until the next reload should run. `false` means the trigger is
    /// exhausted (stdin closed) and the loop should end.
    fn wait(&self) -> Result<bool> {
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
struct ConnectionArgs {
    /// Which object-storage backend to use.
    #[arg(long, value_enum, default_value = "aws")]
    backend: Backend,

    /// [AWS/GCS] Bucket name (without any scheme prefix). Required for the
    /// object-storage backends; unused by `uio-grpc`.
    #[arg(long, env = "BLOB_BUCKET")]
    bucket: Option<String>,

    /// [AWS] Custom S3 endpoint URL (set for MinIO / RustFS / LocalStack; omit for real AWS).
    /// [UIO] The Qdrant peer's public gRPC URL (e.g. `http://localhost:6334`); required.
    #[arg(long, env = "S3_ENDPOINT")]
    endpoint: Option<String>,

    /// [AWS] Region (e.g. `us-east-1`). Required for real AWS; optional for S3-compatible endpoints.
    #[arg(long, env = "S3_REGION")]
    region: Option<String>,

    /// [AWS] Access key id. If omitted, the AWS default credential chain is used.
    #[arg(long, env = "S3_ACCESS_KEY")]
    access_key: Option<String>,

    /// [AWS] Secret access key. Required when `--access-key` is given.
    #[arg(long, env = "S3_SECRET_KEY")]
    secret_key: Option<String>,

    /// [AWS] Optional session token for short-lived credentials.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    session_token: Option<String>,

    /// [AWS] Use S3 Express One Zone (directory buckets, named `*--x-s3`).
    #[arg(long, env = "S3_EXPRESS")]
    s3_express: bool,

    /// [GCS] Path to a service-account JSON key file. Takes precedence over
    /// `--gcs-service-account-key`; if neither is set, application default
    /// credentials (ADC) are used.
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_PATH")]
    gcs_service_account_path: Option<String>,

    /// [GCS] Inline service-account JSON key contents (instead of a path).
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_KEY")]
    gcs_service_account_key: Option<String>,

    /// [UIO] Collection name on the Qdrant peer; required.
    #[arg(long, env = "QDRANT_COLLECTION")]
    collection: Option<String>,

    /// [UIO] Shard id within the collection.
    #[arg(long, default_value_t = 0)]
    shard_id: u32,

    /// [UIO] Qdrant API key sent on every request. Omit for an unauthenticated peer.
    #[arg(long, env = "QDRANT_API_KEY")]
    api_key: Option<String>,

    /// [AWS/GCS] Key prefix inside the bucket pointing at the edge-shard root
    /// (contains `edge_config.json` and `segments/`). Empty means the bucket
    /// root. The `uio-grpc` backend addresses the shard root by
    /// `--collection`/`--shard-id`, so leave this empty there.
    #[arg(long, default_value = "")]
    prefix: String,

    /// Local directory for the segment disk cache. Remote blocks are fetched
    /// from object storage once and mirrored here; later reads hit this
    /// directory instead. Defaults to a stable subdirectory of the system temp
    /// dir, so the cache persists across runs.
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Number of threads in the shard's search thread pool (used to read
    /// segments in parallel at open and to run searches). `0` derives the count
    /// from the number of available CPUs. Omit to use the default.
    #[arg(long)]
    search_threads: Option<usize>,

    /// Disable the request-derived load profile: warm every segment component
    /// per the persisted segment configs, like a long-lived deployment would,
    /// instead of parking the components this request won't touch cold.
    #[arg(long, default_value_t = false)]
    no_load_profile: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Paginate over points, optionally filtered.
    Scroll(ScrollArgs),
    /// Nearest-neighbour search for a query vector, optionally filtered.
    Search(SearchArgs),
}

/// Filter and result options shared by every sub-command.
#[derive(ClapArgs, Debug)]
struct CommonReadArgs {
    /// Payload filter as JSON, in Qdrant's filter DSL (the `filter` field of a
    /// REST request). Accepts a literal JSON string, `@path` to read the JSON
    /// from a file, or `@-` to read it from stdin. Mutually exclusive with the
    /// `--filter-key`/`--filter-value` shortcut.
    #[arg(long, conflicts_with_all = ["filter_key", "filter_value"])]
    filter: Option<String>,

    /// Shortcut: payload field to filter on (e.g. `city`). Builds a single
    /// "field equals value" condition together with `--filter-value`.
    #[arg(long, requires = "filter_value")]
    filter_key: Option<String>,

    /// Shortcut: value the `--filter-key` field must equal. Parsed as an integer
    /// or boolean when it looks like one, otherwise as a string.
    #[arg(long, requires = "filter_key")]
    filter_value: Option<String>,

    /// Maximum number of points to return.
    #[arg(long, default_value_t = 10)]
    limit: usize,

    /// Include vectors in the output.
    #[arg(long, default_value_t = false)]
    with_vectors: bool,
}

impl CommonReadArgs {
    /// Resolve the effective filter from either `--filter` (JSON) or the
    /// `--filter-key`/`--filter-value` shortcut. Returns `None` when neither is
    /// given (operate over all points). `clap` already guarantees the two
    /// sources are mutually exclusive and that key/value come as a pair.
    fn resolve_filter(&self) -> Result<Option<Filter>> {
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
struct ScrollArgs {
    #[command(flatten)]
    common: CommonReadArgs,

    /// Start ID to read points from (resume a previous page). Accepts an integer
    /// id or a UUID string.
    #[arg(long)]
    offset: Option<String>,

    /// Order the records by this payload field instead of by id.
    #[arg(long)]
    order_by: Option<String>,
}

#[derive(ClapArgs, Debug)]
struct SearchArgs {
    #[command(flatten)]
    common: CommonReadArgs,

    /// Query vector. Accepts a JSON array (`[0.1, 0.2, ...]`), a comma-separated
    /// list (`0.1,0.2,...`), or `@path`/`@-` to read either form from a file or
    /// stdin.
    #[arg(long)]
    vector: String,

    /// Name of the vector to search (for collections with named vectors). Omit
    /// to use the default/unnamed vector.
    #[arg(long)]
    using: Option<String>,

    /// Number of results to skip before collecting `--limit` results.
    #[arg(long, default_value_t = 0)]
    offset: usize,

    /// Only return results scoring at least this value.
    #[arg(long)]
    score_threshold: Option<f32>,

    /// HNSW `ef` search parameter (size of the beam). Larger is more accurate
    /// and slower.
    #[arg(long)]
    hnsw_ef: Option<usize>,

    /// Search exhaustively without the HNSW approximation (slow, exact).
    #[arg(long, default_value_t = false)]
    exact: bool,
}

/// Read a curl `--data`-style argument: a literal value, `@path` to read from a
/// file, or `@-` to read from stdin.
fn read_data_arg(raw: &str) -> Result<String> {
    match raw.strip_prefix('@') {
        Some("-") => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .context("failed to read from stdin")?;
            Ok(buf)
        }
        Some(path) => {
            fs_err::read_to_string(path).with_context(|| format!("failed to read file {path:?}"))
        }
        None => Ok(raw.to_string()),
    }
}

/// Build a single "field equals value" filter from the `--filter-key`/
/// `--filter-value` shortcut. Returns `None` when neither is set. The value is
/// parsed as an integer or boolean when it looks like one, otherwise as a string.
fn build_kv_filter(key: Option<&str>, value: Option<&str>) -> Result<Option<Filter>> {
    let (Some(key), Some(value)) = (key, value) else {
        return Ok(None);
    };

    let path: JsonPath = key
        .parse()
        .map_err(|()| anyhow!("invalid --filter-key path: {key:?}"))?;

    let value = if let Ok(int) = value.parse::<i64>() {
        ValueVariants::Integer(int)
    } else if let Ok(flag) = value.parse::<bool>() {
        ValueVariants::Bool(flag)
    } else {
        ValueVariants::String(value.to_string())
    };

    let condition = FieldCondition::new_match(path, Match::from(value));
    Ok(Some(Filter::new_must(Condition::Field(condition))))
}

/// Parse a point id from the command line: a bare integer id or a UUID string.
fn parse_point_id(raw: &str) -> Result<PointId> {
    // `PointId` deserializes a JSON number into a numeric id; a bare UUID is not
    // valid JSON, so quote it and retry as a JSON string.
    if let Ok(id) = serde_json::from_str::<PointId>(raw) {
        return Ok(id);
    }
    let quoted = serde_json::to_string(raw).expect("string is always serializable");
    serde_json::from_str::<PointId>(&quoted)
        .with_context(|| format!("invalid point id (not an integer or UUID): {raw:?}"))
}

/// Parse a query vector from a JSON array or a comma-separated list of floats.
fn parse_vector(raw: &str) -> Result<Vec<f32>> {
    let data = read_data_arg(raw)?;
    let trimmed = data.trim();
    if trimmed.starts_with('[') {
        serde_json::from_str(trimmed)
            .with_context(|| format!("failed to parse --vector as a JSON float array: {trimmed}"))
    } else {
        trimmed
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<f32>()
                    .with_context(|| format!("invalid float in --vector: {s:?}"))
            })
            .collect()
    }
}

/// The bucket name, required by the object-storage backends (`uio-grpc` has none).
fn require_bucket(conn: &ConnectionArgs) -> Result<String> {
    conn.bucket
        .clone()
        .ok_or_else(|| anyhow!("--bucket is required for the {:?} backend", conn.backend))
}

fn build_aws_config(conn: &ConnectionArgs) -> Result<AwsConfig> {
    let credentials = match (&conn.access_key, &conn.secret_key) {
        (Some(access_key_id), Some(secret_access_key)) => AwsCredentials::Static {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: conn.session_token.clone(),
        },
        (None, None) => AwsCredentials::Default,
        _ => {
            return Err(anyhow!(
                "--access-key and --secret-key must be provided together"
            ));
        }
    };

    Ok(AwsConfig {
        bucket: require_bucket(conn)?,
        region: conn.region.clone(),
        endpoint: conn.endpoint.clone(),
        s3_express: conn.s3_express,
        credentials,
    })
}

fn build_gcs_config(conn: &ConnectionArgs) -> Result<GcsConfig> {
    let credentials = if let Some(key) = &conn.gcs_service_account_key {
        GcsCredentials::ServiceAccountKey(key.clone())
    } else if let Some(path) = &conn.gcs_service_account_path {
        GcsCredentials::ServiceAccountPath(path.clone())
    } else {
        GcsCredentials::Default
    };

    Ok(GcsConfig {
        bucket: require_bucket(conn)?,
        credentials,
    })
}

fn build_uio_config(conn: &ConnectionArgs) -> Result<UioGrpcConfig> {
    Ok(UioGrpcConfig {
        endpoint: conn.endpoint.clone().ok_or_else(|| {
            anyhow!(
                "--endpoint is required for the uio-grpc backend \
                 (the peer's public gRPC URL, e.g. http://localhost:6334)"
            )
        })?,
        collection: conn
            .collection
            .clone()
            .ok_or_else(|| anyhow!("--collection is required for the uio-grpc backend"))?,
        shard_id: conn.shard_id,
        api_key: conn.api_key.clone(),
    })
}

/// Build the disk-cache-backed filesystem used to read segment data.
///
/// Remote objects under `remote_prefix` are mirrored into `cache_dir`: each
/// block is fetched from object storage the first time it is read and served
/// from the local mirror afterwards. `cache_dir` must exist locally, so it is
/// created first.
fn build_cached_fs<A>(
    remote_config: A::Config,
    remote_prefix: &Path,
    cache_dir: &Path,
) -> Result<DiskCacheFs<BlobFile<A>>>
where
    A: AsyncRead + Clone,
{
    fs_err::create_dir_all(cache_dir)
        .with_context(|| format!("failed to create cache dir {}", cache_dir.display()))?;

    let config = DiskCacheConfig::new(remote_prefix.to_path_buf(), cache_dir.to_path_buf())
        .context("failed to build disk cache config")?;

    DiskCacheFs::<BlobFile<A>>::from_context(DiskCacheFsContext {
        config: Arc::new(config),
        remote: remote_config,
    })
    .context("failed to build disk-cache filesystem")
}

/// Render a payload + (optional) vector as a single JSON line.
///
/// `VectorStructInternal` is not `Serialize`, so the vector is rendered via
/// `Debug` instead.
fn record_json(
    id: serde_json::Value,
    payload: serde_json::Value,
    vector: Option<impl std::fmt::Debug>,
    extra: serde_json::Value,
) -> serde_json::Value {
    let mut json = serde_json::json!({
        "id": id,
        "payload": payload,
        "vector": vector.map(|v| format!("{v:?}")),
    });
    if let (Some(obj), serde_json::Value::Object(extra)) = (json.as_object_mut(), extra) {
        obj.extend(extra);
    }
    json
}

/// One result row in a comparable form: the point id (as its JSON encoding,
/// used as the diff key) plus the rendered JSON line.
type Row = (String, serde_json::Value);

/// A read request parsed once from the CLI args, so every live-reload
/// iteration re-runs exactly the same request.
enum PreparedRequest {
    Scroll(ScrollRequest),
    Search(SearchRequest),
}

impl PreparedRequest {
    fn build(command: &Command) -> Result<Self> {
        match command {
            Command::Scroll(args) => {
                let filter = args.common.resolve_filter()?;
                match &filter {
                    Some(filter) => {
                        log::info!("scrolling with filter: {}", serde_json::to_string(filter)?)
                    }
                    None => log::info!("scrolling with no filter (all points)"),
                }

                let offset = args.offset.as_deref().map(parse_point_id).transpose()?;
                let order_by = args
                    .order_by
                    .as_deref()
                    .map(|key| {
                        key.parse::<JsonPath>()
                            .map(OrderByInterface::Key)
                            .map_err(|()| anyhow!("invalid --order-by path: {key:?}"))
                    })
                    .transpose()?;

                Ok(Self::Scroll(ScrollRequest {
                    offset,
                    limit: Some(args.common.limit),
                    filter,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: args.common.with_vectors.into(),
                    order_by,
                }))
            }
            Command::Search(args) => {
                let filter = args.common.resolve_filter()?;
                match &filter {
                    Some(filter) => {
                        log::info!("searching with filter: {}", serde_json::to_string(filter)?)
                    }
                    None => log::info!("searching with no filter"),
                }

                let vector = parse_vector(&args.vector)?;
                log::info!("query vector has {} dimension(s)", vector.len());

                let query = QueryEnum::Nearest(NamedQuery {
                    query: VectorInternal::Dense(vector),
                    using: args.using.clone(),
                });

                let params = (args.hnsw_ef.is_some() || args.exact).then(|| SearchParams {
                    hnsw_ef: args.hnsw_ef,
                    exact: args.exact,
                    ..Default::default()
                });

                Ok(Self::Search(SearchRequest {
                    query,
                    filter,
                    params,
                    limit: args.common.limit,
                    offset: args.offset,
                    with_payload: Some(WithPayloadInterface::Bool(true)),
                    with_vector: Some(args.common.with_vectors.into()),
                    score_threshold: args.score_threshold,
                }))
            }
        }
    }

    /// The request's [`LoadProfile`], deciding which segment components the
    /// shard open warms. Derived once, before the open.
    fn load_profile(&self) -> LoadProfile {
        match self {
            Self::Scroll(request) => request.load_profile(),
            Self::Search(request) => request.load_profile(),
        }
    }

    /// Run the request against the shard's current state. Returns the result
    /// rows and, for scroll, the next-page offset of this run.
    fn run<S: EdgeShardRead>(&self, shard: &S) -> Result<(Vec<Row>, Option<PointId>)> {
        match self {
            Self::Scroll(request) => {
                let (records, next_offset) = shard
                    .scroll(request.clone())
                    .context("scroll request failed")?;
                let rows = records.iter().map(record_row).collect::<Result<_>>()?;
                Ok((rows, next_offset))
            }
            Self::Search(request) => {
                let points = shard
                    .search(request.clone())
                    .context("search request failed")?;
                let rows = points.iter().map(scored_point_row).collect::<Result<_>>()?;
                Ok((rows, None))
            }
        }
    }

    /// Print a full result set (the first answer; later runs print diffs).
    fn print_full(&self, rows: &[Row], next_offset: Option<&PointId>) -> Result<()> {
        match self {
            Self::Scroll(_) => println!("scroll returned {} record(s)", rows.len()),
            Self::Search(_) => println!("search returned {} result(s)", rows.len()),
        }
        for (_, row) in rows {
            println!("{}", serde_json::to_string(row)?);
        }
        if let Self::Scroll(_) = self {
            match next_offset {
                Some(offset) => println!("next_page_offset: {}", serde_json::to_string(offset)?),
                None => println!("next_page_offset: <none>"),
            }
        }
        Ok(())
    }
}

fn record_row(record: &Record) -> Result<Row> {
    let json = record_json(
        serde_json::to_value(record.id)?,
        serde_json::to_value(&record.payload)?,
        record.vector.as_ref(),
        serde_json::Value::Null,
    );
    Ok((serde_json::to_string(&record.id)?, json))
}

fn scored_point_row(point: &ScoredPoint) -> Result<Row> {
    let json = record_json(
        serde_json::to_value(point.id)?,
        serde_json::to_value(&point.payload)?,
        point.vector.as_ref(),
        serde_json::json!({ "score": point.score, "version": point.version }),
    );
    Ok((serde_json::to_string(&point.id)?, json))
}

/// Print the difference between the previous and the current run of the same
/// request: `+` rows whose id appeared, `-` rows whose id disappeared, `~` rows
/// whose rendered content changed for the same id (old -> new). A pure
/// reordering of unchanged rows prints nothing.
fn print_diff(previous: &[Row], current: &[Row]) -> Result<()> {
    let previous_by_id: HashMap<&str, &serde_json::Value> = previous
        .iter()
        .map(|(id, row)| (id.as_str(), row))
        .collect();
    let current_by_id: HashMap<&str, &serde_json::Value> =
        current.iter().map(|(id, row)| (id.as_str(), row)).collect();

    let mut added = 0usize;
    let mut removed = 0usize;
    let mut changed = 0usize;
    for (id, row) in current {
        match previous_by_id.get(id.as_str()) {
            None => {
                added += 1;
                println!("+ {}", serde_json::to_string(row)?);
            }
            Some(old) if **old != *row => {
                changed += 1;
                println!(
                    "~ {} -> {}",
                    serde_json::to_string(old)?,
                    serde_json::to_string(row)?
                );
            }
            Some(_) => {}
        }
    }
    for (id, row) in previous {
        if !current_by_id.contains_key(id.as_str()) {
            removed += 1;
            println!("- {}", serde_json::to_string(row)?);
        }
    }

    if added == 0 && removed == 0 && changed == 0 {
        println!("no changes");
    } else {
        println!("{added} added, {removed} removed, {changed} changed");
    }
    Ok(())
}

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
    log::info!("caching segment reads under {}", cache_dir.display());

    // Build the request before the open: the shard is opened for exactly this request, so the
    // request's load profile decides which segment components are warmed at all (unless disabled
    // with `--no-load-profile`).
    let request = PreparedRequest::build(&cli.command)?;
    let load_profile = (!cli.connection.no_load_profile).then(|| request.load_profile());

    // No edge_config.json: `ReadOnlyEdgeShard` derives its config from the segments and discovers
    // them via the manifest. `prefix` is passed only as the shard's (logical) path label. A
    // caller-provided `--search-threads` overrides the derived search-pool size (segments never
    // carry `max_search_threads`, so this is the only way to set it here).
    let config = cli
        .connection
        .search_threads
        .map(|n| EdgeConfig::builder().max_search_threads(n).build());
    let shard =
        ReadOnlyEdgeShard::<DiskCache<BlobFile<A>>>::open(cached_fs, prefix, config, load_profile)
            .context("failed to open read-only edge shard over object storage")?;
    log::info!("opened shard with {} segment(s)", shard.segments_count());

    let (rows, next_offset) = request.run(&shard)?;
    request.print_full(&rows, next_offset.as_ref())?;

    let Some(trigger) = ReloadTrigger::from_cli(cli) else {
        return Ok(());
    };

    // Live-reload loop: on every trigger (timer tick or Enter), refresh the
    // shard from the backend, re-run the same request, and print the diff
    // against the previous run. Runs until interrupted (or stdin closes, in
    // key mode).
    let mut previous = rows;
    for iteration in 1u64.. {
        if !trigger.wait()? {
            log::info!("stdin closed; stopping live-reload");
            return Ok(());
        }

        if let Err(err) = shard.refresh() {
            // The shard keeps serving its previous state; retry on the next trigger.
            log::error!("live-reload refresh failed (will retry on next reload): {err}");
            continue;
        }
        log::info!(
            "live-reload #{iteration}: refreshed shard, {} segment(s)",
            shard.segments_count(),
        );

        let (rows, _) = request.run(&shard)?;
        println!("--- refresh #{iteration}: diff vs previous results ---");
        print_diff(&previous, &rows)?;
        previous = rows;
    }
    unreachable!("live-reload loop only ends by interruption");
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let cli = Cli::parse();
    let conn = &cli.connection;
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
