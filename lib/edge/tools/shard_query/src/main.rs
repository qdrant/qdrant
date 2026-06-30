//! Experimental binary that opens a [`ReadOnlyEdgeShard`] directly over object
//! storage (AWS S3 / S3-compatible, or Google Cloud Storage) and runs a single
//! read request against it.
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

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use common::universal_io::{
    DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
};
use edge::{
    Condition, EdgeShardRead, FieldCondition, Filter, JsonPath, Match, NamedQuery,
    OrderByInterface, PointId, QueryEnum, ReadOnlyEdgeShard, Record, ScoredPoint, ScrollRequest,
    SearchParams, SearchRequest, ValueVariants, VectorInternal, WithPayloadInterface,
};
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
use io_bridge_object_store::{AsyncRead, BlobFile, ObjectStoreSource};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;

/// Object-storage backend to read from.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Backend {
    /// AWS S3 or an S3-compatible store (MinIO, RustFS, LocalStack, ...).
    Aws,
    /// Google Cloud Storage.
    Gcs,
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

    /// Which read request to run against the shard.
    #[command(subcommand)]
    command: Command,
}

/// How to reach the object store and where the shard lives inside it. Shared by
/// every sub-command.
#[derive(ClapArgs, Debug)]
struct ConnectionArgs {
    /// Which object-storage backend to use.
    #[arg(long, value_enum, default_value = "aws")]
    backend: Backend,

    /// Bucket name (without any scheme prefix). Used by both backends.
    #[arg(long, env = "BLOB_BUCKET")]
    bucket: String,

    /// [AWS] Custom S3 endpoint URL (set for MinIO / RustFS / LocalStack). Omit for real AWS.
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

    /// [GCS] Path to a service-account JSON key file. Takes precedence over
    /// `--gcs-service-account-key`; if neither is set, application default
    /// credentials (ADC) are used.
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_PATH")]
    gcs_service_account_path: Option<String>,

    /// [GCS] Inline service-account JSON key contents (instead of a path).
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_KEY")]
    gcs_service_account_key: Option<String>,

    /// Key prefix inside the bucket pointing at the edge-shard root (contains
    /// `edge_config.json` and `segments/`). Empty means the bucket root.
    #[arg(long, default_value = "")]
    prefix: String,

    /// Local directory for the segment disk cache. Remote blocks are fetched
    /// from object storage once and mirrored here; later reads hit this
    /// directory instead. Defaults to a stable subdirectory of the system temp
    /// dir, so the cache persists across runs.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
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
        bucket: conn.bucket.clone(),
        region: conn.region.clone(),
        endpoint: conn.endpoint.clone(),
        credentials,
    })
}

fn build_gcs_config(conn: &ConnectionArgs) -> GcsConfig {
    let credentials = if let Some(key) = &conn.gcs_service_account_key {
        GcsCredentials::ServiceAccountKey(key.clone())
    } else if let Some(path) = &conn.gcs_service_account_path {
        GcsCredentials::ServiceAccountPath(path.clone())
    } else {
        GcsCredentials::Default
    };

    GcsConfig {
        bucket: conn.bucket.clone(),
        credentials,
    }
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

fn print_records(records: &[Record], next_offset: Option<PointId>) -> Result<()> {
    println!("scroll returned {} record(s)", records.len());
    for record in records {
        let json = record_json(
            serde_json::to_value(record.id)?,
            serde_json::to_value(&record.payload)?,
            record.vector.as_ref(),
            serde_json::Value::Null,
        );
        println!("{}", serde_json::to_string(&json)?);
    }
    if let Some(offset) = next_offset {
        println!("next_page_offset: {}", serde_json::to_string(&offset)?);
    } else {
        println!("next_page_offset: <none>");
    }
    Ok(())
}

fn print_scored_points(points: &[ScoredPoint]) -> Result<()> {
    println!("search returned {} result(s)", points.len());
    for point in points {
        let json = record_json(
            serde_json::to_value(point.id)?,
            serde_json::to_value(&point.payload)?,
            point.vector.as_ref(),
            serde_json::json!({ "score": point.score, "version": point.version }),
        );
        println!("{}", serde_json::to_string(&json)?);
    }
    Ok(())
}

fn run_scroll<S: EdgeShardRead>(shard: &S, args: &ScrollArgs) -> Result<()> {
    let filter = args.common.resolve_filter()?;
    match &filter {
        Some(filter) => log::info!("scrolling with filter: {}", serde_json::to_string(filter)?),
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

    let request = ScrollRequest {
        offset,
        limit: Some(args.common.limit),
        filter,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: args.common.with_vectors.into(),
        order_by,
    };

    let (records, next_offset) = shard.scroll(request).context("scroll request failed")?;
    print_records(&records, next_offset)
}

fn run_search<S: EdgeShardRead>(shard: &S, args: &SearchArgs) -> Result<()> {
    let filter = args.common.resolve_filter()?;
    match &filter {
        Some(filter) => log::info!("searching with filter: {}", serde_json::to_string(filter)?),
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

    let request = SearchRequest {
        query,
        filter,
        params,
        limit: args.common.limit,
        offset: args.offset,
        with_payload: Some(WithPayloadInterface::Bool(true)),
        with_vector: Some(args.common.with_vectors.into()),
        score_threshold: args.score_threshold,
    };

    let points = shard.search(request).context("search request failed")?;
    print_scored_points(&points)
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

    // No edge_config.json: `ReadOnlyEdgeShard` derives its config from the segments and discovers
    // them via the manifest. `prefix` is passed only as the shard's (logical) path label.
    let shard = ReadOnlyEdgeShard::<DiskCache<BlobFile<A>>>::open(cached_fs, prefix)
        .context("failed to open read-only edge shard over object storage")?;
    log::info!("opened shard with {} segment(s)", shard.segments_count());

    match &cli.command {
        Command::Scroll(args) => run_scroll(&shard, args),
        Command::Search(args) => run_search(&shard, args),
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let conn = &cli.connection;
    let prefix = PathBuf::from(&conn.prefix);
    let cache_dir = conn
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("edge-shard-query-cache"));

    log::info!(
        "opening read-only edge shard backend={:?} bucket={:?} prefix={:?}",
        conn.backend,
        conn.bucket,
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
            build_gcs_config(conn),
        ),
    }
}
