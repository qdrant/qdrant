//! Experimental binary that opens a [`ReadOnlyEdgeShard`] directly over object
//! storage (AWS S3 / S3-compatible, or Google Cloud Storage) and runs a single
//! scroll request, optionally filtered by a payload field (`--filter-key` /
//! `--filter-value`).
//!
//! The shard data is read through the `io_bridge_object_store` blob backend,
//! wrapped in a [`DiskCache`] so each remote block is fetched once and then
//! served from a local mirror directory. Segments are discovered from the
//! leader's segment manifest, and the shard config is derived from the segments
//! themselves, so no `edge_config.json` is required.
//!
//! Example (RustFS/MinIO running locally):
//!
//! ```sh
//! cargo run -p edge-s3-scroll -- \
//!     --backend  aws \
//!     --endpoint http://localhost:9000 \
//!     --bucket   test-bucket \
//!     --region   us-east-1 \
//!     --access-key  rustfsadmin \
//!     --secret-key  rustfsadmin \
//!     --prefix   collection/0 \
//!     --filter-key   city \
//!     --filter-value London \
//!     --limit    20
//! ```
//!
//! Example (Google Cloud Storage with a service-account key):
//!
//! ```sh
//! cargo run -p edge-s3-scroll -- \
//!     --backend  gcs \
//!     --bucket   my-bucket \
//!     --gcs-service-account-path /path/to/key.json \
//!     --prefix   collection/0
//! ```
//!
//! `--prefix` is the key prefix inside the bucket that points at the edge-shard
//! root — the directory that contains `edge_config.json` and `segments/`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use common::universal_io::{
    DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
};
use edge::{
    Condition, EdgeShardRead, FieldCondition, Filter, JsonPath, Match, ReadOnlyEdgeShard, Record,
    ScrollRequest, ValueVariants,
};
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
use io_bridge_object_store::{AsyncRead, BlobFile};
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
    about = "Open a ReadOnlyEdgeShard over S3/GCS object storage and run a scroll, optionally filtered by a payload field"
)]
struct Args {
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

    /// Payload field to filter on (e.g. `city`). When omitted, no filter is
    /// applied and the scroll returns all points.
    #[arg(long, requires = "filter_value")]
    filter_key: Option<String>,

    /// Value the `--filter-key` field must equal. Parsed as an integer or
    /// boolean when it looks like one, otherwise as a string.
    #[arg(long, requires = "filter_key")]
    filter_value: Option<String>,

    /// Local directory for the segment disk cache. Remote blocks are fetched
    /// from object storage once and mirrored here; later reads hit this
    /// directory instead. Defaults to a stable subdirectory of the system temp
    /// dir, so the cache persists across runs.
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Maximum number of points to return.
    #[arg(long, default_value_t = 10)]
    limit: usize,

    /// Include vectors in the output.
    #[arg(long, default_value_t = false)]
    with_vectors: bool,
}

/// Build the scroll filter from the `--filter-key`/`--filter-value` pair.
///
/// Returns `None` when neither is set (scroll everything). `clap`'s `requires`
/// already enforces that the two are supplied together. The value is parsed as
/// an integer or boolean when it looks like one, otherwise treated as a string.
fn build_filter(key: Option<&str>, value: Option<&str>) -> Result<Option<Filter>> {
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

fn build_aws_config(args: &Args) -> Result<AwsConfig> {
    let credentials = match (&args.access_key, &args.secret_key) {
        (Some(access_key_id), Some(secret_access_key)) => AwsCredentials::Static {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: args.session_token.clone(),
        },
        (None, None) => AwsCredentials::Default,
        _ => {
            return Err(anyhow!(
                "--access-key and --secret-key must be provided together"
            ));
        }
    };

    Ok(AwsConfig {
        bucket: args.bucket.clone(),
        region: args.region.clone(),
        endpoint: args.endpoint.clone(),
        credentials,
    })
}

fn build_gcs_config(args: &Args) -> GcsConfig {
    let credentials = if let Some(key) = &args.gcs_service_account_key {
        GcsCredentials::ServiceAccountKey(key.clone())
    } else if let Some(path) = &args.gcs_service_account_path {
        GcsCredentials::ServiceAccountPath(path.clone())
    } else {
        GcsCredentials::Default
    };

    GcsConfig {
        bucket: args.bucket.clone(),
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

fn print_records(records: &[Record], next_offset: Option<edge::PointId>) -> Result<()> {
    println!("scroll returned {} record(s)", records.len());
    for record in records {
        // `VectorStructInternal` is not `Serialize`, so render it via `Debug`.
        let json = serde_json::json!({
            "id": record.id,
            "payload": record.payload,
            "vector": record.vector.as_ref().map(|v| format!("{v:?}")),
        });
        println!("{}", serde_json::to_string(&json)?);
    }
    if let Some(offset) = next_offset {
        println!("next_page_offset: {}", serde_json::to_string(&offset)?);
    } else {
        println!("next_page_offset: <none>");
    }
    Ok(())
}

/// Open the read-only shard over backend `A` and run the scroll.
///
/// Generic over the object-storage backend handle `A` (e.g. `Arc<AmazonS3>` or
/// `Arc<GoogleCloudStorage>`) so the whole read path stays monomorphic per
/// backend. `remote_config` is that backend's connection config.
fn run<A>(args: &Args, prefix: &Path, cache_dir: &Path, remote_config: A::Config) -> Result<()>
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

    let filter = build_filter(args.filter_key.as_deref(), args.filter_value.as_deref())?;
    match &filter {
        Some(filter) => log::info!("scrolling with filter: {}", serde_json::to_string(filter)?),
        None => log::info!("scrolling with no filter (all points)"),
    }

    let request = ScrollRequest {
        offset: None,
        limit: Some(args.limit),
        filter,
        with_payload: None,
        with_vector: args.with_vectors.into(),
        order_by: None,
    };

    let (records, next_offset) = shard.scroll(request).context("scroll request failed")?;
    print_records(&records, next_offset)
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let prefix = PathBuf::from(&args.prefix);
    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("edge-s3-scroll-cache"));

    log::info!(
        "opening read-only edge shard backend={:?} bucket={:?} prefix={:?}",
        args.backend,
        args.bucket,
        args.prefix,
    );

    match args.backend {
        Backend::Aws => run::<Arc<AmazonS3>>(&args, &prefix, &cache_dir, build_aws_config(&args)?),
        Backend::Gcs => {
            run::<Arc<GoogleCloudStorage>>(&args, &prefix, &cache_dir, build_gcs_config(&args))
        }
    }
}
