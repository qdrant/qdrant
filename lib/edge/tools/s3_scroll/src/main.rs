//! Experimental binary that opens a [`ReadOnlyEdgeShard`] directly over an S3
//! (or S3-compatible: MinIO, RustFS, LocalStack) bucket and runs a single
//! scroll request with a hard-coded filter.
//!
//! The shard data is read straight from object storage via the
//! `io_bridge_object_store` blob backend; only `edge_config.json` is fetched to
//! a local temp directory first, because [`ReadOnlyEdgeShard::open`] reads the
//! config from the local filesystem.
//!
//! Example (RustFS/MinIO running locally):
//!
//! ```sh
//! cargo run -p edge-s3-scroll -- \
//!     --endpoint http://localhost:9000 \
//!     --bucket   test-bucket \
//!     --region   us-east-1 \
//!     --access-key  rustfsadmin \
//!     --secret-key  rustfsadmin \
//!     --prefix   collection/0 \
//!     --limit    20
//! ```
//!
//! `--prefix` is the key prefix inside the bucket that points at the edge-shard
//! root — the directory that contains `edge_config.json` and `segments/`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use common::universal_io::{UniversalReadFileOps, read_json_via, read_whole_via};
use edge::{
    EdgeShardRead, Filter, OperationError, OperationResult, ReadOnlyEdgeShard, Record,
    ScrollRequest, SegmentEnumerator,
};
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::{BlobFile, BlobFs};
use object_store::aws::AmazonS3;
use shard::files::{SEGMENT_MANIFEST_FILE, SEGMENTS_PATH};
use shard::segment_manifest::{SegmentManifestState, SegmentsManifest};
use uuid::Uuid;

/// Hard-coded filter applied to the scroll request.
///
/// Edit this to experiment with different conditions. It is parsed into an
/// [`edge::Filter`] at startup; an invalid JSON filter aborts the run. Set it to
/// `"{}"` to match everything.
const HARD_CODED_FILTER: &str = r#"
{
    "must": [
        {
            "key": "city",
            "match": { "value": "London" }
        }
    ]
}
"#;

type S3File = BlobFile<Arc<AmazonS3>>;
type S3Fs = BlobFs<Arc<AmazonS3>>;

/// Config file name at the edge-shard root. Mirrors `edge`'s internal constant
/// (which is crate-private).
const EDGE_CONFIG_FILE: &str = "edge_config.json";

#[derive(Parser, Debug)]
#[command(
    about = "Open a ReadOnlyEdgeShard over an S3 bucket and run a scroll with a hard-coded filter"
)]
struct Args {
    /// Custom S3 endpoint URL (set for MinIO / RustFS / LocalStack). Omit for real AWS.
    #[arg(long, env = "S3_ENDPOINT")]
    endpoint: Option<String>,

    /// S3 bucket name (without the `s3://` prefix).
    #[arg(long, env = "S3_BUCKET")]
    bucket: String,

    /// AWS region (e.g. `us-east-1`). Required for real AWS; optional for S3-compatible endpoints.
    #[arg(long, env = "S3_REGION")]
    region: Option<String>,

    /// AWS access key id. If omitted, the AWS default credential chain is used.
    #[arg(long, env = "S3_ACCESS_KEY")]
    access_key: Option<String>,

    /// AWS secret access key. Required when `--access-key` is given.
    #[arg(long, env = "S3_SECRET_KEY")]
    secret_key: Option<String>,

    /// Optional session token for short-lived credentials.
    #[arg(long, env = "S3_SESSION_TOKEN")]
    session_token: Option<String>,

    /// Key prefix inside the bucket pointing at the edge-shard root (contains
    /// `edge_config.json` and `segments/`). Empty means the bucket root.
    #[arg(long, default_value = "")]
    prefix: String,

    /// Maximum number of points to return.
    #[arg(long, default_value_t = 10)]
    limit: usize,

    /// Include vectors in the output.
    #[arg(long, default_value_t = false)]
    with_vectors: bool,
}

/// [`SegmentEnumerator`] that reads the leader's segment manifest over the blob
/// backend (the local [`edge::ManifestSegmentEnumerator`] reads it from the
/// local filesystem, which is not where it lives for an S3 follower).
struct S3ManifestSegmentEnumerator {
    fs: S3Fs,
    segments_path: PathBuf,
}

impl SegmentEnumerator for S3ManifestSegmentEnumerator {
    fn list_segments(&self) -> OperationResult<HashMap<Uuid, PathBuf>> {
        let manifest_path = self.segments_path.join(SEGMENT_MANIFEST_FILE);
        let manifest: SegmentsManifest =
            read_json_via(&self.fs, &manifest_path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to read segment manifest {} over S3: {err}",
                    manifest_path.display(),
                ))
            })?;
        Ok(manifest
            .iter()
            .filter(|(_, state)| matches!(state, SegmentManifestState::Active))
            .map(|(uuid, _)| (*uuid, self.segments_path.join(uuid.to_string())))
            .collect())
    }
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

/// Fetch `edge_config.json` from the bucket into a local temp directory, because
/// [`ReadOnlyEdgeShard::open`] reads the config from the local filesystem.
/// Returns the temp directory holding the config (kept alive by the caller).
fn fetch_config_locally(fs: &S3Fs, prefix: &Path) -> Result<tempfile::TempDir> {
    let remote_config = prefix.join(EDGE_CONFIG_FILE);
    let bytes: Vec<u8> = read_whole_via(fs, &remote_config, |bytes| Ok(bytes.into_owned()))
        .with_context(|| format!("failed to read {} over S3", remote_config.display()))?;

    let temp_dir = tempfile::Builder::new()
        .prefix("edge-s3-scroll-")
        .tempdir()
        .context("failed to create temp dir for edge_config.json")?;
    let local_config = temp_dir.path().join(EDGE_CONFIG_FILE);
    fs_err::write(&local_config, &bytes)
        .with_context(|| format!("failed to write {}", local_config.display()))?;

    Ok(temp_dir)
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

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let aws_config = build_aws_config(&args)?;
    let prefix = PathBuf::from(&args.prefix);

    log::info!(
        "opening read-only edge shard over s3 bucket={:?} prefix={:?} endpoint={:?}",
        args.bucket,
        args.prefix,
        args.endpoint,
    );

    // Blob filesystem backed by the S3 bucket; drives async object-store reads
    // through the global bridge runtime.
    let fs = S3Fs::from_context(aws_config).context("failed to build S3 blob filesystem")?;

    // edge_config.json must exist locally for `open`; fetch it from the bucket.
    let config_dir = fetch_config_locally(&fs, &prefix)?;

    let enumerator = S3ManifestSegmentEnumerator {
        fs: fs.clone(),
        segments_path: prefix.join(SEGMENTS_PATH),
    };

    let shard = ReadOnlyEdgeShard::<S3File>::open(fs, config_dir.path(), enumerator)
        .context("failed to open read-only edge shard over S3")?;
    log::info!("opened shard with {} segment(s)", shard.segments_count());

    let filter: Filter =
        serde_json::from_str(HARD_CODED_FILTER).context("failed to parse hard-coded filter")?;
    log::info!("scrolling with filter: {}", serde_json::to_string(&filter)?);

    let request = ScrollRequest {
        offset: None,
        limit: Some(args.limit),
        filter: Some(filter),
        with_payload: None,
        with_vector: args.with_vectors.into(),
        order_by: None,
    };

    let (records, next_offset) = shard.scroll(request).context("scroll request failed")?;
    print_records(&records, next_offset)?;

    Ok(())
}
