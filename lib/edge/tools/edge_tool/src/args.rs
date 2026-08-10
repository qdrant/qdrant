//! CLI argument definitions.

use std::path::PathBuf;

use clap::{ArgAction, Args as ClapArgs, Parser, Subcommand, ValueEnum};
use edge::Distance;

use crate::quantization::QuantizationPreset;

#[derive(Parser, Debug)]
#[command(
    name = "edge-tool",
    about = "Create, seed, optimize, and upload minimal local Qdrant edge collections \
             (for benchmarking and manual testing of the edge write/read/optimize paths)"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a new minimal edge collection on local disk.
    Create(CreateArgs),
    /// Upsert randomly generated points into an existing local collection.
    Upsert(UpsertArgs),
    /// Run the shard optimizers (merge/indexing/vacuum) until idle.
    Optimize(OptimizeArgs),
    /// Upload a local collection directory to S3/GCS object storage.
    Upload(UploadArgs),
}

#[derive(ClapArgs, Debug)]
pub struct CreateArgs {
    /// Directory to create the collection in. Must not already contain segment data.
    pub path: PathBuf,

    /// Add a dense vector. A single bare `--dense SIZE` creates one unnamed vector;
    /// with more than one dense vector, every one must be named as `--dense NAME:SIZE`.
    /// Repeatable.
    #[arg(long = "dense", value_name = "SIZE|NAME:SIZE")]
    pub dense: Vec<String>,

    /// Distance metric applied to every dense vector.
    #[arg(long, value_enum, default_value_t = DistanceArg::Cosine)]
    pub distance: DistanceArg,

    /// Add a sparse vector. Bare `--sparse` creates one vector named `sparse`;
    /// `--sparse=NAME` names it (the `=` is required — `--sparse NAME` is ambiguous
    /// with the trailing PATH positional and is rejected). Repeatable for multiple
    /// sparse vectors.
    #[arg(
        long = "sparse",
        value_name = "NAME",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "",
        action = ArgAction::Append
    )]
    pub sparse: Vec<String>,

    /// Quantize every dense vector with this preset.
    #[arg(long, value_enum)]
    pub quantization: Option<QuantizationPreset>,

    /// Create a payload index: `NAME:TYPE`, where TYPE is one of keyword, integer,
    /// float, text, geo. Comma-separated and/or repeatable.
    #[arg(
        long = "payload-index",
        value_name = "NAME:TYPE",
        value_delimiter = ','
    )]
    pub payload_index: Vec<String>,

    /// Indexing threshold in KB: segments larger than this get an HNSW index built
    /// for them by `optimize`. Omit to use the built-in default.
    #[arg(long)]
    pub indexing_threshold_kb: Option<usize>,

    /// Store payload on disk (mmap) instead of RAM.
    #[arg(long, default_value_t = false)]
    pub on_disk_payload: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum DistanceArg {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

impl From<DistanceArg> for Distance {
    fn from(value: DistanceArg) -> Self {
        match value {
            DistanceArg::Cosine => Distance::Cosine,
            DistanceArg::Euclid => Distance::Euclid,
            DistanceArg::Dot => Distance::Dot,
            DistanceArg::Manhattan => Distance::Manhattan,
        }
    }
}

#[derive(ClapArgs, Debug)]
pub struct UpsertArgs {
    /// Path to an existing collection directory.
    pub path: PathBuf,

    /// Number of random points to upsert.
    #[arg(short = 'n', long, default_value_t = 100)]
    pub num: usize,

    /// First point id to use. Defaults to the collection's current (approximate)
    /// point count, so repeated `upsert` calls append rather than overwrite.
    #[arg(long)]
    pub start_id: Option<u64>,

    /// RNG seed for the generated points.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}

#[derive(ClapArgs, Debug)]
pub struct OptimizeArgs {
    /// Path to an existing collection directory.
    pub path: PathBuf,
}

#[derive(ClapArgs, Debug)]
pub struct UploadArgs {
    /// Local collection directory to upload.
    pub source: PathBuf,

    /// Destination key prefix inside the bucket (e.g. `my_collection/0`).
    pub destination: String,

    /// Upload to AWS S3 or an S3-compatible store (MinIO, RustFS, ...). Default backend.
    #[arg(long, conflicts_with = "gcs")]
    pub aws: bool,

    /// Upload to Google Cloud Storage.
    #[arg(long)]
    pub gcs: bool,

    /// [AWS/GCS] Bucket name (without any scheme prefix).
    #[arg(long, env = "BLOB_BUCKET")]
    pub bucket: Option<String>,

    /// [AWS] Custom S3 endpoint URL (MinIO / RustFS / LocalStack; omit for real AWS).
    #[arg(long, env = "S3_ENDPOINT")]
    pub endpoint: Option<String>,

    /// [AWS] Region (e.g. `us-east-1`). Required for real AWS; optional otherwise.
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

    /// Number of files to upload concurrently.
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
}
