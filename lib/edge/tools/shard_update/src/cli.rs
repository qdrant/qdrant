//! Command-line definitions: connection options and the batch parameters.

use std::path::PathBuf;

use clap::{Args as ClapArgs, Parser, ValueEnum};

/// Storage backend to read the shard from.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Backend {
    /// A local shard directory (`--path`), read via memory-mapped files and
    /// discovered by scanning `segments/`.
    Local,
    /// AWS S3 or an S3-compatible store (MinIO, RustFS, GCS interop, ...).
    Aws,
    /// Google Cloud Storage.
    Gcs,
}

#[derive(Parser, Debug)]
#[command(
    about = "Open an UpdateOnlyEdgeShard over a local directory or S3/GCS object storage and \
             run a batch of random upserts against it — a dry run by default, written for real \
             with --apply"
)]
pub struct Cli {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Apply the batch for real: append to the write target and tombstone
    /// older copies, durable on the backend once the run reports Ok. Without
    /// this flag the batch is only resolved and logged; nothing is written.
    #[arg(long)]
    pub apply: bool,

    /// After a batch is applied, prompt on stdin for the next round's ids
    /// (comma-separated; an empty line or EOF quits) and apply them through
    /// the same writer — no shard re-open — with --op-num incremented by one
    /// per round.
    #[arg(long, requires = "apply")]
    pub interactive: bool,

    /// Point ids to overwrite with random points: comma-separated integers or
    /// UUIDs. Ids no segment holds are created rather than overwritten.
    #[arg(long, value_delimiter = ',', required = true)]
    pub ids: Vec<String>,

    /// Operation number recorded as the new points' version. A point whose
    /// stored version is at or beyond this is reported as skipped — the
    /// writer's replay semantics — so pick a value above the versions the log
    /// reports to see the points overwritten.
    #[arg(long, default_value_t = 1)]
    pub op_num: u64,

    /// RNG seed for the generated points, so a run is reproducible.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}

/// How to reach the shard: a local path, or an object-storage location.
#[derive(ClapArgs, Debug)]
pub struct ConnectionArgs {
    /// Which backend to read the shard from.
    #[arg(long, value_enum, default_value = "local")]
    pub backend: Backend,

    /// [local] Path to the shard root directory (the one containing
    /// `segments/`).
    #[arg(long)]
    pub path: Option<PathBuf>,

    /// [AWS/GCS] Bucket name (without any scheme prefix).
    #[arg(long, env = "BLOB_BUCKET")]
    pub bucket: Option<String>,

    /// [AWS] Custom S3 endpoint URL (MinIO / RustFS / GCS interop; omit for real AWS).
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

    /// [AWS] The store honors native write-offset appends without being S3
    /// Express — MinIO AiStor, RustFS. Implied by --s3-express; leave off
    /// for plain S3, where appends go through server-side rewrites.
    #[arg(long, env = "S3_NATIVE_APPEND")]
    pub native_append: bool,

    /// [GCS] Path to a service-account JSON key file. Takes precedence over
    /// `--gcs-service-account-key`; if neither is set, application default
    /// credentials (ADC) are used.
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_PATH")]
    pub gcs_service_account_path: Option<String>,

    /// [GCS] Inline service-account JSON key contents (instead of a path).
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_KEY")]
    pub gcs_service_account_key: Option<String>,

    /// [AWS/GCS] Key prefix inside the bucket pointing at the edge-shard root
    /// (the directory containing `segments/` and the segment manifest). Empty
    /// means the bucket root.
    #[arg(long, default_value = "")]
    pub prefix: String,

    /// [AWS/GCS] Local directory for the segment disk cache. Remote blocks are
    /// fetched once and mirrored here; later reads hit this directory instead.
    /// Defaults to a stable subdirectory of the system temp dir.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,
}
