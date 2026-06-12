//! Standalone tool to test the streaming tar unpack used by snapshot transfer.
//!
//! It downloads a snapshot tar archive from a URL and unpacks it *while it is being
//! streamed* — exactly the same way [`storage::content_manager::snapshots::download_tar::download_and_unpack_tar`]
//! does during shard snapshot transfer / recovery. By default the unpacked content is
//! written to a temporary directory that is deleted on exit, so the tool just verifies
//! that the streamed archive is well-formed and not corrupted. Pass `--out <DIR>` to
//! keep the extracted content on disk.
//!
//! Examples:
//! ```text
//! cargo run --features service_debug --bin snapshot_stream_check -- \
//!     http://localhost:6333/collections/my-col/shards/0/snapshot
//!
//! cargo run --features service_debug --bin snapshot_stream_check -- \
//!     --api-key "$QDRANT_API_KEY" --out ./extracted \
//!     https://my-host:6333/collections/my-col/shards/0/snapshot
//! ```

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, bail};
use clap::Parser;
use common::defaults::APP_USER_AGENT;
use reqwest::header::{HeaderMap, HeaderValue};
use storage::content_manager::snapshots::download_tar::download_and_unpack_tar;
use url::Url;

#[derive(Parser, Debug)]
#[command(
    about = "Stream-unpack a snapshot tar archive (as snapshot transfer does) to verify it is not corrupted"
)]
struct Args {
    /// URL of the snapshot tar archive to download and stream-unpack.
    url: Url,

    /// API key sent as the `api-key` header (same as snapshot transfer).
    #[arg(long, env = "QDRANT_API_KEY")]
    api_key: Option<String>,

    /// Directory to extract the archive into. If omitted, a temporary directory is used
    /// and deleted on exit — only the integrity of the stream is verified.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Do not compute the SHA-256 checksum of the streamed bytes.
    #[arg(long)]
    no_checksum: bool,

    /// Accept invalid/self-signed TLS certificates (insecure).
    #[arg(long, short = 'k')]
    insecure: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();

    let client = build_client(args.api_key.as_deref(), args.insecure)
        .context("failed to build HTTP client")?;

    // Either unpack into the user-provided directory, or into a throwaway temp dir.
    let temp_dir = match &args.out {
        Some(_) => None,
        None => Some(tempfile::tempdir().context("failed to create temporary directory")?),
    };
    let target_dir = match (&args.out, &temp_dir) {
        (Some(out), _) => out.clone(),
        (None, Some(temp)) => temp.path().to_path_buf(),
        (None, None) => unreachable!(),
    };
    fs_err::create_dir_all(&target_dir).context("failed to create target directory")?;

    println!("Streaming {} -> {}", args.url, target_dir.display());
    if temp_dir.is_some() {
        println!("(temporary directory, will be removed on exit)");
    }

    let started = Instant::now();
    let hash = download_and_unpack_tar(&client, &args.url, &target_dir, !args.no_checksum)
        .await
        .context("streaming tar unpack failed - archive may be corrupted")?;
    let elapsed = started.elapsed();

    let (files, bytes) = dir_stats(&target_dir).context("failed to inspect extracted content")?;

    println!("OK - archive streamed and unpacked successfully");
    println!("  files extracted: {files}");
    println!("  bytes on disk:   {bytes}");
    println!("  elapsed:         {elapsed:.2?}");
    if let Some(hash) = hash {
        println!("  sha256:          {hash}");
    }

    Ok(())
}

/// Build a reqwest client that mirrors how snapshot transfer authenticates:
/// the API key (if any) is sent as a sensitive `api-key` default header.
fn build_client(api_key: Option<&str>, insecure: bool) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().user_agent(APP_USER_AGENT.as_str());

    if let Some(api_key) = api_key {
        let mut headers = HeaderMap::new();
        let mut value = HeaderValue::from_str(api_key).context("malformed API key")?;
        value.set_sensitive(true);
        headers.insert(api::HTTP_HEADER_API_KEY, value);
        builder = builder.default_headers(headers);
    }

    if insecure {
        builder = builder
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    }

    Ok(builder.build()?)
}

/// Recursively count regular files and total bytes under `dir`.
fn dir_stats(dir: &std::path::Path) -> anyhow::Result<(u64, u64)> {
    let mut files = 0;
    let mut bytes = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in fs_err::read_dir(&path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                files += 1;
                bytes += entry.metadata()?.len();
            } else {
                bail!("unexpected non-regular entry: {}", entry.path().display());
            }
        }
    }
    Ok((files, bytes))
}
