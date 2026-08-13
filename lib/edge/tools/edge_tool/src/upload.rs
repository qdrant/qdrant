//! Recursively upload a local collection directory to S3/GCS object storage.

use std::ffi::OsStr;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use edge::WAL_PATH;
use futures::StreamExt as _;
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
use io_bridge_object_store::{BlobBackend, BridgeRuntime};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;
use object_store::{ObjectStore, ObjectStoreExt as _, WriteMultipart};
use walkdir::WalkDir;

use crate::args::UploadArgs;

/// Chunk size for `WriteMultipart`: every non-final part must be at least
/// 5 MiB on S3/GCS, so this stays comfortably above that.
const MULTIPART_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Parts of one file in flight at once. `WriteMultipart::write` starts an
/// upload the moment a chunk fills, however many are already running, so
/// without this a whole file is resident as spawned parts.
const MAX_PARTS_IN_FLIGHT: usize = 4;

pub fn run(args: UploadArgs) -> Result<()> {
    if !args.source.is_dir() {
        bail!("source {} is not a directory", args.source.display());
    }
    if args.clean && args.destination.trim_matches('/').is_empty() {
        bail!("--clean with an empty destination would delete every object in the bucket");
    }

    let files = collect_files(&args.source, args.include_wal)?;
    if files.is_empty() && !args.clean {
        log::warn!("{} has no files to upload", args.source.display());
        return Ok(());
    }

    let runtime = BridgeRuntime::new().context("failed to start the upload runtime")?;
    let concurrency = args.concurrency.max(1);

    if args.gcs {
        let config = build_gcs_config(&args)?;
        let store =
            GoogleCloudStorage::build_store(&config).context("failed to build the GCS client")?;
        runtime.block_on(clean_and_upload(&store, &args, &files, concurrency))?;
    } else {
        let config = build_aws_config(&args)?;
        let store = AmazonS3::build_store(&config).context("failed to build the S3 client")?;
        runtime.block_on(clean_and_upload(&store, &args, &files, concurrency))?;
    }

    log::info!("uploaded {} file(s) to {}", files.len(), args.destination);
    Ok(())
}

async fn clean_and_upload<O: ObjectStore>(
    store: &O,
    args: &UploadArgs,
    files: &[PathBuf],
    concurrency: usize,
) -> Result<()> {
    if args.clean {
        let removed = clean_destination(store, &destination_prefix(&args.destination)).await?;
        log::info!(
            "removed {removed} existing object(s) under {}",
            args.destination
        );
    }

    log::info!(
        "uploading {} file(s) from {} to {}",
        files.len(),
        args.source.display(),
        args.destination,
    );
    upload_all(store, &args.source, &args.destination, files, concurrency).await
}

/// Delete every object already under `prefix`, so a re-upload doesn't leave
/// stale files behind from a previous run with a different shape (e.g.
/// different segment UUIDs).
async fn clean_destination<O: ObjectStore>(
    store: &O,
    prefix: &object_store::path::Path,
) -> Result<usize> {
    let locations = store
        .list(Some(prefix))
        .map(|meta| meta.map(|meta| meta.location))
        .boxed();

    let mut removed = 0usize;
    let mut results = store.delete_stream(locations);
    while let Some(result) = results.next().await {
        result.context("failed to delete an existing object under the destination prefix")?;
        removed += 1;
    }
    Ok(removed)
}

/// The WAL is write-path state: the read paths open segments and the config
/// only, and an [`edge::EdgeShard`] recreates an empty one. Its segments are
/// preallocated to their full capacity and never truncated, so it dwarfs a
/// freshly seeded collection.
fn collect_files(root: &Path, include_wal: bool) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut skipped_wal = 0usize;
    for entry in WalkDir::new(root) {
        let entry = entry.with_context(|| format!("failed to walk {}", root.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        if !include_wal && entry.path().strip_prefix(root).is_ok_and(is_wal_path) {
            skipped_wal += 1;
            continue;
        }
        files.push(entry.into_path());
    }
    if skipped_wal > 0 {
        log::info!("skipped {skipped_wal} WAL file(s); pass --include-wal to upload them");
    }
    files.sort();
    Ok(files)
}

fn is_wal_path(relative: &Path) -> bool {
    relative
        .components()
        .next()
        .is_some_and(|first| first.as_os_str() == OsStr::new(WAL_PATH))
}

async fn upload_all<O: ObjectStore>(
    store: &O,
    source: &Path,
    destination: &str,
    files: &[PathBuf],
    concurrency: usize,
) -> Result<()> {
    futures::stream::iter(files)
        .map(|file| upload_file(store, source, destination, file))
        .buffer_unordered(concurrency)
        .collect::<Vec<Result<()>>>()
        .await
        .into_iter()
        .collect()
}

/// Stream a single local file into object storage as a multipart upload.
/// Every file goes through the multipart path (even small ones, as a single
/// final part) so there is one upload code path regardless of size.
async fn upload_file<O: ObjectStore>(
    store: &O,
    source: &Path,
    destination: &str,
    file: &Path,
) -> Result<()> {
    let relative = file
        .strip_prefix(source)
        .with_context(|| format!("{} is not under {}", file.display(), source.display()))?;
    let key = object_key(destination, relative)?;

    let upload = store
        .put_multipart(&key)
        .await
        .with_context(|| format!("failed to start upload of {}", file.display()))?;
    let mut write = WriteMultipart::new_with_chunk_size(upload, MULTIPART_CHUNK_SIZE);

    let mut reader =
        fs_err::File::open(file).with_context(|| format!("failed to open {}", file.display()))?;
    let mut buffer = vec![0u8; MULTIPART_CHUNK_SIZE];
    loop {
        let bytes_read = reader
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", file.display()))?;
        if bytes_read == 0 {
            break;
        }
        write
            .wait_for_capacity(MAX_PARTS_IN_FLIGHT)
            .await
            .with_context(|| format!("failed to upload a part of {}", file.display()))?;
        write.write(&buffer[..bytes_read]);
    }
    write
        .finish()
        .await
        .with_context(|| format!("failed to finish upload of {}", file.display()))?;

    log::debug!("uploaded {} -> {key}", file.display());
    Ok(())
}

fn destination_prefix(destination: &str) -> object_store::path::Path {
    object_store::path::Path::from(destination.trim_matches('/'))
}

fn object_key(destination: &str, relative: &Path) -> Result<object_store::path::Path> {
    let relative = relative
        .to_str()
        .ok_or_else(|| anyhow!("non-UTF8 path: {}", relative.display()))?;
    let destination = destination.trim_matches('/');
    let key = if destination.is_empty() {
        relative.to_string()
    } else {
        format!("{destination}/{relative}")
    };
    Ok(object_store::path::Path::from(key))
}

fn require_bucket(args: &UploadArgs) -> Result<String> {
    args.bucket
        .clone()
        .ok_or_else(|| anyhow!("--bucket is required"))
}

fn build_aws_config(args: &UploadArgs) -> Result<AwsConfig> {
    let credentials = match (&args.access_key, &args.secret_key) {
        (Some(access_key_id), Some(secret_access_key)) => AwsCredentials::Static {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: args.session_token.clone(),
        },
        (None, None) => AwsCredentials::Default,
        _ => bail!("--access-key and --secret-key must be provided together"),
    };

    Ok(AwsConfig {
        bucket: require_bucket(args)?,
        region: args.region.clone(),
        endpoint: args.endpoint.clone(),
        s3_express: args.s3_express,
        native_append: false,
        credentials,
    })
}

fn build_gcs_config(args: &UploadArgs) -> Result<GcsConfig> {
    let credentials = if let Some(key) = &args.gcs_service_account_key {
        GcsCredentials::ServiceAccountKey(key.clone())
    } else if let Some(path) = &args.gcs_service_account_path {
        GcsCredentials::ServiceAccountPath(path.clone())
    } else {
        GcsCredentials::Default
    };

    Ok(GcsConfig {
        bucket: require_bucket(args)?,
        credentials,
    })
}
