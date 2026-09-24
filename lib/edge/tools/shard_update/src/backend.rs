//! Backend connection configs and the cached-blob filesystem built on top of them.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use common::universal_io::{DiskCacheConfig, UniversalReadFileOps};
use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
use io_bridge_object_store::{AsyncAppend, CachedBlobFs, CachedBlobFsContext};

use crate::cli::ConnectionArgs;

/// The bucket name, required by the object-storage backends.
fn require_bucket(conn: &ConnectionArgs) -> Result<String> {
    conn.bucket
        .clone()
        .ok_or_else(|| anyhow!("--bucket is required for the {:?} backend", conn.backend))
}

pub fn build_aws_config(conn: &ConnectionArgs) -> Result<AwsConfig> {
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
        native_append: conn.native_append,
        credentials,
    })
}

pub fn build_gcs_config(conn: &ConnectionArgs) -> Result<GcsConfig> {
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

/// Build the combined cached-blob filesystem: reads are served through a
/// local disk-cache mirror (each remote block fetched once), appends go
/// straight to the remote.
pub fn build_cached_fs<A>(
    remote_config: A::Config,
    remote_prefix: &Path,
    cache_dir: &Path,
) -> Result<CachedBlobFs<A>>
where
    A: AsyncAppend + Clone,
    A::Config: Clone,
{
    fs_err::create_dir_all(cache_dir)
        .with_context(|| format!("failed to create cache dir {}", cache_dir.display()))?;

    let config = DiskCacheConfig::new(remote_prefix.to_path_buf(), cache_dir.to_path_buf())
        .context("failed to build disk cache config")?;

    CachedBlobFs::<A>::from_context(CachedBlobFsContext {
        disk_cache: Arc::new(config),
        remote: remote_config,
    })
    .context("failed to build cached-blob filesystem")
}
