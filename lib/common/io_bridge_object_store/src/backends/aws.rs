//! AWS S3 backend.

use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::backend::BlobBackend;

/// Connection parameters for [`AmazonS3`]. Fed into [`AmazonS3Builder`] by
/// the [`BlobBackend`] impl below.
#[derive(Clone, Debug)]
pub struct AwsConfig {
    /// S3 bucket name (without `s3://` prefix).
    pub bucket: String,
    /// AWS region (e.g. `us-east-1`). Required for real AWS; optional for
    /// S3-compatible endpoints that ignore it.
    pub region: Option<String>,
    /// Custom endpoint URL — set this for MinIO / RustFS / LocalStack.
    /// `with_allow_http(true)` is enabled when this is set.
    pub endpoint: Option<String>,
    /// How to authenticate to S3.
    pub credentials: AwsCredentials,
}

/// Authentication mode for AWS S3.
#[derive(Clone, Debug)]
pub enum AwsCredentials {
    /// AWS default credential chain: env vars, shared config files, IMDS,
    /// ECS task role, etc. (Delegates to whatever the AWS SDK auto-detects.)
    Default,
    /// Hard-coded access key + secret + optional session token. Useful for
    /// tests and for short-lived credentials provided out-of-band.
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

impl BlobBackend for AmazonS3 {
    type Config = AwsConfig;

    fn build_store(config: &Self::Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        builder = match &config.credentials {
            AwsCredentials::Default => builder,
            AwsCredentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let mut b = builder
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
                if let Some(t) = session_token {
                    b = b.with_token(t);
                }
                b
            }
        };
        builder.build().map_err(|err| UniversalIoError::S3Config {
            description: format!("AmazonS3Builder: {err}"),
        })
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}
