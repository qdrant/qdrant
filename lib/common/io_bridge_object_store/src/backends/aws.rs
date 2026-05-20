//! AWS S3 backend.

use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::aws::{AmazonS3, AmazonS3Builder};

use crate::backend::BlobBackend;

#[derive(Clone, Debug)]
pub struct AwsConfig {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: AwsCredentials,
}

#[derive(Clone, Debug)]
pub enum AwsCredentials {
    /// AWS default chain (env vars, IMDS, etc.)
    Default,
    /// Static access key + secret (+ optional session token)
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
