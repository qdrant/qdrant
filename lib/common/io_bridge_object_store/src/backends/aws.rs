//! AWS S3 backend.

use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::aws::{AmazonS3, AmazonS3Builder};
use url::Url;

use crate::append::AppendContext;
use crate::backend::BlobBackend;

/// SigV4 signing needs a region even for endpoints that ignore it.
const DEFAULT_REGION: &str = "us-east-1";

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
    /// Enable S3 Express One Zone (`*--x-s3` directory buckets).
    pub s3_express: bool,
    /// How to authenticate to S3.
    pub credentials: AwsCredentials,
}

/// Authentication mode for AWS S3.
#[derive(Clone, Debug)]
pub enum AwsCredentials {
    /// AWS default credential chain, resolved from the environment by `object_store`.
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
        let mut builder = match &config.credentials {
            AwsCredentials::Default => AmazonS3Builder::from_env(),
            AwsCredentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let mut b = AmazonS3Builder::new()
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);
                if let Some(t) = session_token {
                    b = b.with_token(t);
                }
                b
            }
        };
        builder = builder
            .with_bucket_name(&config.bucket)
            .with_s3_express(config.s3_express);
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        builder.build().map_err(|err| UniversalIoError::S3Config {
            description: format!("AmazonS3Builder: {err}"),
        })
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }

    fn append_context(config: &Self::Config) -> Result<Option<AppendContext>> {
        let s3_config_error = |description: String| UniversalIoError::S3Config { description };

        let (endpoint, region) =
            resolve_endpoint_and_region(config, |name| std::env::var(name).ok());

        let object_url_base = match &endpoint {
            // Path-style addressing, as object_store uses for custom
            // endpoints.
            Some(endpoint) => {
                let mut url = Url::parse(endpoint)
                    .map_err(|err| s3_config_error(format!("append endpoint url: {err}")))?;
                url.path_segments_mut()
                    .map_err(|()| s3_config_error("append endpoint url cannot be a base".into()))?
                    .pop_if_empty()
                    .push(&config.bucket);
                url
            }
            // Virtual-hosted-style addressing, as object_store uses for real
            // AWS.
            None => {
                let url = format!("https://{}.s3.{region}.amazonaws.com", config.bucket);
                Url::parse(&url)
                    .map_err(|err| s3_config_error(format!("append object url: {err}")))?
            }
        };

        // Allowing plain http mirrors `build_store`: custom endpoints
        // (MinIO & co) are commonly plain http. The HTTP client itself is
        // built lazily on first append.
        let allow_http = endpoint.is_some();

        Ok(Some(AppendContext::new(
            allow_http,
            object_url_base,
            region,
        )))
    }
}

/// Resolve the endpoint and signing region for appends the same way
/// [`BlobBackend::build_store`] resolves them for every other request:
/// explicit config fields first, and — only with the default credential
/// chain, where the store is built via [`AmazonS3Builder::from_env`] — the
/// same environment variables `from_env` honors. Without this, reads and
/// appends could target different hosts or signing regions.
fn resolve_endpoint_and_region(
    config: &AwsConfig,
    env: impl Fn(&str) -> Option<String>,
) -> (Option<String>, String) {
    let from_env = |names: &[&str]| {
        matches!(config.credentials, AwsCredentials::Default)
            .then(|| names.iter().find_map(|name| env(name)))
            .flatten()
    };

    let endpoint = config.endpoint.clone().or_else(|| {
        // `AWS_ENDPOINT_URL_S3` wins over the generic names, as in
        // `AmazonS3Builder::from_env`.
        from_env(&["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL", "AWS_ENDPOINT"])
    });

    let region = config
        .region
        .clone()
        .or_else(|| from_env(&["AWS_REGION", "AWS_DEFAULT_REGION"]))
        .unwrap_or_else(|| DEFAULT_REGION.to_string());

    (endpoint, region)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(credentials: AwsCredentials) -> AwsConfig {
        AwsConfig {
            bucket: "bucket".to_string(),
            region: None,
            endpoint: None,
            s3_express: false,
            credentials,
        }
    }

    #[test]
    fn append_endpoint_and_region_resolve_like_build_store() {
        let env = |name: &str| match name {
            "AWS_ENDPOINT" => Some("http://minio:9000".to_string()),
            "AWS_REGION" => Some("eu-west-1".to_string()),
            _ => None,
        };

        // Default credential chain: the environment applies.
        let (endpoint, region) = resolve_endpoint_and_region(&config(AwsCredentials::Default), env);
        assert_eq!(endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(region, "eu-west-1");

        // Explicit config fields win over the environment.
        let mut explicit = config(AwsCredentials::Default);
        explicit.endpoint = Some("http://other:9000".to_string());
        explicit.region = Some("us-west-2".to_string());
        let (endpoint, region) = resolve_endpoint_and_region(&explicit, env);
        assert_eq!(endpoint.as_deref(), Some("http://other:9000"));
        assert_eq!(region, "us-west-2");

        // Static credentials never consult the environment, mirroring
        // `build_store` (which only uses `from_env` for `Default`).
        let static_credentials = AwsCredentials::Static {
            access_key_id: "id".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
        };
        let (endpoint, region) = resolve_endpoint_and_region(&config(static_credentials), env);
        assert_eq!(endpoint, None);
        assert_eq!(region, DEFAULT_REGION);

        // `AWS_ENDPOINT_URL_S3` wins over the generic endpoint names.
        let env_s3 = |name: &str| match name {
            "AWS_ENDPOINT_URL_S3" => Some("http://s3:9000".to_string()),
            "AWS_ENDPOINT" => Some("http://generic:9000".to_string()),
            _ => None,
        };
        let (endpoint, _) = resolve_endpoint_and_region(&config(AwsCredentials::Default), env_s3);
        assert_eq!(endpoint.as_deref(), Some("http://s3:9000"));
    }
}
