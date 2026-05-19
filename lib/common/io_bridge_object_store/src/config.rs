use std::sync::Arc;

use object_store::ObjectStore;

#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: S3Credentials,
}

#[derive(Clone, Debug)]
pub enum S3Credentials {
    /// AWS default chain (env vars, IMDS, etc.)
    Default,
    /// Static access key + secret
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}

pub fn build_object_store(
    config: &S3Config,
) -> Result<Arc<dyn ObjectStore>, common::universal_io::UniversalIoError> {
    let mut builder = object_store::aws::AmazonS3Builder::new().with_bucket_name(&config.bucket);

    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }

    builder = match &config.credentials {
        S3Credentials::Default => builder,
        S3Credentials::Static {
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

    let store =
        builder
            .build()
            .map_err(|err| common::universal_io::UniversalIoError::S3Config {
                description: format!("AmazonS3Builder: {err}"),
            })?;

    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_static_credentials_succeeds() {
        let config = S3Config {
            bucket: "test-bucket".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://localhost:9000".into()),
            credentials: S3Credentials::Static {
                access_key_id: "ak".into(),
                secret_access_key: "sk".into(),
                session_token: None,
            },
        };
        let store = build_object_store(&config).expect("build should succeed");
        std::mem::drop(store.clone());
    }

    #[test]
    fn build_default_credentials_with_region_succeeds() {
        let config = S3Config {
            bucket: "test-bucket".into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            credentials: S3Credentials::Default,
        };
        std::mem::drop(build_object_store(&config).expect("build should succeed"));
    }
}
