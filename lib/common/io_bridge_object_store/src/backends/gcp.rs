//! Google Cloud Storage backend.

use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};

use crate::backend::BlobBackend;

#[derive(Clone, Debug)]
pub struct GcsConfig {
    pub bucket: String,
    pub credentials: GcsCredentials,
}

#[derive(Clone, Debug)]
pub enum GcsCredentials {
    /// Application default credentials (ADC: env, gcloud, metadata server).
    Default,
    /// Path to a service-account JSON key file.
    ServiceAccountPath(String),
    /// Inline service-account JSON key contents.
    ServiceAccountKey(String),
    /// Path to an application_default_credentials.json file.
    ApplicationCredentialsPath(String),
}

impl BlobBackend for GoogleCloudStorage {
    type Config = GcsConfig;

    fn build_store(config: &Self::Config) -> Result<Self> {
        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket);
        builder = match &config.credentials {
            GcsCredentials::Default => builder,
            GcsCredentials::ServiceAccountPath(p) => builder.with_service_account_path(p),
            GcsCredentials::ServiceAccountKey(k) => builder.with_service_account_key(k),
            GcsCredentials::ApplicationCredentialsPath(p) => {
                builder.with_application_credentials(p)
            }
        };
        builder.build().map_err(|err| UniversalIoError::S3Config {
            description: format!("GoogleCloudStorageBuilder: {err}"),
        })
    }

    fn kind() -> UniversalKind {
        UniversalKind::Gcs
    }
}
