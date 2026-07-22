//! Google Cloud Storage backend.

use common::universal_io::{UioResult, UniversalIoError, UniversalKind};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};

use crate::backend::BlobBackend;

/// Connection parameters for [`GoogleCloudStorage`]. Fed into
/// [`GoogleCloudStorageBuilder`] by the [`BlobBackend`] impl below.
#[derive(Clone, Debug)]
pub struct GcsConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// How to authenticate to GCS.
    pub credentials: GcsCredentials,
}

/// Authentication mode for Google Cloud Storage.
#[derive(Clone, Debug)]
pub enum GcsCredentials {
    /// Application default credentials (ADC): env vars, gcloud config,
    /// metadata server (GCE / GKE workload identity), etc.
    Default,
    /// Filesystem path to a service-account JSON key file.
    ServiceAccountPath(String),
    /// Inline contents of a service-account JSON key — same shape as the
    /// `*.json` you would download from the Cloud Console, but passed as a
    /// string instead of a path.
    ServiceAccountKey(String),
    /// Filesystem path to an `application_default_credentials.json` file
    /// (typically `~/.config/gcloud/application_default_credentials.json`).
    ApplicationCredentialsPath(String),
}

impl BlobBackend for GoogleCloudStorage {
    type Config = GcsConfig;

    fn build_store(config: &Self::Config) -> UioResult<Self> {
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
