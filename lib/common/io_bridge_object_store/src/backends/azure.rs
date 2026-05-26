//! Microsoft Azure Blob Storage backend.

use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};

use crate::backend::BlobBackend;

/// Connection parameters for [`MicrosoftAzure`]. Fed into
/// [`MicrosoftAzureBuilder`] by the [`BlobBackend`] impl below.
#[derive(Clone, Debug)]
pub struct AzureConfig {
    /// Storage account name (without the `.blob.core.windows.net` suffix).
    pub account: String,
    /// Container name within the storage account.
    pub container: String,
    /// Custom endpoint URL — set for Azurite / emulator. When set,
    /// `with_allow_http(true)` is enabled.
    pub endpoint: Option<String>,
    /// How to authenticate to Azure.
    pub credentials: AzureCredentials,
}

/// Authentication mode for Azure Blob Storage.
#[derive(Clone, Debug)]
pub enum AzureCredentials {
    /// Azure default credential chain: env vars, managed identity, az CLI, …
    Default,
    /// Account shared access key.
    AccessKey(String),
    /// OAuth bearer token (e.g. obtained from `az account get-access-token`).
    BearerToken(String),
    /// Service-principal client-secret OAuth flow.
    ClientSecret {
        client_id: String,
        client_secret: String,
        tenant_id: String,
    },
    /// SAS (shared access signature) query-string pairs.
    Sas(Vec<(String, String)>),
}

impl BlobBackend for MicrosoftAzure {
    type Config = AzureConfig;

    fn build_store(config: &Self::Config) -> Result<Self> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&config.account)
            .with_container_name(&config.container);
        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint.clone())
                .with_allow_http(true);
        }
        builder = match &config.credentials {
            AzureCredentials::Default => builder,
            AzureCredentials::AccessKey(k) => builder.with_access_key(k),
            AzureCredentials::BearerToken(t) => builder.with_bearer_token_authorization(t),
            AzureCredentials::ClientSecret {
                client_id,
                client_secret,
                tenant_id,
            } => builder
                .with_client_id(client_id)
                .with_client_secret(client_secret)
                .with_tenant_id(tenant_id),
            AzureCredentials::Sas(pairs) => builder.with_sas_authorization(pairs.clone()),
        };
        builder.build().map_err(|err| UniversalIoError::S3Config {
            description: format!("MicrosoftAzureBuilder: {err}"),
        })
    }

    fn kind() -> UniversalKind {
        UniversalKind::Azure
    }
}
