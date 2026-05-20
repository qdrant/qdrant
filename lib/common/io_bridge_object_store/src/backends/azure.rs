//! Microsoft Azure Blob Storage backend.

use common::universal_io::{Result, UniversalIoError, UniversalKind};
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};

use crate::backend::BlobBackend;

#[derive(Clone, Debug)]
pub struct AzureConfig {
    pub account: String,
    pub container: String,
    pub endpoint: Option<String>,
    pub credentials: AzureCredentials,
}

#[derive(Clone, Debug)]
pub enum AzureCredentials {
    /// Azure default credential chain (env vars, managed identity, …).
    Default,
    /// Shared key (account access key).
    AccessKey(String),
    /// Bearer token (e.g. from `az account get-access-token`).
    BearerToken(String),
    /// Client-secret OAuth flow.
    ClientSecret {
        client_id: String,
        client_secret: String,
        tenant_id: String,
    },
    /// SAS query-string pairs.
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
