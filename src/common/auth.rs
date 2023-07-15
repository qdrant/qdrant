use crate::settings::ServiceConfig;

/// The API keys used for auth
#[derive(Clone, Debug)]
pub enum AuthScheme {
    /// A key is allowing Read/Write operations and another key is allowing Read operations while
    /// forbidding all Write ones.
    SeparateReadAndReadWrite {
        read_write: String,
        read_only: String,
    },

    /// A key is allowing Read operations. All Write operations are forbidden.
    ReadOnly { read_only: String },

    /// A key is allowing all Read and Write operations.
    ReadWrite { read_write: String },
}

impl AuthScheme {
    /// Defines the auth scheme given the service config
    ///
    /// Returns None if no scheme is specified.
    pub fn try_create(service_config: &ServiceConfig) -> Option<Self> {
        match (
            service_config.api_key.clone(),
            service_config.read_only_api_key.clone(),
        ) {
            (None, None) => None,
            (Some(read_write), Some(read_only)) => Some(Self::SeparateReadAndReadWrite {
                read_write,
                read_only,
            }),
            (Some(read_write), None) => Some(Self::ReadWrite { read_write }),
            (None, Some(read_only)) => Some(Self::ReadOnly { read_only }),
        }
    }
}
