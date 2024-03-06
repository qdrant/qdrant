use super::strings::ct_eq;
use crate::settings::ServiceConfig;

/// The API keys used for auth
#[derive(Clone, Debug)]
pub struct AuthKeys {
    /// A key allowing Read or Write operations
    read_write: Option<String>,

    /// A key allowing Read operations
    read_only: Option<String>,
}

impl AuthKeys {
    /// Defines the auth scheme given the service config
    ///
    /// Returns None if no scheme is specified.
    pub fn try_create(service_config: &ServiceConfig) -> Option<Self> {
        match (
            service_config.api_key.clone(),
            service_config.read_only_api_key.clone(),
        ) {
            (None, None) => None,
            (read_write, read_only) => Some(Self {
                read_write,
                read_only,
            }),
        }
    }

    #[allow(dead_code)] // Not actually dead
    pub fn rw_key(&self) -> Option<&str> {
        self.read_write.as_deref()
    }

    /// Check if a key is allowed to read
    #[inline]
    pub fn can_read(&self, key: &str) -> bool {
        self.read_only
            .as_ref()
            .map(|ro_key| ct_eq(ro_key, key))
            .unwrap_or_else(|| self.can_write(key))
    }

    /// Check if a key is allowed to write
    #[inline]
    pub fn can_write(&self, key: &str) -> bool {
        self.read_write
            .as_ref()
            .map(|rw_key| ct_eq(rw_key, key))
            .unwrap_or_default()
    }
}
