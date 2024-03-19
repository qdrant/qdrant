use std::sync::Arc;

use collection::operations::{
    shard_selector_internal::ShardSelectorInternal, types::ScrollRequestInternal,
};
use rbac::jwt::Claims;
use rbac::JwtParser;
use segment::types::{WithPayloadInterface, WithVector};
use storage::content_manager::toc::TableOfContent;

use super::strings::ct_eq;
use crate::settings::ServiceConfig;

/// The API keys used for auth
#[derive(Clone)]
pub struct AuthKeys {
    /// A key allowing Read or Write operations
    read_write: Option<String>,

    /// A key allowing Read operations
    read_only: Option<String>,

    /// A JWT parser, based on the read_write key
    jwt_parser: Option<JwtParser>,

    toc: Arc<TableOfContent>,
}

impl AuthKeys {
    /// Defines the auth scheme given the service config
    ///
    /// Returns None if no scheme is specified.
    pub fn try_create(service_config: &ServiceConfig, toc: Arc<TableOfContent>) -> Option<Self> {
        match (
            service_config.api_key.clone(),
            service_config.read_only_api_key.clone(),
        ) {
            (None, None) => None,
            (read_write, read_only) => Some(Self {
                read_write,
                read_only,
                jwt_parser: service_config
                    .api_key
                    .as_ref()
                    .map(|secret| JwtParser::new(secret)),
                toc,
            }),
        }
    }

    /// Validate that the specified request is allowed for given keys.
    ///
    /// # Returns
    ///
    /// - `Ok(None)` if the request is allowed through the API key.
    /// - `Ok(Some(claims))` if the request is allowed through the JWT token.
    /// - `Err(description)` if the request is not allowed.
    pub async fn validate_request<'a>(
        &self,
        get_header: impl Fn(&'a str) -> Option<&'a str>,
        is_read_only: bool,
    ) -> Result<Option<Claims>, String> {
        let Some(key) = get_header("api-key")
            .or_else(|| get_header("authorization").and_then(|v| v.strip_prefix("Bearer ")))
        else {
            return Err("Must provide an API key or an Authorization bearer token".to_string());
        };

        if self.can_write(key) || (is_read_only && self.can_read(key)) {
            return Ok(None);
        }

        if !is_read_only && self.can_read(key) {
            return Err("Write access denied".to_string());
        }

        if let Some(claims) = self.jwt_parser.as_ref().and_then(|p| p.decode(key).ok()) {
            let Claims {
                expiration: _,  // already validated on decoding
                write_access,
                value_exists,
                payload: _,     //
                collections: _, // will be validated in TableOfContent
            } = &claims;

            if !write_access.unwrap_or(false) && !is_read_only {
                return Err("Write access denied".to_string());
            }

            if let Some(value_exists) = value_exists {
                let scroll_req = ScrollRequestInternal {
                    offset: None,
                    limit: Some(1),
                    filter: Some(value_exists.as_filter()),
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: WithVector::Bool(false),
                    order_by: None,
                };
                let res = self
                    .toc
                    .scroll(
                        &value_exists.collection,
                        scroll_req,
                        None,
                        ShardSelectorInternal::All,
                        None,
                    )
                    .await
                    .map_err(|e| format!("Could not confirm validity of JWT: {e}"))?;

                if res.points.is_empty() {
                    return Err("Invalid JWT, stateful validation failed".to_string());
                }
            }

            return Ok(Some(claims));
        }

        Err("Invalid API key or JWT".to_string())
    }

    /// Check if a key is allowed to read
    #[inline]
    fn can_read(&self, key: &str) -> bool {
        self.read_only
            .as_ref()
            .map(|ro_key| ct_eq(ro_key, key))
            .unwrap_or_default()
    }

    /// Check if a key is allowed to write
    #[inline]
    fn can_write(&self, key: &str) -> bool {
        self.read_write
            .as_ref()
            .map(|rw_key| ct_eq(rw_key, key))
            .unwrap_or_default()
    }
}
