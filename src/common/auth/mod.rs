use std::sync::Arc;

use collection::operations::shard_selector_internal::ShardSelectorInternal;
use collection::operations::types::ScrollRequestInternal;
use segment::types::{WithPayloadInterface, WithVector};
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;

use self::claims::{Claims, ValueExists};
use self::jwt_parser::JwtParser;
use super::strings::ct_eq;
use crate::settings::ServiceConfig;

pub mod claims;
pub mod jwt_parser;

/// The API keys used for auth
#[derive(Clone)]
pub struct AuthKeys {
    /// A key allowing Read or Write operations
    read_write: Option<String>,

    /// A key allowing Read operations
    read_only: Option<String>,

    /// A JWT parser, based on the read_write key
    jwt_parser: Option<JwtParser>,

    /// Table of content, needed to do stateful validation of JWT
    toc: Arc<TableOfContent>,
}

#[derive(Debug)]
pub enum AuthError {
    Unauthorized(String),
    Forbidden(String),
    StorageError(StorageError),
}

impl AuthKeys {
    fn get_jwt_parser(service_config: &ServiceConfig) -> Option<JwtParser> {
        if service_config.jwt_rbac.unwrap_or_default() {
            service_config
                .api_key
                .as_ref()
                .map(|secret| JwtParser::new(secret))
        } else {
            None
        }
    }

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
                jwt_parser: Self::get_jwt_parser(service_config),
                toc,
            }),
        }
    }

    /// Validate that the specified request is allowed for given keys.
    pub async fn validate_request<'a>(
        &self,
        get_header: impl Fn(&'a str) -> Option<&'a str>,
        is_read_only: bool,
    ) -> Result<Access, AuthError> {
        let Some(key) = get_header("api-key")
            .or_else(|| get_header("authorization").and_then(|v| v.strip_prefix("Bearer ")))
        else {
            return Err(AuthError::Unauthorized(
                "Must provide an API key or an Authorization bearer token".to_string(),
            ));
        };

        if is_read_only {
            if self.can_read(key) || self.can_write(key) {
                return Ok(Access::full_ro("Read-only access by key"));
            }
        } else {
            if self.can_write(key) {
                return Ok(Access::full("Read-write access by key"));
            }
            if self.can_read(key) {
                return Err(AuthError::Forbidden("Write access denied".to_string()));
            }
        }

        if let Some(claims) = self.jwt_parser.as_ref().and_then(|p| p.decode(key)) {
            let Claims {
                exp: _, // already validated on decoding
                access,
                value_exists,
            } = claims?;

            if let Some(value_exists) = value_exists {
                self.validate_value_exists(&value_exists).await?;
            }

            return Ok(access);
        }

        Err(AuthError::Unauthorized(
            "Invalid API key or JWT".to_string(),
        ))
    }

    async fn validate_value_exists(&self, value_exists: &ValueExists) -> Result<(), AuthError> {
        let scroll_req = ScrollRequestInternal {
            offset: None,
            limit: Some(1),
            filter: Some(value_exists.to_filter()),
            with_payload: Some(WithPayloadInterface::Bool(false)),
            with_vector: WithVector::Bool(false),
            order_by: None,
        };

        let res = self
            .toc
            .scroll(
                value_exists.get_collection(),
                scroll_req,
                None,
                ShardSelectorInternal::All,
                Access::full("JWT stateful validation"),
            )
            .await
            .map_err(|e| match e {
                StorageError::NotFound { .. } => {
                    AuthError::Forbidden("Invalid JWT, stateful validation failed".to_string())
                }
                _ => AuthError::StorageError(e),
            })?;

        if res.points.is_empty() {
            return Err(AuthError::Unauthorized(
                "Invalid JWT, stateful validation failed".to_string(),
            ));
        };

        Ok(())
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
