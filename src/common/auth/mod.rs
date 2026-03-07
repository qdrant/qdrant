use std::sync::Arc;

use collection::operations::shard_selector_internal::ShardSelectorInternal;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::types::{WithPayloadInterface, WithVector};
use shard::scroll::ScrollRequestInternal;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::Access;

use self::claims::{Claims, ValueExists};
use self::jwt_parser::JwtParser;
use super::strings::ct_eq;
use crate::common::inference::api_keys::InferenceToken;
use crate::settings::ServiceConfig;
pub mod claims;
pub mod jwt_parser;

// Re-export Auth and AuthType from storage crate.
pub use storage::rbac::AuthType;
pub use storage::rbac::auth::Auth;

pub const HTTP_HEADER_API_KEY: &str = "api-key";

/// The API keys used for auth
#[derive(Clone)]
pub struct AuthKeys {
    /// A key allowing Read or Write operations
    read_write: Option<String>,

    /// Alternative to `read_write` key
    alt_read_write: Option<String>,

    /// A key allowing Read operations
    read_only: Option<String>,

    /// A JWT parser, based on the read_write key
    jwt_parser: Option<JwtParser>,

    /// Alternative JWT parser, based on the alt_read_write key
    alt_jwt_parser: Option<JwtParser>,

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
    fn get_jwt_parser(service_config: &ServiceConfig) -> (Option<JwtParser>, Option<JwtParser>) {
        if service_config.jwt_rbac.unwrap_or_default() {
            (
                service_config
                    .api_key
                    .as_ref()
                    .map(|secret| JwtParser::new(secret)),
                service_config
                    .alt_api_key
                    .as_ref()
                    .map(|secret| JwtParser::new(secret)),
            )
        } else {
            (None, None)
        }
    }

    /// Defines the auth scheme given the service config
    ///
    /// Returns None if no scheme is specified.
    pub fn try_create(service_config: &ServiceConfig, toc: Arc<TableOfContent>) -> Option<Self> {
        match (
            service_config.api_key.clone(),
            service_config.alt_api_key.clone(),
            service_config.read_only_api_key.clone(),
        ) {
            (None, None, None) => None,
            (read_write, alt_read_write, read_only) => {
                let (jwt_parser, alt_jwt_parser) = Self::get_jwt_parser(service_config);

                Some(Self {
                    read_write,
                    alt_read_write,
                    read_only,
                    jwt_parser,
                    alt_jwt_parser,
                    toc,
                })
            }
        }
    }

    /// Validate that the specified request is allowed for given keys.
    ///
    /// Returns `(Access, InferenceToken, AuthType, Option<subject>)`.
    pub async fn validate_request<'a>(
        &self,
        get_header: impl Fn(&'a str) -> Option<&'a str>,
        blacklist_matches: bool,
    ) -> Result<(Access, InferenceToken, AuthType, Option<String>), AuthError> {
        let Some(key) = get_header(HTTP_HEADER_API_KEY)
            .or_else(|| get_header("authorization").and_then(|v| v.strip_prefix("Bearer ")))
        else {
            return Err(AuthError::Unauthorized(
                "Must provide an API key or an Authorization bearer token".to_string(),
            ));
        };

        if self.can_write(key) {
            return Ok((
                Access::full("Read-write access by key"),
                InferenceToken(None),
                AuthType::ApiKey,
                None,
            ));
        }

        if self.can_read(key) {
            return Ok((
                Access::full_ro("Read-only access by key"),
                InferenceToken(None),
                AuthType::ApiKey,
                None,
            ));
        }

        let (claims, errors): (Vec<_>, Vec<_>) =
            [self.jwt_parser.as_ref(), self.alt_jwt_parser.as_ref()]
                .into_iter()
                .flatten()
                .filter_map(|p| p.decode(key))
                .partition_result();

        if let Some(claims) = claims.into_iter().next() {
            let Claims {
                sub,
                exp: _, // already validated on decoding
                access,
                value_exists,
                subject,
            } = claims;

            if let Some(value_exists) = value_exists {
                self.validate_value_exists(&value_exists).await?;
            }

            if blacklist_matches {
                return Err(AuthError::Forbidden(
                    "This path is blacklisted by config".to_string(),
                ));
            }

            return Ok((access, InferenceToken(sub), AuthType::Jwt, subject));
        }

        // JTW parser exists, but can't decode the token
        if let Some(error) = errors.into_iter().next() {
            return Err(error);
        }

        // No JTW parser configured
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
                None, // no timeout
                ShardSelectorInternal::All,
                Auth::new(
                    Access::full("JWT stateful validation"),
                    None,
                    None,
                    AuthType::Internal,
                ),
                HwMeasurementAcc::disposable(),
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
            .is_some_and(|ro_key| ct_eq(ro_key, key))
    }

    /// Check if a key is allowed to write
    #[inline]
    fn can_write(&self, key: &str) -> bool {
        let can_write = self
            .read_write
            .as_ref()
            .is_some_and(|rw_key| ct_eq(rw_key, key));
        let alt_can_write = self
            .alt_read_write
            .as_ref()
            .is_some_and(|alt_rw_key| ct_eq(alt_rw_key, key));
        can_write || alt_can_write
    }
}
