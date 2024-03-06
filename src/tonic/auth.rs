use actix_web_httpauth::headers::authorization::{Bearer, Scheme};
use rbac::jwt::Claims;
use rbac::JwtParser;
use tonic::Status;
use tower::filter::{FilterLayer, Predicate};

use crate::common::auth::AuthKeys;
use crate::common::strings::ct_eq;

type Request = tonic::codegen::http::Request<tonic::transport::Body>;

const READ_ONLY_RPC_PATHS: [&str; 14] = [
    "/qdrant.Collections/CollectionExists",
    "/qdrant.Collections/List",
    "/qdrant.Collections/Get",
    "/qdrant.Points/Scroll",
    "/qdrant.Points/Get",
    "/qdrant.Points/Count",
    "/qdrant.Points/Search",
    "/qdrant.Points/SearchGroups",
    "/qdrant.Points/SearchBatch",
    "/qdrant.Points/Recommend",
    "/qdrant.Points/RecommendGroups",
    "/qdrant.Points/RecommendBatch",
    "/qdrant.Points/Discover",
    "/qdrant.Points/DiscoverBatch",
];

fn is_read_only<R>(req: &tonic::codegen::http::Request<R>) -> bool {
    let uri_path = req.uri().path();
    READ_ONLY_RPC_PATHS
        .iter()
        .any(|ro_uri_path| ct_eq(uri_path, ro_uri_path))
}

fn is_allowed(auth_keys: &AuthKeys, key: &str, request: &Request) -> bool {
    auth_keys.can_write(key) || (is_read_only(request) && auth_keys.can_read(key))
}

#[derive(Clone)]
pub struct AuthMiddleware {
    auth_keys: AuthKeys,
    jwt_decoder: Option<JwtParser>,
}

impl AuthMiddleware {
    pub fn new_layer(auth_keys: AuthKeys) -> FilterLayer<Self> {
        let jwt_decoder = auth_keys.rw_key().map(JwtParser::new);

        FilterLayer::new(Self {
            auth_keys,
            jwt_decoder,
        })
    }
}

impl Predicate<Request> for AuthMiddleware {
    type Request = Request;

    fn check(&mut self, mut request: Self::Request) -> Result<Self::Request, tower::BoxError> {
        // Grab API key from "api-key" header
        let api_key = request
            .headers()
            .get("api-key")
            .and_then(|key| key.to_str().ok())
            .map(|key| key.to_string());

        if let Some(key) = api_key {
            if is_allowed(&self.auth_keys, &key, &request) {
                return Ok(request);
            }

            return Err(Box::new(Status::permission_denied("Invalid api-key")));
        }

        // Grab "authorization" bearer token
        let bearer =
            // Request header
            request.headers().get("authorization")
                .and_then(|auth| {
                    Bearer::parse(auth).ok()
                });

        if let Some(bearer) = bearer {
            // Check if the bearer token is a valid API key
            if is_allowed(&self.auth_keys, bearer.token(), &request) {
                return Ok(request);
            }

            // Otherwise, if read-write key is set...
            if self.auth_keys.rw_key().is_none() {
                return Err(Box::new(Status::permission_denied(
                    "Invalid bearer api-key",
                )));
            }

            // ...check if the bearer token is a valid JWT
            if let Some(jwt_decoder) = &self.jwt_decoder {
                let claims = jwt_decoder.decode(bearer.token()).map_err(|e| {
                    Box::new(Status::permission_denied(format!(
                        "Invalid bearer token: {}",
                        e
                    )))
                })?;

                check_write_access(&claims, &request)?;

                let _previous = request.extensions_mut().insert(claims);

                debug_assert!(
                    _previous.is_none(),
                    "Previous claims should not exist in the request"
                );

                return Ok(request);
            }
        }

        Err(Box::new(Status::permission_denied(
            "Must provide an api-key or an authorization bearer token",
        )))
    }
}

fn check_write_access(claims: &Claims, request: &Request) -> Result<(), tower::BoxError> {
    let write_access = claims.w.unwrap_or(false);

    if !write_access && !is_read_only(request) {
        return Err(Box::new(Status::permission_denied(
            "Read-only access is not allowed",
        )));
    }

    Ok(())
}
