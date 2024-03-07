use actix_web_httpauth::headers::authorization::{Bearer, Scheme};
use tonic::Status;
use tower::filter::{FilterLayer, Predicate};

use crate::common::auth::AuthKeys;
use crate::common::strings::ct_eq;

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

#[derive(Clone)]
pub struct ApiKeyMiddleware {
    auth_keys: AuthKeys,
}

impl ApiKeyMiddleware {
    pub fn new_layer(auth_keys: AuthKeys) -> FilterLayer<Self> {
        FilterLayer::new(Self { auth_keys })
    }
}

impl Predicate<tonic::codegen::http::Request<tonic::transport::Body>> for ApiKeyMiddleware {
    type Request = tonic::codegen::http::Request<tonic::transport::Body>;

    fn check(&mut self, request: Self::Request) -> Result<Self::Request, tower::BoxError> {
        // Grab API key from request
        let key =
            // Request header
            request.headers().get("api-key").and_then(|key| key.to_str().ok()).map(|key| key.to_string())
                // Fall back to authentication header with bearer token
                .or_else(|| {
                    request.headers().get("authorization")
                        .and_then(|auth| {
                            Bearer::parse(auth).ok().map(|bearer| bearer.token().into())
                        })
                });

        if let Some(key) = key {
            let is_allowed = self.auth_keys.can_write(&key)
                || (is_read_only(&request) && self.auth_keys.can_read(&key));
            if is_allowed {
                return Ok(request);
            }
        }

        Err(Box::new(Status::permission_denied("Invalid api-key")))
    }
}

fn is_read_only<R>(req: &tonic::codegen::http::Request<R>) -> bool {
    let uri_path = req.uri().path();
    READ_ONLY_RPC_PATHS
        .iter()
        .any(|ro_uri_path| ct_eq(uri_path, ro_uri_path))
}
