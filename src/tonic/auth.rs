use rbac::jwt::Claims;
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

#[derive(Clone)]
pub struct AuthMiddleware {
    auth_keys: AuthKeys,
}

impl AuthMiddleware {
    pub fn new_layer(auth_keys: AuthKeys) -> FilterLayer<Self> {
        FilterLayer::new(Self { auth_keys })
    }
}

impl Predicate<Request> for AuthMiddleware {
    type Request = Request;

    fn check(&mut self, mut req: Self::Request) -> Result<Self::Request, tower::BoxError> {
        match self.auth_keys.validate_request(
            |key| req.headers().get(key).and_then(|val| val.to_str().ok()),
            is_read_only(&req),
        ) {
            Ok(claims) => {
                if let Some(claims) = claims {
                    let _previous = req.extensions_mut().insert::<Claims>(claims);
                    debug_assert!(
                        _previous.is_none(),
                        "Previous claims should not exist in the request"
                    );
                }
                Ok(req)
            }
            Err(e) => Err(Box::new(Status::permission_denied(e))),
        }
    }
}

pub fn extract_claims<R>(req: &mut tonic::Request<R>) -> Option<Claims> {
    req.extensions_mut().remove::<Claims>()
}

fn is_read_only<R>(req: &tonic::codegen::http::Request<R>) -> bool {
    let uri_path = req.uri().path();
    READ_ONLY_RPC_PATHS
        .iter()
        .any(|ro_uri_path| ct_eq(uri_path, ro_uri_path))
}
