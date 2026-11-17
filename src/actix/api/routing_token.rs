use std::convert::Infallible;
use std::future::{Ready, ready};

use actix_web::{FromRequest, HttpRequest};
use collection::operations::routing::RoutingToken;

/// HTTP header carrying the optional deterministic read-routing token.
///
/// Clients send a stable value (for example a user or session id); requests with
/// the same value are consistently routed to the same shard replicas, avoiding the
/// "blinking" of results caused by deferred updates. See [`RoutingToken`].
pub const ROUTING_TOKEN_HEADER: &str = api::HTTP_HEADER_ROUTING_TOKEN;

/// Actix extractor for the optional [`RoutingToken`] supplied via the
/// [`ROUTING_TOKEN_HEADER`] HTTP header.
///
/// When the header is present and non-empty, its raw bytes seed the token. When
/// absent or empty, routing falls back to the default (local-preferred, random)
/// behaviour.
pub struct ActixRoutingToken(pub Option<RoutingToken>);

impl FromRequest for ActixRoutingToken {
    type Error = Infallible;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut actix_web::dev::Payload) -> Self::Future {
        ready(Ok(ActixRoutingToken(routing_token_from_request(req))))
    }
}

/// Read the routing token from the request's [`ROUTING_TOKEN_HEADER`], if any.
fn routing_token_from_request(req: &HttpRequest) -> Option<RoutingToken> {
    let bytes = req.headers().get(ROUTING_TOKEN_HEADER)?.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    Some(RoutingToken::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use actix_web::test::TestRequest;

    use super::*;

    async fn extract(req: TestRequest) -> Option<RoutingToken> {
        let (req, mut payload) = req.to_http_parts();
        ActixRoutingToken::from_request(&req, &mut payload)
            .await
            .unwrap()
            .0
    }

    #[actix_web::test]
    async fn extracts_token_from_header() {
        let token =
            extract(TestRequest::default().insert_header((ROUTING_TOKEN_HEADER, "user-42"))).await;
        assert_eq!(token, Some(RoutingToken::from_bytes(b"user-42")));
    }

    #[actix_web::test]
    async fn same_header_yields_same_token() {
        let a = extract(TestRequest::default().insert_header((ROUTING_TOKEN_HEADER, "abc"))).await;
        let b = extract(TestRequest::default().insert_header((ROUTING_TOKEN_HEADER, "abc"))).await;
        assert_eq!(a, b);
        assert!(a.is_some());
    }

    #[actix_web::test]
    async fn missing_header_yields_none() {
        assert_eq!(extract(TestRequest::default()).await, None);
    }

    #[actix_web::test]
    async fn empty_header_yields_none() {
        let token = extract(TestRequest::default().insert_header((ROUTING_TOKEN_HEADER, ""))).await;
        assert_eq!(token, None);
    }
}
