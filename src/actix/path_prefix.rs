use actix_web::dev::ServiceRequest;
use actix_web::http::Uri;
use actix_web::http::uri::PathAndQuery;

/// Remove `prefix` from the request path, as a reverse proxy that strips it would.
///
/// Only applies when the path is the prefix itself or continues with `/` after it, and does not
/// already resolve to a route. Other requests are left as they are.
pub fn strip_path_prefix(req: &mut ServiceRequest, prefix: &str) {
    let Some(uri) = without_prefix(req.uri(), prefix) else {
        return;
    };
    if req.match_pattern().is_some() {
        return;
    }
    req.match_info_mut().get_mut().update(&uri);
    req.head_mut().uri = uri;
}

fn without_prefix(uri: &Uri, prefix: &str) -> Option<Uri> {
    let path = match uri.path().strip_prefix(prefix)? {
        "" => "/",
        path if path.starts_with('/') => path,
        _ => return None,
    };
    let path_and_query = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    };

    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(PathAndQuery::try_from(path_and_query).ok()?);
    Uri::from_parts(parts).ok()
}

#[cfg(test)]
mod tests {
    use actix_web::middleware::{Next, from_fn};
    use actix_web::test::{self, TestRequest};
    use actix_web::{App, HttpMessage, HttpRequest, web};

    use super::*;

    /// Responds with the request URI and the route pattern seen by middlewares, such as auth.
    async fn call(uri: &str) -> String {
        let srv = test::init_service(
            App::new()
                .wrap(from_fn(|req: ServiceRequest, next: Next<_>| {
                    let pattern = req.match_pattern().unwrap_or_default();
                    req.extensions_mut().insert(pattern);
                    async move { next.call(req).await }
                }))
                .wrap(from_fn(|mut req: ServiceRequest, next: Next<_>| {
                    strip_path_prefix(&mut req, "/qdrant");
                    async move { next.call(req).await }
                }))
                .route("/", web::get().to(respond))
                .route("/qdrant/metrics", web::get().to(respond))
                .route("/telemetry", web::get().to(respond))
                .route("/collections/{name}", web::get().to(respond)),
        )
        .await;

        let res = test::call_service(&srv, TestRequest::with_uri(uri).to_request()).await;
        if !res.status().is_success() {
            return res.status().as_str().to_string();
        }
        String::from_utf8(test::read_body(res).await.to_vec()).unwrap()
    }

    async fn respond(req: HttpRequest) -> String {
        let pattern = req.extensions().get::<String>().cloned().unwrap();
        format!("{} {pattern}", req.uri())
    }

    #[actix_web::test]
    async fn test_strip_path_prefix() {
        // Paths under the prefix
        assert_eq!(call("/qdrant").await, "/ /");
        assert_eq!(call("/qdrant/").await, "/ /");
        assert_eq!(
            call("/qdrant/telemetry?details_level=2").await,
            "/telemetry?details_level=2 /telemetry",
        );
        assert_eq!(
            call("/qdrant/collections/test").await,
            "/collections/test /collections/{name}",
        );

        // Paths that already resolve to a route take precedence
        assert_eq!(
            call("/qdrant/metrics").await,
            "/qdrant/metrics /qdrant/metrics"
        );

        // Paths without the prefix
        assert_eq!(call("/").await, "/ /");
        assert_eq!(
            call("/collections/test").await,
            "/collections/test /collections/{name}",
        );
        assert_eq!(call("/qdrantx/telemetry").await, "404");
        assert_eq!(call("/x/qdrant/telemetry").await, "404");
    }
}
