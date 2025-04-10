use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tonic::body::BoxBody;
use tonic::transport::Body;
use tonic::{Request, Response, Status};
use tower::{Layer, Service};

/// Middleware that rejects write operations when in read-only mode
#[derive(Clone)]
pub struct ReadOnlyMiddleware<S> {
    inner: S,
    read_only: bool,
}

impl<S> ReadOnlyMiddleware<S> {
    pub fn new(inner: S, read_only: bool) -> Self {
        Self { inner, read_only }
    }
}

impl<S> Service<Request<Body>> for ReadOnlyMiddleware<S>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if self.read_only && is_write_operation(&request) {
            let response = Response::new(BoxBody::new(Body::from(
                Status::permission_denied("Write operations are not allowed in read-only mode")
                    .to_http(),
            )));
            return Box::pin(async move { Ok(response) });
        }

        let future = self.inner.call(request);
        Box::pin(async move { future.await })
    }
}

/// Layer that applies the read-only middleware
#[derive(Clone)]
pub struct ReadOnlyLayer {
    read_only: bool,
}

impl ReadOnlyLayer {
    pub fn new(read_only: bool) -> Self {
        Self { read_only }
    }
}

impl<S> Layer<S> for ReadOnlyLayer {
    type Service = ReadOnlyMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        ReadOnlyMiddleware::new(service, self.read_only)
    }
}

fn is_write_operation<ReqBody>(req: &Request<ReqBody>) -> bool {
    // Check if the request is a write operation based on the gRPC method
    let path = req.uri().path();
    // List of write operations based on gRPC service definitions
    matches!(
        path,
        // Collections API
        "/qdrant.Collections/CreateCollection" |
        "/qdrant.Collections/UpdateCollection" |
        "/qdrant.Collections/DeleteCollection" |
        // Points API
        "/qdrant.Points/Upsert" |
        "/qdrant.Points/Delete" |
        "/qdrant.Points/SetPayload" |
        "/qdrant.Points/DeletePayload" |
        "/qdrant.Points/ClearPayload" |
        "/qdrant.Points/UpdateVectors" |
        "/qdrant.Points/DeleteVectors" |
        // Snapshots API
        "/qdrant.Snapshots/CreateFullSnapshot" |
        "/qdrant.Snapshots/CreateSnapshot" |
        "/qdrant.Snapshots/DeleteSnapshot" |
        "/qdrant.Snapshots/DeleteFullSnapshot" |
        // Internal APIs
        "/qdrant.QdrantInternal/GetConsensusCommit" |
        "/qdrant.QdrantInternal/WaitOnConsensusCommit"
    )
}
