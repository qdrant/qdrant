use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{Error, HttpResponse};
use futures::future::{Ready, ok};
use storage::content_manager::toc::TableOfContent;

pub struct ReadOnlyMiddleware {
    is_read_only: bool,
}

impl ReadOnlyMiddleware {
    pub fn new(is_read_only: bool) -> Self {
        Self { is_read_only }
    }
}

impl<S, B> Transform<S, ServiceRequest> for ReadOnlyMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ReadOnlyMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(ReadOnlyMiddlewareService {
            service,
            is_read_only: self.is_read_only,
        })
    }
}

pub struct ReadOnlyMiddlewareService<S> {
    service: S,
    is_read_only: bool,
}

impl<S, B> Service<ServiceRequest> for ReadOnlyMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = S::Future;

    fn poll_ready(
        &self,
        ctx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        if self.is_read_only && is_write_operation(&req) {
            return Box::pin(async move {
                Ok(req.into_response(
                    HttpResponse::Forbidden()
                        .content_type("application/json")
                        .body(r#"{"error": "Instance is in read-only mode"}"#),
                ))
            });
        }
        self.service.call(req)
    }
}

fn is_write_operation(req: &ServiceRequest) -> bool {
    // Check if the request is a write operation based on HTTP method
    matches!(
        req.method(),
        &actix_web::http::Method::POST
            | &actix_web::http::Method::PUT
            | &actix_web::http::Method::DELETE
    )
}
