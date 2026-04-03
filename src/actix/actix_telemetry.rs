use std::future::{Ready, ready};
use std::sync::Arc;

use actix_web::Error;
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use futures_util::future::LocalBoxFuture;
use parking_lot::Mutex;

use crate::common::telemetry_ops::requests_telemetry::{
    ActixTelemetryCollector, ActixWorkerTelemetryCollector,
};

pub struct ActixTelemetryService<S> {
    service: S,
    telemetry_data: Arc<Mutex<ActixWorkerTelemetryCollector>>,
}

pub struct ActixTelemetryTransform {
    telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>,
}

/// Actix telemetry service. It hooks every request and looks into response status code.
///
/// More about actix service with similar example
/// <https://actix.rs/docs/middleware/>
impl<S, B> Service<ServiceRequest> for ActixTelemetryService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    actix_web::dev::forward_ready!(service);

    fn call(&self, request: ServiceRequest) -> Self::Future {
        let match_pattern = request
            .match_pattern()
            .unwrap_or_else(|| "unknown".to_owned());

        let request_key = format!("{} {}", request.method(), match_pattern);
        let future = self.service.call(request);
        let telemetry_data = self.telemetry_data.clone();
        Box::pin(async move {
            let instant = std::time::Instant::now();
            let response = future.await?;
            let status = response.response().status().as_u16();

            let collection_name = response
                .request()
                .match_info()
                .get("collection_name")
                .map(|s| s.to_string());

            telemetry_data
                .lock()
                .add_response(request_key, status, instant, collection_name);
            Ok(response)
        })
    }
}

impl ActixTelemetryTransform {
    pub fn new(telemetry_collector: Arc<Mutex<ActixTelemetryCollector>>) -> Self {
        Self {
            telemetry_collector,
        }
    }
}

/// Actix telemetry transform. It's a builder for an actix service
///
/// More about actix transform with similar example
/// <https://actix.rs/docs/middleware/>
impl<S, B> Transform<S, ServiceRequest> for ActixTelemetryTransform
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = ActixTelemetryService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ActixTelemetryService {
            service,
            telemetry_data: self
                .telemetry_collector
                .lock()
                .create_web_worker_telemetry(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::actix::api::query_api::THIS_FILE;

    /// Recursively collect all `.rs` files under `dir`.
    fn collect_rs_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in fs_err::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    #[test]
    fn test_collection_routes_use_collection_name_param() {
        let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"));
        let file_path = manifest_path.join(THIS_FILE);
        let actix_dir = file_path.parent().unwrap();

        let mut rs_files = Vec::new();
        collect_rs_files(actix_dir, &mut rs_files);

        let mut bad_lines = Vec::new();
        for path in &rs_files {
            let contents = fs_err::read_to_string(path).unwrap();
            for (line_no, line) in contents.lines().enumerate() {
                let trimmed = line.trim();
                // Only check route attribute macros like #[get("/collections/{...")]
                if trimmed.starts_with("#[")
                    && trimmed.contains("/collections/{")
                    && !trimmed.contains("{collection_name}")
                {
                    bad_lines.push(format!("{}:{}: {}", path.display(), line_no + 1, trimmed));
                }
            }
        }

        assert!(
            bad_lines.is_empty(),
            "All collection routes must use {{collection_name}} as path parameter:\n{}",
            bad_lines.join("\n"),
        );
    }
}
