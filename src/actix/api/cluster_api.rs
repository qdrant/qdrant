use crate::actix::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::{get, web, Responder};
use std::sync::Arc;
use storage::content_manager::toc::TableOfContent;

#[get("/cluster")]
async fn cluster_status(toc: web::Data<Arc<TableOfContent>>) -> impl Responder {
    let timing = Instant::now();
    let response = toc.cluster_status().await;
    process_response(response, timing)
}

// Configure services
pub fn config_cluster_api(cfg: &mut web::ServiceConfig) {
    cfg.service(cluster_status);
}
