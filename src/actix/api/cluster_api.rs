use crate::actix::helpers::process_response;
use actix_web::rt::time::Instant;
use actix_web::{get, web, Responder};
use std::sync::Arc;
use storage::Dispatcher;

#[get("/cluster")]
async fn cluster_status(dispatcher: web::Data<Arc<Dispatcher>>) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher.cluster_status().await;
    process_response(Ok(response), timing)
}

// Configure services
pub fn config_cluster_api(cfg: &mut web::ServiceConfig) {
    cfg.service(cluster_status);
}
