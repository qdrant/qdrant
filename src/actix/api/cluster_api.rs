use std::sync::Arc;

use actix_web::rt::time::Instant;
use actix_web::{get, web, Responder};
use storage::Dispatcher;

use crate::actix::helpers::process_response;

#[get("/cluster")]
async fn cluster_status(dispatcher: web::Data<Arc<Dispatcher>>) -> impl Responder {
    let timing = Instant::now();
    let response = dispatcher.cluster_status();
    process_response(Ok(response), timing)
}

// Configure services
pub fn config_cluster_api(cfg: &mut web::ServiceConfig) {
    cfg.service(cluster_status);
}
