use actix_web::rt::time::Instant;
use actix_web::{get, web, Responder};
use collection::operations::types::IssuesReport;

use crate::actix::helpers::process_response;

#[get("/issues")]
async fn get_issues() -> impl Responder {
    let timing = Instant::now();
    let response = Ok(IssuesReport {
        issues: issues::all_issues(),
    });
    process_response(response, timing)
}

// Configure services
pub fn config_issues_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_issues);
}
