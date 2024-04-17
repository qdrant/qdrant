use actix_web::rt::time::Instant;
use actix_web::{get, web, Responder};
use collection::operations::types::IssuesReport;
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;
use crate::actix::helpers::process_response;

#[get("/issues")]
async fn get_issues(ActixAccess(access): ActixAccess) -> impl Responder {
    let timing = Instant::now();
    let response = access
        .check_global_access(AccessRequirements::new().manage())
        .map(|_| IssuesReport {
            issues: issues::all_issues(),
        });
    process_response(response, timing)
}

// Configure services
pub fn config_issues_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_issues);
}
