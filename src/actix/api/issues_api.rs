use actix_web::{delete, get, web, Responder};
use collection::operations::types::IssuesReport;
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;

#[get("/issues")]
async fn get_issues(ActixAccess(access): ActixAccess) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        Ok(IssuesReport {
            issues: issues::all_issues(),
        })
    })
    .await
}

#[delete("/issues")]
async fn clear_issues(ActixAccess(access): ActixAccess) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        issues::clear();
        Ok(true)
    })
    .await
}

// Configure services
pub fn config_issues_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_issues);
    cfg.service(clear_issues);
}
