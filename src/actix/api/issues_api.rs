use actix_web::{Responder, delete, get, web};
use collection::operations::types::IssuesReport;
use storage::rbac::{Access, AccessRequirements};

use crate::actix::auth::ActixAccess;

#[get("/issues")]
async fn get_issues(ActixAccess(access): ActixAccess) -> impl Responder {
    crate::actix::helpers::time(async move {
        match access {
            Access::Global(_) => Ok(IssuesReport {
                issues: issues::all_issues(),
            }),
            Access::Collection(collection_access_list) => {
                let requirements = AccessRequirements::new().whole();

                let mut allowed_issues = Vec::new();
                for collection_name in collection_access_list.meeting_requirements(requirements) {
                    let collection_issues = issues::all_collection_issues(collection_name);
                    allowed_issues.extend(collection_issues);
                }

                Ok(IssuesReport {
                    issues: allowed_issues,
                })
            }
        }
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
