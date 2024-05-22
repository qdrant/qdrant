use actix_web::{get, patch, web, Responder};
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;
use crate::common::debug::{DebugConfig, DebugState};

#[get("/debug")]
async fn get_debug_config(
    ActixAccess(access): ActixAccess,
    debug_state: web::Data<DebugState>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        Ok(debug_state.get_config())
    })
    .await
}

#[patch("/debug")]
async fn update_debug_config(
    ActixAccess(access): ActixAccess,
    debug_state: web::Data<DebugState>,
    new_debug_config: web::Json<DebugConfig>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        Ok(debug_state.apply_config_patch(new_debug_config.into_inner()))
    })
    .await
}

// Configure services
pub fn config_debug_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debug_config);
    cfg.service(update_debug_config);
}
