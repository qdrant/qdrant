use actix_web::{get, patch, web, Responder};
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;
use crate::common::debug::{DebugConfigPatch, DebugState};

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
    debug_patch: web::Json<DebugConfigPatch>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;

        log::info!("===== Debug patch {:?} =====", debug_patch);

        Ok(debug_state.apply_config_patch(debug_patch.into_inner()))
    })
    .await
}

// Configure services
pub fn config_debug_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debug_config);
    cfg.service(update_debug_config);
}
