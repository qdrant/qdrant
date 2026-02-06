use actix_web::{Responder, get, patch, web};
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAuth;
use crate::common::debugger::{DebugConfigPatch, DebuggerState};

#[get("/debugger")]
async fn get_debugger_config(
    ActixAuth(auth): ActixAuth,
    debugger_state: web::Data<DebuggerState>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_debugger_config")?;
        Ok(debugger_state.get_config())
    })
    .await
}

#[patch("/debugger")]
async fn update_debugger_config(
    ActixAuth(auth): ActixAuth,
    debugger_state: web::Data<DebuggerState>,
    debug_patch: web::Json<DebugConfigPatch>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "update_debugger_config")?;
        Ok(debugger_state.apply_config_patch(debug_patch.into_inner()))
    })
    .await
}

// Configure services
pub fn config_debugger_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debugger_config);
    cfg.service(update_debugger_config);
}
