use actix_web::{get, post, web, Responder};
use schemars::JsonSchema;
use serde::Serialize;
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;
use crate::common::pyroscope_state::PyroscopeState;
use crate::settings::PyroscopeConfig;

#[derive(Serialize, JsonSchema)]
pub struct GetDebugConfigResponse {
    pub pyroscope: Option<PyroscopeConfig>,
}

#[get("/debug")]
async fn get_debug_config(
    ActixAccess(access): ActixAccess,
    _state: web::Data<PyroscopeState>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;

        // let config_guard = state.config.lock().unwrap();
        // let config = config_guard.clone();

        Ok(true)
    })
    .await
}

#[post("/debug")]
async fn update_debug_config(
    ActixAccess(access): ActixAccess,
    state: web::Data<PyroscopeState>,
    new_config: web::Json<PyroscopeConfig>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        state.update_agent(&new_config);

        Ok(true)
    })
    .await
}

// Configure services
pub fn config_debug_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debug_config);
    cfg.service(update_debug_config);
}
