use actix_web::{get, post, web, Responder};
use api::grpc::models::{
    GetDebugConfigResponse,
    PyroscopeConfig,
    // UpdateDebugConfigRequest
};
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;
use crate::common::pyroscope_state::PyroscopeState;

#[get("/debug")]
async fn get_debug_config(
    ActixAccess(access): ActixAccess,
    _state: web::Data<PyroscopeState>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;

        // let config_guard = state.config.lock().unwrap();
        // let config = config_guard.clone();

        Ok(GetDebugConfigResponse { pyroscope: None })
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

        // Get the current agent and stop it
        let mut agent_guard = state.agent.lock().unwrap();
        if let Some(running_agent) = agent_guard.take() {
            let ready_agent = running_agent.stop().unwrap();
            ready_agent.shutdown();
        }

        *agent_guard = Some(PyroscopeState::build_agent(&new_config));

        let mut config = state.config.lock().unwrap();
        *config = new_config.into_inner();

        Ok(true)
    })
    .await
}

// Configure services
pub fn config_debug_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debug_config);
    cfg.service(update_debug_config);
}
