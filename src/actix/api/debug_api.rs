use actix_web::{get, post, web, Responder};
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

        let _ = debug_state;

        #[cfg(target_os = "linux")]
        {
            let pyroscope_state_guard = debug_state.pyroscope.lock().unwrap();
            let pyroscope_config = pyroscope_state_guard.as_ref().map(|s| s.config.clone());
            Ok(DebugConfig {
                pyroscope: pyroscope_config,
            })
        }

        #[cfg(not(target_os = "linux"))]
        Ok(DebugConfig { pyroscope: None });
    })
    .await
}

#[post("/debug")]
async fn update_debug_config(
    ActixAccess(access): ActixAccess,
    debug_state: web::Data<DebugState>,
    new_debug_config: web::Json<DebugConfig>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        #[cfg(target_os = "linux")]
        {
            match new_debug_config.pyroscope.clone() {
                Some(pyro_config) => debug_state.update_pyroscope_config(pyro_config),
                None => {
                    let mut pyroscope_guard = debug_state.pyroscope.lock().unwrap();
                    *pyroscope_guard = None; // TODO: Find out if this actually calls drop (shutdown agent) or not?
                }
            }
            Ok(true)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = debug_state; // Ignore new_config on non-linux OS
            let _ = new_debug_config;
            Ok(false)
        }
    })
    .await
}

// Configure services
pub fn config_debug_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debug_config);
    cfg.service(update_debug_config);
}
