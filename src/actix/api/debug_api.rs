use actix_web::http::header::{ContentDisposition, DispositionParam, DispositionType};
use actix_web::{HttpResponse, Responder, get, patch, web};
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAccess;
use crate::common::debugger::{DebugConfigPatch, DebuggerState};

#[get("/debugger")]
async fn get_debugger_config(
    ActixAccess(access): ActixAccess,
    debugger_state: web::Data<DebuggerState>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        Ok(debugger_state.get_config())
    })
    .await
}

#[patch("/debugger")]
async fn update_debugger_config(
    ActixAccess(access): ActixAccess,
    debugger_state: web::Data<DebuggerState>,
    debug_patch: web::Json<DebugConfigPatch>,
) -> impl Responder {
    crate::actix::helpers::time(async move {
        access.check_global_access(AccessRequirements::new().manage())?;
        Ok(debugger_state.apply_config_patch(debug_patch.into_inner()))
    })
    .await
}

#[get("/debug/pprof/heap")]
async fn debug_pprof_heap(ActixAccess(access): ActixAccess) -> impl Responder {
    if let Err(err) = access.check_global_access(AccessRequirements::new().manage()) {
        return HttpResponse::Forbidden().body(err.to_string());
    }

    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    if !prof_ctl.activated() {
        return HttpResponse::Forbidden().body("heap profiling not activated");
    }

    // Grab pprof sample
    let pprof = match prof_ctl.dump_pprof() {
        Ok(pprof) => pprof,
        Err(err) => {
            return HttpResponse::InternalServerError().body(err.to_string());
        }
    };

    // Attach content disposition header to tell clients what file we're sending
    let content_header = ContentDisposition {
        disposition: DispositionType::Attachment,
        parameters: vec![DispositionParam::Filename("heap.pb.gz".into())],
    };

    HttpResponse::Ok()
        .insert_header(content_header)
        .content_type("application/octet-stream")
        .body(pprof)
}

// Configure services
pub fn config_debugger_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debugger_config)
        .service(update_debugger_config)
        .service(debug_pprof_heap);
}
