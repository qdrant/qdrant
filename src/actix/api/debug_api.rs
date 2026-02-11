use actix_web::{Responder, get, patch, web};
use collection::operations::verification;
use collection::shards::shard::ShardId;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;
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

#[get("/collections/{collection}/shards/{shard}/recovery_point")]
async fn get_shard_recovery_point(
    dispatcher: web::Data<Dispatcher>,
    path: web::Path<(String, ShardId)>,
    ActixAuth(auth): ActixAuth,
) -> impl Responder {
    helpers::time(async move {
        let (collection, shard) = path.into_inner();

        let pass = verification::new_unchecked_verification_pass();
        let collection_pass = auth.check_collection_access(
            &collection,
            AccessRequirements::new().extras(),
            "get_shard_recovery_point",
        )?;

        let recovery_point: Vec<_> = dispatcher
            .toc(&auth, &pass)
            .get_collection(&collection_pass)
            .await?
            .shard_recovery_point(shard)
            .await?
            .iter_as_clock_tags()
            .collect();

        Ok(recovery_point)
    })
    .await
}

// Configure services
pub fn config_debugger_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_debugger_config)
        .service(update_debugger_config)
        .service(get_shard_recovery_point);
}
