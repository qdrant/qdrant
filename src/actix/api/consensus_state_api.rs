//! Service API to inspect and fix consensus state on a single peer, without going through consensus.
//!
//! Intended for support, to repair peers whose consensus state diverged from the cluster.

use actix_web::{Responder, get, patch, web};
use serde::Deserialize;
use storage::content_manager::consensus_manager::local_patch::PatchMode;
use storage::content_manager::consensus_manager::local_patch::diff::ConsensusStateDiff;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;

#[get("/cluster/consensus_state")]
async fn get_consensus_state(
    ActixAuth(auth): ActixAuth,
    dispatcher: web::Data<Dispatcher>,
) -> impl Responder {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_consensus_state")?;

        let consensus_state = dispatcher
            .consensus_state()
            .ok_or_else(StorageError::standalone_mode)?
            .0
            .clone();

        tokio::task::spawn_blocking(move || consensus_state.consensus_state_data())
            .await
            .map_err(|err| StorageError::service_error(err.to_string()))
    })
    .await
}

#[derive(Debug, Deserialize)]
struct PatchConsensusStateParams {
    /// Only validate the diff and report changes. Enabled by default.
    #[serde(default = "default_dry_run")]
    dry_run: bool,
}

const fn default_dry_run() -> bool {
    true
}

#[patch("/cluster/consensus_state")]
async fn patch_consensus_state(
    ActixAuth(auth): ActixAuth,
    dispatcher: web::Data<Dispatcher>,
    params: web::Query<PatchConsensusStateParams>,
    diff: web::Json<ConsensusStateDiff>,
) -> impl Responder {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "patch_consensus_state")?;

        let consensus_state = dispatcher
            .consensus_state()
            .ok_or_else(StorageError::standalone_mode)?
            .0
            .clone();

        let PatchConsensusStateParams { dry_run } = params.into_inner();
        let mode = if dry_run {
            PatchMode::DryRun
        } else {
            PatchMode::Apply
        };

        let diff = diff.into_inner();

        tokio::task::spawn_blocking(move || consensus_state.patch_consensus_state(diff, mode))
            .await
            .map_err(|err| StorageError::service_error(err.to_string()))?
    })
    .await
}

pub fn config_consensus_state_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_consensus_state)
        .service(patch_consensus_state);
}
