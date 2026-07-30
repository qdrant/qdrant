use actix_web::{HttpResponse, get, put, web};
use actix_web_validator::{Json, Query};
use collection::operations::verification::new_unchecked_verification_pass;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::dispatcher::Dispatcher;
use storage::quota::QuotaConfig;
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct QuotaParams {
    /// Wait until the new quota config is confirmed by consensus on this peer.
    #[serde(default)]
    pub wait: bool,
}

#[get("/quotas")]
async fn get_quotas(dispatcher: web::Data<Dispatcher>, ActixAuth(auth): ActixAuth) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new(), "get_quotas")?;

        Ok(dispatcher.toc(&auth, &pass).quota_manager().status())
    })
    .await
}

#[put("/quotas")]
async fn update_quotas(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    Query(params): Query<QuotaParams>,
    Json(config): Json<QuotaConfig>,
) -> HttpResponse {
    // Not a collection level request.
    let pass = new_unchecked_verification_pass();

    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "update_quotas")?;

        dispatcher
            .toc(&auth, &pass)
            .update_quota_config(config, params.wait)
            .await?;

        Ok(true)
    })
    .await
}

pub fn config_quota_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_quotas).service(update_quotas);
}
