use actix_web::{HttpResponse, get, put, web};
use actix_web_validator::{Json, Query};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::dispatcher::Dispatcher;
use storage::quota::QuotaConfig;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;
use crate::common::quotas::{get_quota_status, update_quota_status};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct QuotaParams {
    /// Wait until the new quota config is confirmed by consensus on this peer.
    #[serde(default)]
    pub wait: bool,
}

#[get("/quotas")]
async fn get_quotas(dispatcher: web::Data<Dispatcher>, ActixAuth(auth): ActixAuth) -> HttpResponse {
    helpers::time(async move { get_quota_status(&dispatcher, &auth).await }).await
}

#[put("/quotas")]
async fn update_quotas(
    dispatcher: web::Data<Dispatcher>,
    ActixAuth(auth): ActixAuth,
    Query(params): Query<QuotaParams>,
    Json(config): Json<QuotaConfig>,
) -> HttpResponse {
    helpers::time(async move {
        update_quota_status(&dispatcher, &auth, config, params.wait).await?;
        Ok(true)
    })
    .await
}

pub fn config_quota_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_quotas).service(update_quotas);
}
