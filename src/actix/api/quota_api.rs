use actix_web::{HttpResponse, get, put, web};
use actix_web_validator::{Json, Query};
use schemars::JsonSchema;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use storage::dispatcher::Dispatcher;
use storage::quota::QuotaConfig;
use validator::{Validate, ValidationErrors};

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;
use crate::common::quotas::{get_quota_status, update_quota_status};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
pub struct QuotaParams {
    /// Wait until the new quota config is confirmed by consensus on this peer.
    #[serde(default)]
    pub wait: bool,
}

/// Body of `PUT /quotas`: a [`QuotaConfig`], or `None` when the body does not
/// set `enabled` (or its alias `enable`).
///
/// `enabled` defaults to `false` in `QuotaConfig`, so a body without it, such
/// as `{}`, would otherwise replace the current config with a disabled one
/// across the cluster. Such a request is a no-op instead.
#[derive(Debug)]
struct UpdateQuotas(Option<QuotaConfig>);

impl<'de> Deserialize<'de> for UpdateQuotas {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let body = serde_json::Map::deserialize(deserializer)?;
        if !body.contains_key("enabled") && !body.contains_key("enable") {
            return Ok(Self(None));
        }
        QuotaConfig::deserialize(serde_json::Value::Object(body))
            .map(|config| Self(Some(config)))
            .map_err(D::Error::custom)
    }
}

impl Validate for UpdateQuotas {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match &self.0 {
            Some(config) => config.validate(),
            None => Ok(()),
        }
    }
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
    Json(UpdateQuotas(config)): Json<UpdateQuotas>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_body_is_noop() {
        let UpdateQuotas(config) = serde_json::from_str("{}").unwrap();
        assert_eq!(config, None);
    }

    #[test]
    fn body_without_enabled_is_noop() {
        let UpdateQuotas(config) =
            serde_json::from_str(r#"{"max_disk_usage_percent": 90}"#).unwrap();
        assert_eq!(config, None);
    }

    #[test]
    fn accepts_enable_as_alias_of_enabled() {
        let UpdateQuotas(config) =
            serde_json::from_str(r#"{"enable": true, "max_disk_usage_percent": 90}"#).unwrap();
        let config = config.unwrap();
        assert!(config.enabled);
        assert_eq!(config.max_disk_usage_percent, Some(90));
    }

    #[test]
    fn keeps_existing_behavior_when_enabled_is_set() {
        let UpdateQuotas(config) = serde_json::from_str(
            r#"{"enabled": false, "max_disk_usage_percent": 90, "unknown": 1}"#,
        )
        .unwrap();
        let config = config.unwrap();
        assert!(!config.enabled);
        assert_eq!(config.max_disk_usage_percent, Some(90));
    }
}
