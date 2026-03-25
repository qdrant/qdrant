use std::collections::HashMap;
use std::time::Duration;

use actix_web::{HttpResponse, post, web};
use actix_web_validator::{Json, Query};
use api::grpc::transport_channel_pool::DEFAULT_GRPC_TIMEOUT;
use chrono::{DateTime, Utc};
use collection::operations::verification::new_unchecked_verification_pass;
use serde::{Deserialize, Serialize};
use storage::audit::{AuditConfig, AuditEvent};
use storage::audit_reader::AuditLogQuery;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;
use crate::common::audit::{AuditLogResult, fetch_cluster_audit_logs};

#[derive(Debug, Deserialize, Validate)]
pub struct AuditLogQueryParams {
    /// Timeout in seconds for cross-peer requests
    #[validate(range(min = 1))]
    pub timeout: Option<u64>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct AuditLogRequest {
    /// ISO-8601 start time (inclusive)
    pub time_from: Option<DateTime<Utc>>,
    /// ISO-8601 end time (exclusive)
    pub time_to: Option<DateTime<Utc>>,
    /// Maximum number of entries to return (default: 100, max: 10000)
    #[validate(range(min = 1, max = 10000))]
    pub limit: Option<usize>,
    /// Key-value filters applied to audit log fields, e.g. `{"method": "upsert_points"}`
    #[serde(default)]
    pub filters: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct AuditLogResponse {
    pub entries: Vec<AuditEvent>,
}

#[post("/audit/logs")]
async fn get_audit_logs(
    dispatcher: web::Data<Dispatcher>,
    audit_config: web::Data<Option<AuditConfig>>,
    ActixAuth(auth): ActixAuth,
    query_params: Query<AuditLogQueryParams>,
    body: Json<AuditLogRequest>,
) -> HttpResponse {
    helpers::time(async move {
        auth.check_global_access(AccessRequirements::new().manage(), "get_audit_logs")?;

        let pass = new_unchecked_verification_pass();
        let toc = dispatcher.toc(&auth, &pass);

        let audit_config =
            audit_config
                .as_ref()
                .as_ref()
                .ok_or_else(|| StorageError::BadRequest {
                    description: "Audit logging is not configured".to_string(),
                })?;

        if !audit_config.enabled {
            return Err(StorageError::BadRequest {
                description: "Audit logging is not enabled".to_string(),
            });
        }

        let AuditLogRequest {
            time_from,
            time_to,
            limit,
            filters,
        } = body.into_inner();

        let query = AuditLogQuery::new(time_from, time_to, filters, limit);

        let timeout = Duration::from_secs(
            query_params
                .timeout
                .unwrap_or(DEFAULT_GRPC_TIMEOUT.as_secs()),
        );

        let AuditLogResult { entries } = fetch_cluster_audit_logs(
            audit_config,
            &query,
            toc.get_channel_service(),
            toc.this_peer_id,
            timeout,
        )
        .await?;

        Ok(AuditLogResponse { entries })
    })
    .await
}

pub fn config_audit_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_audit_logs);
}
