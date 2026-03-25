use std::collections::HashMap;
use std::time::Duration;

use actix_web::{HttpResponse, get, web};
use actix_web_validator::Query;
use api::grpc::transport_channel_pool::DEFAULT_GRPC_TIMEOUT;
use chrono::{DateTime, Utc};
use collection::operations::verification::new_unchecked_verification_pass;
use serde::{Deserialize, Serialize};
use storage::audit::AuditConfig;
use storage::audit_reader::AuditLogQuery;
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;
use crate::common::audit::{AuditLogResult, fetch_cluster_audit_logs};

#[derive(Debug, Deserialize, Validate)]
pub struct AuditLogParams {
    /// ISO-8601 start time (inclusive)
    pub time_from: Option<DateTime<Utc>>,
    /// ISO-8601 end time (exclusive)
    pub time_to: Option<DateTime<Utc>>,
    /// Maximum number of entries to return (default: 100, max: 10000)
    #[validate(range(min = 1, max = 10000))]
    pub limit: Option<usize>,
    /// Timeout in seconds for cross-peer requests
    #[validate(range(min = 1))]
    pub timeout: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct AuditLogResponse {
    pub entries: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub missing_peers: Vec<u64>,
}

/// Parse `filter=key=value` pairs from the raw query string.
///
/// Supports multiple `filter` parameters, e.g.:
/// `?filter=method=upsert_points&filter=result=ok`
fn parse_filters(query_string: &str) -> HashMap<String, String> {
    let mut filters = HashMap::new();
    for part in query_string.split('&') {
        if let Some(value) = part.strip_prefix("filter=") {
            let decoded = urlencoding::decode(value).unwrap_or_default();
            if let Some((k, v)) = decoded.split_once('=') {
                filters.insert(k.to_string(), v.to_string());
            }
        }
    }
    filters
}

#[get("/audit/logs")]
async fn get_audit_logs(
    dispatcher: web::Data<Dispatcher>,
    audit_config: web::Data<Option<AuditConfig>>,
    ActixAuth(auth): ActixAuth,
    params: Query<AuditLogParams>,
    req: actix_web::HttpRequest,
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

        let filters = parse_filters(req.query_string());

        let AuditLogParams {
            time_from,
            time_to,
            limit,
            timeout,
        } = params.into_inner();

        let query = AuditLogQuery::new(time_from, time_to, filters, limit);

        let timeout = Duration::from_secs(timeout.unwrap_or(DEFAULT_GRPC_TIMEOUT.as_secs()));

        let AuditLogResult {
            entries,
            missing_peers,
        } = fetch_cluster_audit_logs(
            audit_config,
            &query,
            toc.get_channel_service(),
            toc.this_peer_id,
            timeout,
        )
        .await?;

        Ok(AuditLogResponse {
            entries,
            missing_peers,
        })
    })
    .await
}

pub fn config_audit_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_audit_logs);
}
