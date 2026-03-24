use std::collections::HashMap;

use actix_web::{HttpResponse, get, web};
use actix_web_validator::Query;
use api::grpc;
use api::grpc::transport_channel_pool::DEFAULT_GRPC_TIMEOUT;
use chrono::{DateTime, Utc};
use collection::operations::verification::new_unchecked_verification_pass;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use storage::audit::AuditConfig;
use storage::audit_reader::{AuditLogQuery, read_local_audit_logs};
use storage::content_manager::errors::StorageError;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use validator::Validate;

use crate::actix::auth::ActixAuth;
use crate::actix::helpers;

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

        let query = AuditLogQuery::new(time_from, time_to, filters.clone(), limit);

        let limit = query.limit;

        // Read local audit logs
        let local_entries = read_local_audit_logs(audit_config, &query).unwrap_or_default();

        // Fan out to remote peers via internal gRPC
        let channel_service = toc.get_channel_service();

        let grpc_filters: HashMap<String, String> = filters;

        let all_peers: Vec<_> = channel_service
            .id_to_address
            .read()
            .keys()
            .copied()
            .collect();

        let timeout = timeout.unwrap_or(DEFAULT_GRPC_TIMEOUT.as_secs());
        let _ = timeout; // reserved for future per-peer timeout

        let mut futures = all_peers
            .into_iter()
            .map(|peer_id| {
                let request = grpc::GetAuditLogRequest {
                    time_from: time_from.map(|dt| dt.to_rfc3339()),
                    time_to: time_to.map(|dt| dt.to_rfc3339()),
                    filters: grpc_filters.clone(),
                    limit: limit as u64,
                };

                async move {
                    let result = channel_service
                        .with_qdrant_client(peer_id, |mut client| {
                            let request = request.clone();
                            async move { client.get_audit_log(request).await }
                        })
                        .await;
                    (peer_id, result)
                }
            })
            .collect::<FuturesUnordered<_>>();

        let mut all_entries: Vec<serde_json::Value> = Vec::new();
        let mut missing_peers: Vec<u64> = Vec::new();

        for entry_str in &local_entries {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(entry_str) {
                all_entries.push(val);
            }
        }

        while let Some((peer_id, result)) = futures.next().await {
            match result {
                Ok(response) => {
                    for entry_str in &response.into_inner().entries {
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(entry_str) {
                            all_entries.push(val);
                        }
                    }
                }
                Err(err) => {
                    log::error!("Failed to fetch audit logs from peer {peer_id}: {err:#?}");
                    missing_peers.push(peer_id);
                }
            }
        }

        // Sort by timestamp descending (newest first)
        all_entries.sort_by(|a, b| {
            let ts_a = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
            let ts_b = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
            ts_b.cmp(ts_a)
        });

        all_entries.truncate(limit);

        Ok(AuditLogResponse {
            entries: all_entries,
            missing_peers,
        })
    })
    .await
}

pub fn config_audit_api(cfg: &mut web::ServiceConfig) {
    cfg.service(get_audit_logs);
}
