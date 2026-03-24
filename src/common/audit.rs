use std::time::Duration;

use api::grpc;
use collection::shards::channel_service::ChannelService;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
pub use storage::audit::*;
use storage::audit_reader::{AuditLogQuery, read_local_audit_logs};
use storage::content_manager::errors::StorageError;

pub struct AuditLogResult {
    pub entries: Vec<serde_json::Value>,
    pub missing_peers: Vec<u64>,
}

/// Fetch audit logs from local node and all remote peers, merge, sort, and truncate.
pub async fn fetch_cluster_audit_logs(
    audit_config: &AuditConfig,
    query: &AuditLogQuery,
    channel_service: &ChannelService,
    timeout: Duration,
) -> Result<AuditLogResult, StorageError> {
    let config = audit_config.clone();
    let query_clone = query.clone();
    let local_entries = cancel::blocking::spawn_cancel_on_drop(move |cancel| {
        read_local_audit_logs(&config, &query_clone, &cancel)
    })
    .await
    .map_err(|e| StorageError::service_error(format!("Failed to read local audit logs: {e}")))?
    .unwrap_or_default();

    let grpc_request = grpc::GetAuditLogRequest {
        time_from: query.time_from.map(|dt| dt.to_rfc3339()),
        time_to: query.time_to.map(|dt| dt.to_rfc3339()),
        filters: query.filters.clone(),
        limit: query.limit as u64,
    };

    let all_peers: Vec<_> = channel_service
        .id_to_address
        .read()
        .keys()
        .copied()
        .collect();

    let mut futures = all_peers
        .into_iter()
        .map(|peer_id| {
            let request = grpc_request.clone();
            async move {
                let result = tokio::time::timeout(
                    timeout,
                    channel_service.with_qdrant_client(peer_id, |mut client| {
                        let request = request.clone();
                        async move { client.get_audit_log(request).await }
                    }),
                )
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
            Ok(Ok(response)) => {
                for entry_str in &response.into_inner().entries {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(entry_str) {
                        all_entries.push(val);
                    }
                }
            }
            Ok(Err(err)) => {
                log::error!("Failed to fetch audit logs from peer {peer_id}: {err:#?}");
                missing_peers.push(peer_id);
            }
            Err(_) => {
                log::error!("Timed out fetching audit logs from peer {peer_id}");
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

    all_entries.truncate(query.limit);

    Ok(AuditLogResult {
        entries: all_entries,
        missing_peers,
    })
}
