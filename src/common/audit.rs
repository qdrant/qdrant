use std::time::Duration;

use api::grpc;
use collection::shards::channel_service::ChannelService;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use shard::PeerId;
use storage::audit::AuditEvent;
pub use storage::audit::*;
use storage::audit_reader::{AuditLogQuery, read_local_audit_logs};
use storage::content_manager::errors::StorageError;

pub struct AuditLogResult {
    pub entries: Vec<AuditEvent>,
}

/// Fetch audit logs from local node and all remote peers, merge and return newest first.
pub async fn fetch_cluster_audit_logs(
    audit_config: &AuditConfig,
    query: &AuditLogQuery,
    channel_service: &ChannelService,
    this_peer_id: PeerId,
    timeout: Duration,
) -> Result<AuditLogResult, StorageError> {
    let config = audit_config.clone();
    let query_clone = query.clone();
    let local_entries = cancel::blocking::spawn_cancel_on_drop(move |cancel| {
        read_local_audit_logs(&config, &query_clone, &cancel)
    })
    .await
    .map_err(|e| StorageError::service_error(format!("Failed to read local audit logs: {e}")))??;

    let grpc_request = grpc::GetAuditLogRequest {
        time_from: query.time_from.map(|dt| dt.to_rfc3339()),
        time_to: query.time_to.map(|dt| dt.to_rfc3339()),
        filters: query.filters.clone(),
        limit: query.limit as u64,
    };

    let all_peers: Vec<_> = channel_service.other_peers(this_peer_id);

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

    // Each source provides entries already sorted descending (newest first).
    let mut sources: Vec<Vec<AuditEvent>> = Vec::new();

    sources.push(local_entries);

    while let Some((peer_id, result)) = futures.next().await {
        match result {
            Ok(Ok(response)) => {
                let entries: Result<Vec<_>, _> = response
                    .into_inner()
                    .entries
                    .iter()
                    .map(|s| serde_json::from_str::<AuditEvent>(s))
                    .collect();

                sources.push(entries?);
            }
            Ok(Err(err)) => {
                return Err(StorageError::service_error(format!(
                    "Failed to fetch audit logs from peer {peer_id}: {err}"
                )));
            }
            Err(elapsed) => {
                return Err(StorageError::timeout(
                    timeout,
                    format!("Timed out fetching audit logs from peer {peer_id} after {elapsed:?}"),
                ));
            }
        }
    }

    // Each source is pre-sorted descending (newest first). K-way merge
    // picks the newest entry across all heads at each step.
    let entries: Vec<AuditEvent> = sources
        .into_iter()
        .kmerge_by(|a, b| a.timestamp >= b.timestamp)
        .take(query.limit)
        .collect();

    Ok(AuditLogResult { entries })
}
