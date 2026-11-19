//! Remote indexing, server side: the client that submits tasks (§4.8), and its registration
//! as the process-wide `RemoteIndexer` (§4.1).
//!
//! Core always submits with gRPC `IndexingService.SubmitTask` to the configured URL. Whether
//! an indexer or a queue in front of a pool of indexers receives it is up to the receiver.

use std::path::{Component, Path};
use std::sync::{Arc, mpsc};
use std::time::{Duration, UNIX_EPOCH};

use api::grpc::qdrant::indexing_service_client::IndexingServiceClient;
use api::grpc::qdrant::{
    CancelIndexingTaskRequest, IndexingTaskRefusal, IndexingTaskSubmission,
    SubmitIndexingTaskResponse,
};
use api::grpc::remote_indexing::index_params_to_grpc;
use segment::index::hnsw_index::remote::{
    OnFailure, RemoteIndexer, RemoteIndexingConfig, SubmitError, TaskSubmission, TaskToken,
    set_remote_indexer,
};
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use crate::settings::{IndexingOnFailure, IndexingServiceConfig, Settings};

/// Header with the entry point API key.
const API_KEY_HEADER: &str = "api-key";

/// Turn remote indexing on, if configured and safe (§4.1). Returns whether it is on.
pub fn init_remote_indexing(settings: &Settings) -> bool {
    let Some(config) = &settings.indexing_service else {
        set_remote_indexer(None);
        return false;
    };

    // The indexer reaches the internal port. Without auth there, it could reach Raft too.
    if settings
        .service
        .api_key
        .as_deref()
        .is_none_or(str::is_empty)
        || !settings.service.enforce_internal_auth()
    {
        log::error!(
            "Remote indexing is configured but disabled: it requires `service.api_key` and \
             `service.enforce_internal_auth`, so that the indexer cannot reach other internal \
             services",
        );
        set_remote_indexer(None);
        return false;
    }

    match RemoteIndexingClient::new(config.clone()) {
        Ok(client) => {
            log::info!(
                "Remote indexing enabled: tasks to {}, advertised as {}, min index size {} bytes",
                config.url,
                config.advertise_url,
                config.min_index_size_bytes,
            );
            set_remote_indexer(Some(Arc::new(client)));
            true
        }
        Err(err) => {
            log::error!("Remote indexing disabled, failed to set up the entry point: {err}");
            set_remote_indexer(None);
            false
        }
    }
}

/// Collection and shard of a segment under construction, from its vector index path:
/// `.../collections/{collection}/{shard}/...`.
fn collection_and_shard(index_path: &Path) -> (String, u32) {
    let components: Vec<_> = index_path
        .components()
        .filter_map(|c| match c {
            Component::Normal(name) => Some(name.to_string_lossy().into_owned()),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => None,
        })
        .collect();
    let Some(pos) = components.iter().rposition(|c| c == "collections") else {
        return (String::new(), 0);
    };
    let collection = components.get(pos + 1).cloned().unwrap_or_default();
    let shard = components
        .get(pos + 2)
        .and_then(|shard| shard.parse().ok())
        .unwrap_or(0);
    (collection, shard)
}

/// Submits tasks to the configured entry point, on a runtime of its own: `submit` is called
/// from optimizer threads, which must not drive the server's runtimes (§4.6).
struct RemoteIndexingClient {
    inner: Arc<ClientInner>,
    runtime: Runtime,
}

struct ClientInner {
    config: IndexingServiceConfig,
    remote_config: RemoteIndexingConfig,
    /// Lazy: the receiver may come up after core.
    channel: Channel,
}

impl RemoteIndexingClient {
    fn new(config: IndexingServiceConfig) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("remote-indexing")
            .enable_all()
            .build()?;
        let timeout = Duration::from_secs(config.submit_timeout_sec);
        let channel = {
            let _guard = runtime.enter();
            Endpoint::from_shared(config.url.clone())?
                .connect_timeout(timeout)
                .timeout(timeout)
                .connect_lazy()
        };
        let remote_config = RemoteIndexingConfig {
            min_index_size_bytes: config.min_index_size_bytes,
            min_healing_coverage: config.min_healing_coverage,
            heartbeat_timeout: Duration::from_secs(config.heartbeat_timeout_sec),
            queue_timeout: Duration::from_secs(config.queue_timeout_sec),
            on_failure: match config.on_failure {
                IndexingOnFailure::Local => OnFailure::Local,
                IndexingOnFailure::Retry => OnFailure::Retry,
            },
            max_retries: config.max_retries,
            max_block_wait: Duration::from_secs(config.max_block_wait_sec),
        };
        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                remote_config,
                channel,
            }),
            runtime,
        })
    }
}

impl ClientInner {
    async fn submit_async(&self, task: TaskSubmission) -> Result<(), SubmitError> {
        let (collection, shard_id) = collection_and_shard(&task.scope.index_path);
        let core_version = common::defaults::QDRANT_VERSION_STRING;
        let mut request = tonic::Request::new(IndexingTaskSubmission {
            task_id: task.task_id.to_string(),
            token: task.token.to_hex(),
            advertise_url: self.config.advertise_url.clone(),
            collection,
            shard_id,
            vector_name: task.scope.vector_name.clone(),
            core_version: core_version.to_string(),
            graph_format_versions: task.graph_format_versions.clone(),
            vector_bytes: task.vector_bytes,
            point_count: task.point_count,
            attempt: task.attempt,
            cluster_id: self.config.cluster_id.clone(),
            index: Some(index_params_to_grpc(&task.index)),
            queue_deadline_unix_ms: task
                .queue_deadline
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_millis() as u64),
        });
        self.authorize(&mut request)
            .map_err(|err| SubmitError::Permanent(format!("invalid api key: {err}")))?;
        let response = IndexingServiceClient::new(self.channel.clone())
            .submit_task(request)
            .await
            .map_err(|status| SubmitError::Transient(format!("SubmitTask: {status}")))?
            .into_inner();
        refusal_to_result(&response)
    }

    /// Add the entry point's API key, if configured.
    fn authorize<T>(
        &self,
        request: &mut tonic::Request<T>,
    ) -> Result<(), tonic::metadata::errors::InvalidMetadataValue> {
        if let Some(api_key) = &self.config.api_key {
            request
                .metadata_mut()
                .insert(API_KEY_HEADER, api_key.parse()?);
        }
        Ok(())
    }

    async fn cancel_async(&self, task_id: Uuid, token: TaskToken) {
        let mut request = tonic::Request::new(CancelIndexingTaskRequest {
            task_id: task_id.to_string(),
            token: token.to_hex(),
        });
        if self.authorize(&mut request).is_err() {
            return;
        }
        match IndexingServiceClient::new(self.channel.clone())
            .cancel_task(request)
            .await
        {
            Ok(response) => log::debug!(
                "Remote indexing task {task_id} cancelled at the entry point (removed: {})",
                response.into_inner().removed,
            ),
            // An indexer without a queue may not implement it: nothing to drop there
            Err(status) => log::debug!("CancelTask of remote indexing task {task_id}: {status}"),
        }
    }
}

/// `Ok` if the entry point accepted the task, otherwise whether retrying can help.
fn refusal_to_result(response: &SubmitIndexingTaskResponse) -> Result<(), SubmitError> {
    let SubmitIndexingTaskResponse {
        accepted,
        message,
        refusal: _,
    } = response;
    if *accepted {
        return Ok(());
    }
    let reason = format!("task refused: {message}");
    // Unknown values decode as `Transient`: retrying is the safe default
    match response.refusal() {
        IndexingTaskRefusal::IndexingRefusalTransient => Err(SubmitError::Transient(reason)),
        IndexingTaskRefusal::IndexingRefusalPermanent => Err(SubmitError::Permanent(reason)),
    }
}

impl RemoteIndexer for RemoteIndexingClient {
    fn config(&self) -> &RemoteIndexingConfig {
        &self.inner.remote_config
    }

    fn submit(&self, task: &TaskSubmission) -> Result<(), SubmitError> {
        // Never `block_on` here: the caller may be inside another runtime's context
        let (sender, receiver) = mpsc::sync_channel(1);
        let task = task.clone();
        let inner = self.inner.clone();
        let join = self.runtime.spawn(async move {
            let _ = sender.send(inner.submit_async(task).await);
        });
        let timeout = Duration::from_secs(self.inner.config.submit_timeout_sec + 5);
        let result = receiver
            .recv_timeout(timeout)
            .unwrap_or_else(|_| Err(SubmitError::Transient("submission timed out".to_string())));
        join.abort();
        result
    }

    fn cancel(&self, task_id: Uuid, token: &TaskToken) {
        // Fire and forget, bounded by the channel's timeout
        let inner = self.inner.clone();
        let token = *token;
        self.runtime
            .spawn(async move { inner.cancel_async(task_id, token).await });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_kinds() {
        let response = |accepted, refusal: IndexingTaskRefusal| SubmitIndexingTaskResponse {
            accepted,
            message: "too large".to_string(),
            refusal: refusal as i32,
        };
        assert_eq!(
            refusal_to_result(&response(
                true,
                IndexingTaskRefusal::IndexingRefusalPermanent
            )),
            Ok(())
        );
        assert_eq!(
            refusal_to_result(&response(
                false,
                IndexingTaskRefusal::IndexingRefusalTransient
            )),
            Err(SubmitError::Transient(
                "task refused: too large".to_string()
            ))
        );
        assert_eq!(
            refusal_to_result(&response(
                false,
                IndexingTaskRefusal::IndexingRefusalPermanent
            )),
            Err(SubmitError::Permanent(
                "task refused: too large".to_string()
            ))
        );
        // A refusal kind from a newer indexer is retried
        let mut unknown = response(false, IndexingTaskRefusal::IndexingRefusalTransient);
        unknown.refusal = 42;
        assert!(matches!(
            refusal_to_result(&unknown),
            Err(SubmitError::Transient(_))
        ));
    }
}
