//! Remote indexing, server side: the entry point clients that submit tasks (§4.8), and their
//! registration as the process-wide `RemoteIndexer` (§4.1).

use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use api::grpc::qdrant::IndexingTaskSubmission;
use api::grpc::qdrant::indexing_service_client::IndexingServiceClient;
use segment::index::hnsw_index::remote::{
    OnFailure, RemoteIndexer, RemoteIndexingConfig, TaskSubmission, set_remote_indexer,
};
use serde::Serialize;
use tokio::runtime::Runtime;
use tonic::transport::{Channel, Endpoint};

use crate::settings::{IndexingEntryPoint, IndexingOnFailure, IndexingServiceConfig, Settings};

/// Header with the entry point API key.
const API_KEY_HEADER: &str = "api-key";

/// Turn remote indexing on, if configured and safe (§4.1). Returns whether it is on.
pub fn init_remote_indexing(settings: &Settings) -> bool {
    let Some(config) = &settings.indexing_service else {
        set_remote_indexer(None);
        return false;
    };

    // The indexer reaches the internal port. Without auth there, it could reach Raft too.
    if settings.service.api_key.as_deref().is_none_or(str::is_empty)
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
                "Remote indexing enabled: {:?} entry point {}, advertised as {}, \
                 min index size {} bytes",
                config.entry_point,
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
            Component::Prefix(_) | Component::RootDir | Component::CurDir | Component::ParentDir => {
                None
            }
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

enum Transport {
    Direct(Channel),
    Queue(reqwest::Client),
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
    transport: Transport,
}

/// Task body for the queue entry point.
#[derive(Serialize)]
struct QueueTask<'a> {
    task_id: String,
    token: String,
    advertise_url: &'a str,
    collection: String,
    shard_id: u32,
    vector_name: &'a str,
    core_version: &'a str,
    graph_format_versions: &'a [u32],
    vector_bytes: u64,
    point_count: u64,
    attempt: u32,
    cluster_id: Option<&'a str>,
}

impl RemoteIndexingClient {
    fn new(config: IndexingServiceConfig) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("remote-indexing")
            .enable_all()
            .build()?;
        let timeout = Duration::from_secs(config.submit_timeout_sec);
        let transport = match config.entry_point {
            IndexingEntryPoint::Direct => {
                // Lazy: the indexer may come up after core
                let _guard = runtime.enter();
                let channel = Endpoint::from_shared(config.url.clone())?
                    .connect_timeout(timeout)
                    .timeout(timeout)
                    .connect_lazy();
                Transport::Direct(channel)
            }
            IndexingEntryPoint::Queue => {
                Transport::Queue(reqwest::Client::builder().timeout(timeout).build()?)
            }
        };
        let remote_config = RemoteIndexingConfig {
            min_index_size_bytes: config.min_index_size_bytes,
            heartbeat_timeout: Duration::from_secs(config.heartbeat_timeout_sec),
            on_failure: match config.on_failure {
                IndexingOnFailure::Local => OnFailure::Local,
                IndexingOnFailure::Retry => OnFailure::Retry,
            },
            max_retries: config.max_retries,
        };
        Ok(Self {
            inner: Arc::new(ClientInner {
                config,
                remote_config,
                transport,
            }),
            runtime,
        })
    }
}

impl ClientInner {
    async fn submit_async(&self, task: TaskSubmission) -> Result<(), String> {
        let (collection, shard_id) = collection_and_shard(&task.scope.index_path);
        let core_version = common::defaults::QDRANT_VERSION_STRING;
        match &self.transport {
            Transport::Direct(channel) => {
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
                });
                if let Some(api_key) = &self.config.api_key {
                    let value = api_key
                        .parse()
                        .map_err(|err| format!("invalid api key: {err}"))?;
                    request.metadata_mut().insert(API_KEY_HEADER, value);
                }
                let response = IndexingServiceClient::new(channel.clone())
                    .submit_task(request)
                    .await
                    .map_err(|status| format!("SubmitTask: {status}"))?
                    .into_inner();
                if response.accepted {
                    Ok(())
                } else {
                    Err(format!("task refused: {}", response.message))
                }
            }
            Transport::Queue(client) => {
                let body = QueueTask {
                    task_id: task.task_id.to_string(),
                    token: task.token.to_hex(),
                    advertise_url: &self.config.advertise_url,
                    collection,
                    shard_id,
                    vector_name: &task.scope.vector_name,
                    core_version,
                    graph_format_versions: &task.graph_format_versions,
                    vector_bytes: task.vector_bytes,
                    point_count: task.point_count,
                    attempt: task.attempt,
                    cluster_id: self.config.cluster_id.as_deref(),
                };
                let url = format!("{}/tasks", self.config.url.trim_end_matches('/'));
                let mut request = client.post(url).json(&body);
                if let Some(api_key) = &self.config.api_key {
                    request = request.header(API_KEY_HEADER, api_key);
                }
                let response = request.send().await.map_err(|err| err.to_string())?;
                let status = response.status();
                if status.is_success() {
                    Ok(())
                } else {
                    let text = response.text().await.unwrap_or_default();
                    Err(format!("queue answered {status}: {text}"))
                }
            }
        }
    }
}

impl RemoteIndexer for RemoteIndexingClient {
    fn config(&self) -> &RemoteIndexingConfig {
        &self.inner.remote_config
    }

    fn submit(&self, task: &TaskSubmission) -> Result<(), String> {
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
            .unwrap_or_else(|_| Err("submission timed out".to_string()));
        join.abort();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_collection_and_shard() {
        let path = Path::new(
            "/qdrant/storage/collections/my_coll/3/segments/temp/abc/vector_index-image",
        );
        assert_eq!(collection_and_shard(path), ("my_coll".to_string(), 3));
        assert_eq!(collection_and_shard(Path::new("/tmp/x")), (String::new(), 0));
    }
}
