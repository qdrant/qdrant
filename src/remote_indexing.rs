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
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use uuid::Uuid;
use validator::Validate as _;

use crate::settings::{IndexingOnFailure, IndexingServiceConfig, Settings};

/// Header with the entry point API key.
const API_KEY_HEADER: &str = "api-key";

/// Interval of the keepalive probes on the channel to the entry point: TCP ones while it
/// idles, HTTP/2 pings while a call is in flight.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Turn remote indexing on, if configured and safe (§4.1). Returns whether it is on.
pub fn init_remote_indexing(settings: &Settings) -> bool {
    let Some(config) = &settings.indexing_service else {
        set_remote_indexer(None);
        return false;
    };

    if let Err(reason) = indexer_can_call_back(settings) {
        log::error!("Remote indexing is configured but disabled: {reason}");
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

/// Whether an indexer can call back with `IndexingTask`, and can do nothing else there.
/// Otherwise every task would wait out `queue_timeout_sec` before the local build.
fn indexer_can_call_back(settings: &Settings) -> Result<(), &'static str> {
    // `IndexingTask` is served on the internal port, which is open in cluster mode only.
    if !settings.cluster.enabled {
        return Err(
            "it requires `cluster.enabled`, the indexer connects back to the internal gRPC port",
        );
    }

    // The internal port then requires a client certificate of the cluster CA, which the
    // indexer does not have.
    if settings.cluster.p2p.enable_tls {
        return Err(
            "it does not work with `cluster.p2p.enable_tls`, the indexer has no client \
             certificate for the internal gRPC port",
        );
    }

    // The indexer reaches the internal port. Without auth there, it could reach Raft too.
    if settings
        .service
        .api_key
        .as_deref()
        .is_none_or(str::is_empty)
        || !settings.service.enforce_internal_auth()
    {
        return Err(
            "it requires `service.api_key` and `service.enforce_internal_auth`, so that the \
             indexer cannot reach other internal services",
        );
    }

    Ok(())
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
    /// The entry point's API key, as the header value sent with every call.
    api_key: Option<AsciiMetadataValue>,
    /// Lazy: the receiver may come up after core.
    channel: Channel,
}

impl RemoteIndexingClient {
    fn new(config: IndexingServiceConfig) -> anyhow::Result<Self> {
        // Settings validation only warns. Here an invalid value breaks every task, e.g. a
        // zero timeout gives each one up at once, and the local build that follows hides it.
        config.validate()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("remote-indexing")
            .enable_all()
            .build()?;
        let timeout = Duration::from_secs(config.submit_timeout_sec);
        let channel = {
            let _guard = runtime.enter();
            let mut endpoint = Endpoint::from_shared(config.url.clone())?
                .connect_timeout(timeout)
                .timeout(timeout)
                // The channel idles between builds, for hours. Without probes, a connection
                // that a NAT or a load balancer dropped meanwhile is only found dead by the
                // next submission, which then times out.
                .tcp_keepalive(Some(KEEPALIVE_INTERVAL))
                .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
                .keep_alive_timeout(timeout);
            if let Some(tls) = entry_point_tls(&config)? {
                endpoint = endpoint.tls_config(tls)?;
            }
            endpoint.connect_lazy()
        };
        // Checked here, not on every submission: an invalid key is a configuration error,
        // which a local build after each failed submission would hide
        let api_key = config
            .api_key
            .as_deref()
            .map(|api_key| {
                let mut value = AsciiMetadataValue::try_from(api_key).map_err(|_| {
                    anyhow::anyhow!("`indexing_service.api_key` is not a valid header value")
                })?;
                value.set_sensitive(true);
                anyhow::Ok(value)
            })
            .transpose()?;
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
                api_key,
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
        self.authorize(&mut request);
        let response = IndexingServiceClient::new(self.channel.clone())
            .submit_task(request)
            .await
            .map_err(|status| SubmitError::Transient(format!("SubmitTask: {status}")))?
            .into_inner();
        refusal_to_result(&response)
    }

    /// Add the entry point's API key, if configured.
    fn authorize<T>(&self, request: &mut tonic::Request<T>) {
        if let Some(api_key) = &self.api_key {
            request
                .metadata_mut()
                .insert(API_KEY_HEADER, api_key.clone());
        }
    }

    async fn cancel_async(&self, task_id: Uuid, token: TaskToken) {
        let mut request = tonic::Request::new(CancelIndexingTaskRequest {
            task_id: task_id.to_string(),
            token: token.to_hex(),
        });
        self.authorize(&mut request);
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

/// TLS for an `https://` entry point, verified against `ca_cert` or the system roots.
/// `None` for a cleartext one.
fn entry_point_tls(config: &IndexingServiceConfig) -> anyhow::Result<Option<ClientTlsConfig>> {
    let https = config
        .url
        .get(.."https://".len())
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"));
    if !https {
        return Ok(None);
    }
    let tls = match &config.ca_cert {
        Some(path) => {
            let pem = fs_err::read_to_string(path)?;
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem))
        }
        None => ClientTlsConfig::new().with_native_roots(),
    };
    Ok(Some(tls))
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
    use api::grpc::qdrant::CancelIndexingTaskResponse;
    use api::grpc::qdrant::indexing_service_server::{IndexingService, IndexingServiceServer};
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    use super::*;

    fn config(url: &str) -> IndexingServiceConfig {
        serde_json::from_value(serde_json::json!({
            "url": url,
            "advertise_url": "http://localhost:6335",
        }))
        .unwrap()
    }

    #[test]
    fn enabled_only_where_the_indexer_can_call_back() {
        let mut settings = Settings::new(None).unwrap();
        settings.cluster.enabled = true;
        settings.cluster.p2p.enable_tls = false;
        settings.service.api_key = Some("secret".to_string());
        settings.service.enforce_internal_auth = Some(true);
        assert_eq!(indexer_can_call_back(&settings), Ok(()));

        // Nothing listens on the internal port of a single node.
        settings.cluster.enabled = false;
        assert!(indexer_can_call_back(&settings).is_err());
        settings.cluster.enabled = true;

        // The internal port asks for a client certificate.
        settings.cluster.p2p.enable_tls = true;
        assert!(indexer_can_call_back(&settings).is_err());
        settings.cluster.p2p.enable_tls = false;

        // The task token must be the only way in.
        settings.service.api_key = None;
        assert!(indexer_can_call_back(&settings).is_err());
        settings.service.api_key = Some("secret".to_string());
        settings.service.enforce_internal_auth = Some(false);
        assert!(indexer_can_call_back(&settings).is_err());
    }

    #[test]
    fn invalid_api_key_fails_at_startup() {
        let config = |api_key: &str| {
            let mut config = config("http://localhost:6340");
            config.api_key = Some(api_key.to_string());
            config
        };
        assert!(RemoteIndexingClient::new(config("secret")).is_ok());
        assert!(RemoteIndexingClient::new(config("line\nbreak")).is_err());
    }

    #[test]
    fn invalid_settings_fail_at_startup() {
        // Shorter than two heartbeats of the indexer: every healthy task would expire
        let mut short_lease = config("http://localhost:6340");
        short_lease.heartbeat_timeout_sec = 9;
        assert!(RemoteIndexingClient::new(short_lease).is_err());
        let mut shortest_lease = config("http://localhost:6340");
        shortest_lease.heartbeat_timeout_sec = 10;
        assert!(RemoteIndexingClient::new(shortest_lease).is_ok());

        let mut zero_queue_lease = config("http://localhost:6340");
        zero_queue_lease.queue_timeout_sec = 0;
        assert!(RemoteIndexingClient::new(zero_queue_lease).is_err());

        let mut coverage = config("http://localhost:6340");
        coverage.min_healing_coverage = 1.5;
        assert!(RemoteIndexingClient::new(coverage).is_err());
    }

    #[test]
    fn https_entry_point_gets_tls() {
        assert!(
            entry_point_tls(&config("http://localhost:6340"))
                .unwrap()
                .is_none()
        );
        assert!(
            entry_point_tls(&config("https://localhost:6340"))
                .unwrap()
                .is_some()
        );
        assert!(RemoteIndexingClient::new(config("https://localhost:6340")).is_ok());

        // A CA certificate that cannot be read is an error at startup
        let mut with_ca = config("https://localhost:6340");
        with_ca.ca_cert = Some("/nonexistent/ca.pem".to_string());
        assert!(RemoteIndexingClient::new(with_ca).is_err());
    }

    /// An entry point that only answers `CancelTask`, with whether the API key arrived.
    struct FakeEntryPoint;

    #[tonic::async_trait]
    impl IndexingService for FakeEntryPoint {
        async fn submit_task(
            &self,
            _request: tonic::Request<IndexingTaskSubmission>,
        ) -> Result<tonic::Response<SubmitIndexingTaskResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used"))
        }

        async fn cancel_task(
            &self,
            request: tonic::Request<CancelIndexingTaskRequest>,
        ) -> Result<tonic::Response<CancelIndexingTaskResponse>, tonic::Status> {
            let removed = request
                .metadata()
                .get(API_KEY_HEADER)
                .is_some_and(|key| key == "secret");
            Ok(tonic::Response::new(CancelIndexingTaskResponse { removed }))
        }
    }

    #[test]
    fn https_entry_point_is_called_over_tls() {
        // As `main` does; another test may have installed it already
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/e2e_tests/test_data/cert");
        let read = |name: &str| fs_err::read_to_string(cert_dir.join(name)).unwrap();
        let identity = Identity::from_pem(read("cert.pem"), read("key.pem"));

        let server_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = server_runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        server_runtime.spawn(async move {
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(IndexingServiceServer::new(FakeEntryPoint))
                .serve_with_incoming(TcpIncoming::from(listener))
                .await
                .unwrap();
        });

        let cancel = |ca_cert: Option<&Path>| {
            let mut config = config(&format!("https://localhost:{port}"));
            config.api_key = Some("secret".to_string());
            config.ca_cert = ca_cert.map(|path| path.to_string_lossy().into_owned());
            let client = RemoteIndexingClient::new(config).unwrap();
            let inner = client.inner.clone();
            client.runtime.block_on(async move {
                let mut request = tonic::Request::new(CancelIndexingTaskRequest {
                    task_id: String::new(),
                    token: String::new(),
                });
                inner.authorize(&mut request);
                IndexingServiceClient::new(inner.channel.clone())
                    .cancel_task(request)
                    .await
            })
        };

        // Verified against the configured CA, and the API key arrives
        let response = cancel(Some(&cert_dir.join("cacert.pem"))).unwrap();
        assert!(response.into_inner().removed);
        // The system roots do not know the test CA
        assert!(cancel(None).is_err());
    }

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
