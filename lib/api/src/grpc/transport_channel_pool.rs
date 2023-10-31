use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Duration;

use rand::{thread_rng, Rng};
use tokio::select;
use tonic::codegen::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, ClientTlsConfig, Error as TonicError, Uri};
use tonic::{Code, Request, Status};

use crate::grpc::dynamic_channel_pool::DynamicChannelPool;
use crate::grpc::dynamic_pool::CountedItem;
use crate::grpc::qdrant::qdrant_client::QdrantClient;
use crate::grpc::qdrant::HealthCheckRequest;

/// Maximum lifetime of a gRPC channel.
///
/// Using 1 day (24 hours) because the request with the longest timeout currently uses the same
/// timeout value. Namely the shard recovery call used in shard snapshot transfer.
pub const MAX_GRPC_CHANNEL_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

pub const DEFAULT_GRPC_TIMEOUT: Duration = Duration::from_secs(60);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_POOL_SIZE: usize = 2;

/// Allow a large number of connections per channel, that is close to the limit of
/// `http2_max_pending_accept_reset_streams` that we configure to minimize the chance of
/// GOAWAY/ENHANCE_YOUR_CALM errors from occurring.
/// More info: <https://github.com/qdrant/qdrant/issues/1907>
const MAX_CONNECTIONS_PER_CHANNEL: usize = 1024;
const DEFAULT_RETRIES: usize = 2;
const DEFAULT_BACKOFF: Duration = Duration::from_millis(100);

/// How long to wait for response from server, before checking health of the server
const SMART_CONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// There is no indication, that health-check API is affected by high parallel load
/// So we can use small timeout for health-check
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

/// Try to recreate channel, if there were no successful requests within this time
const CHANNEL_TTL: Duration = Duration::from_secs(5);

#[derive(thiserror::Error, Debug)]
pub enum RequestError<E: std::error::Error> {
    #[error("Error in closure supplied to transport channel pool: {0}")]
    FromClosure(E),
    #[error("Tonic error: {0}")]
    Tonic(#[from] TonicError),
}

enum RetryAction {
    Fail(Status),
    RetryOnce(Status),
    RetryWithBackoff(Status),
    RetryImmediately(Status),
}

#[derive(Debug)]
enum HealthCheckError {
    NoChannel,
    ConnectionError(TonicError),
    RequestError(Status),
}

#[derive(Debug)]
enum RequestFailure {
    HealthCheck(HealthCheckError),
    RequestError(Status),
    RequestConnection(TonicError),
}

/// Intercepts gRPC requests and adds a default timeout if it wasn't already set.
pub struct AddTimeout {
    default_timeout: Duration,
}

impl AddTimeout {
    pub fn new(default_timeout: Duration) -> Self {
        Self { default_timeout }
    }
}

impl Interceptor for AddTimeout {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if request.metadata().get("grpc-timeout").is_none() {
            request.set_timeout(self.default_timeout);
        }
        Ok(request)
    }
}

/// Holds a pool of channels established for a set of URIs.
/// Channel are shared by cloning them.
/// Make the `pool_size` larger to increase throughput.
pub struct TransportChannelPool {
    uri_to_pool: tokio::sync::RwLock<HashMap<Uri, DynamicChannelPool>>,
    pool_size: NonZeroUsize,
    grpc_timeout: Duration,
    connection_timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
}

impl Default for TransportChannelPool {
    fn default() -> Self {
        Self {
            uri_to_pool: tokio::sync::RwLock::new(HashMap::new()),
            pool_size: NonZeroUsize::new(DEFAULT_POOL_SIZE).unwrap(),
            grpc_timeout: DEFAULT_GRPC_TIMEOUT,
            connection_timeout: DEFAULT_CONNECT_TIMEOUT,
            tls_config: None,
        }
    }
}

impl TransportChannelPool {
    pub fn new(
        p2p_grpc_timeout: Duration,
        connection_timeout: Duration,
        pool_size: usize,
        tls_config: Option<ClientTlsConfig>,
    ) -> Self {
        Self {
            uri_to_pool: Default::default(),
            grpc_timeout: p2p_grpc_timeout,
            connection_timeout,
            pool_size: NonZeroUsize::new(pool_size).unwrap(),
            tls_config,
        }
    }

    async fn _init_pool_for_uri(&self, uri: Uri) -> Result<DynamicChannelPool, TonicError> {
        DynamicChannelPool::new(
            uri,
            MAX_GRPC_CHANNEL_TIMEOUT,
            self.connection_timeout,
            self.tls_config.clone(),
            MAX_CONNECTIONS_PER_CHANNEL,
            self.pool_size.get(),
        )
        .await
    }

    /// Initialize a pool for the URI and return a clone of the first channel.
    /// Does not fail if the pool already exist.
    async fn init_pool_for_uri(&self, uri: Uri) -> Result<CountedItem<Channel>, TonicError> {
        let mut guard = self.uri_to_pool.write().await;
        match guard.get_mut(&uri) {
            None => {
                let channels = self._init_pool_for_uri(uri.clone()).await?;
                let channel = channels.choose().await?;
                guard.insert(uri, channels);
                Ok(channel)
            }
            Some(channels) => channels.choose().await,
        }
    }

    pub async fn drop_pool(&self, uri: &Uri) {
        let mut guard = self.uri_to_pool.write().await;
        guard.remove(uri);
    }

    pub async fn drop_channel(&self, uri: &Uri, channel: CountedItem<Channel>) {
        let guard = self.uri_to_pool.read().await;
        if let Some(pool) = guard.get(uri) {
            pool.drop_channel(channel);
        }
    }

    async fn get_pooled_channel(
        &self,
        uri: &Uri,
    ) -> Option<Result<CountedItem<Channel>, TonicError>> {
        let guard = self.uri_to_pool.read().await;
        match guard.get(uri) {
            None => None,
            Some(channels) => Some(channels.choose().await),
        }
    }

    async fn get_or_create_pooled_channel(
        &self,
        uri: &Uri,
    ) -> Result<CountedItem<Channel>, TonicError> {
        match self.get_pooled_channel(uri).await {
            None => self.init_pool_for_uri(uri.clone()).await,
            Some(channel) => channel,
        }
    }

    /// Checks if the channel is still alive.
    ///
    /// It uses duplicate "fast" channel, equivalent to the original, but with smaller timeout.
    /// If it can't get healthcheck response in the timeout, it assumes the channel is dead.
    /// And we need to drop the pool for the uri and try again.
    /// For performance reasons, we start the check only after `SMART_CONNECT_TIMEOUT`.
    async fn check_connectability(&self, uri: &Uri) -> HealthCheckError {
        loop {
            tokio::time::sleep(SMART_CONNECT_INTERVAL).await;
            let channel = self.get_pooled_channel(uri).await;
            match channel {
                None => return HealthCheckError::NoChannel,
                Some(Err(tonic_error)) => return HealthCheckError::ConnectionError(tonic_error),
                Some(Ok(channel)) => {
                    let mut client = QdrantClient::new(channel.item().clone());

                    let resp: Result<_, Status> = select! {
                        res = client.health_check(HealthCheckRequest {}) => {
                            res
                        }
                        _ = tokio::time::sleep(HEALTH_CHECK_TIMEOUT) => {
                            // Current healthcheck timed out, but maybe there were other requests
                            // that succeeded in a given time window.
                            // If so, we can continue watching.
                            if channel.last_success_age() > HEALTH_CHECK_TIMEOUT {
                                return HealthCheckError::RequestError(Status::deadline_exceeded(format!("Healthcheck timeout {}ms exceeded", HEALTH_CHECK_TIMEOUT.as_millis())))
                            } else {
                                continue;
                            }
                        }
                    };
                    match resp {
                        Ok(_) => {
                            channel.report_success();
                            // continue watching
                        }
                        Err(status) => return HealthCheckError::RequestError(status),
                    }
                }
            }
        }
    }

    async fn make_request<T, O: Future<Output = Result<T, Status>>>(
        &self,
        uri: &Uri,
        f: &impl Fn(InterceptedService<Channel, AddTimeout>) -> O,
        timeout: Duration,
    ) -> Result<T, RequestFailure> {
        let channel = match self.get_or_create_pooled_channel(uri).await {
            Ok(channel) => channel,
            Err(tonic_error) => {
                return Err(RequestFailure::RequestConnection(tonic_error));
            }
        };

        let intercepted_channel =
            InterceptedService::new(channel.item().clone(), AddTimeout::new(timeout));

        let result: RequestFailure = select! {
            res = f(intercepted_channel) => {
                match res {
                    Ok(body) => {
                        channel.report_success();
                        return Ok(body);
                    },
                    Err(err) => RequestFailure::RequestError(err)
                }
            }
            res = self.check_connectability(uri) => {
               RequestFailure::HealthCheck(res)
            }
        };

        // After this point the request is not successful, but we can try to recover
        let last_success_age = channel.last_success_age();
        if last_success_age > CHANNEL_TTL {
            // There were no successful requests for a long time, we can try to reconnect
            // It might be possible that server died and changed its ip address
            self.drop_channel(uri, channel).await;
        } else {
            // We don't need this channel anymore, drop before waiting for the backoff
            drop(channel);
        }

        Err(result)
    }

    // Allows to use channel to `uri`. If there is no channels to specified uri - they will be created.
    pub async fn with_channel_timeout<T, O: Future<Output = Result<T, Status>>>(
        &self,
        uri: &Uri,
        f: impl Fn(InterceptedService<Channel, AddTimeout>) -> O,
        timeout: Option<Duration>,
        retries: usize,
    ) -> Result<T, RequestError<Status>> {
        let mut retries_left = retries;
        let mut attempt = 0;
        let max_timeout = timeout.unwrap_or_else(|| self.grpc_timeout + self.connection_timeout);

        loop {
            let request_result: Result<T, _> = self.make_request(uri, &f, max_timeout).await;

            let error_result = match request_result {
                Ok(body) => return Ok(body),
                Err(err) => err,
            };

            let action = match error_result {
                RequestFailure::HealthCheck(healthcheck_error) => {
                    match healthcheck_error {
                        HealthCheckError::NoChannel => {
                            // The channel pool was dropped during the request processing.
                            // Meaning that the peer is not available anymore.
                            // So we can just fail the request.
                            RetryAction::Fail(Status::unavailable(format!(
                                "Peer {} is not available",
                                uri
                            )))
                        }
                        HealthCheckError::ConnectionError(error) => {
                            // Can't establish connection to the server during the healthcheck.
                            // Possible situation:
                            // - Server was killed during the request processing and request timed out.
                            // Actions:
                            // - retry no backoff
                            RetryAction::RetryImmediately(Status::unavailable(format!(
                                "Failed to connect to {}, error: {}",
                                uri, error
                            )))
                        }
                        HealthCheckError::RequestError(status) => {
                            // Channel might be unavailable or overloaded.
                            // Or server might be dead.
                            RetryAction::RetryWithBackoff(status)
                        }
                    }
                }
                RequestFailure::RequestError(status) => {
                    match status.code() {
                        Code::Cancelled | Code::Unavailable => {
                            // Possible situations:
                            // - Server is frozen and will never respond.
                            // - Server is overloaded and will respond in the future.
                            RetryAction::RetryWithBackoff(status)
                        }
                        Code::Internal => {
                            // Something is broken, but let's retry anyway, but only once.
                            RetryAction::RetryOnce(status)
                        }
                        _ => {
                            // No special handling, just fail already.
                            RetryAction::Fail(status)
                        }
                    }
                }
                RequestFailure::RequestConnection(error) => {
                    // Can't establish connection to the server during the request.
                    // Possible situation:
                    // - Server is killed
                    // - Server is overloaded
                    // Actions:
                    // - retry with backoff
                    RetryAction::RetryWithBackoff(Status::unavailable(format!(
                        "Failed to connect to {}, error: {}",
                        uri, error
                    )))
                }
            };

            let (backoff_time, fallback_status) = match action {
                RetryAction::Fail(err) => return Err(RequestError::FromClosure(err)),
                RetryAction::RetryImmediately(fallback_status) => (Duration::ZERO, fallback_status),
                RetryAction::RetryWithBackoff(fallback_status) => {
                    // Calculate backoff
                    let backoff = DEFAULT_BACKOFF * 2u32.pow(attempt as u32)
                        + Duration::from_millis(thread_rng().gen_range(0..100));

                    if backoff > max_timeout {
                        // We can't wait for the request any longer, return the error as is
                        return Err(RequestError::FromClosure(fallback_status));
                    }
                    (backoff, fallback_status)
                }
                RetryAction::RetryOnce(fallback_status) => {
                    if retries_left > 1 {
                        retries_left = 1;
                    }
                    (Duration::ZERO, fallback_status)
                }
            };

            attempt += 1;
            if retries_left == 0 {
                return Err(RequestError::FromClosure(fallback_status));
            }
            retries_left = retries_left.saturating_sub(1);

            // Wait for the backoff
            tokio::time::sleep(backoff_time).await;
        }
    }

    // Allows to use channel to `uri`. If there is no channels to specified uri - they will be created.
    pub async fn with_channel<T, O: Future<Output = Result<T, Status>>>(
        &self,
        uri: &Uri,
        f: impl Fn(InterceptedService<Channel, AddTimeout>) -> O,
    ) -> Result<T, RequestError<Status>> {
        self.with_channel_timeout(uri, f, None, DEFAULT_RETRIES)
            .await
    }
}
