use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use std::vec::Vec;

use rand::seq::SliceRandom;
use tokio::select;
use tonic::transport::{Channel, ClientTlsConfig, Error as TonicError, Uri};
use tonic::{Code, Status};

use crate::grpc::qdrant::qdrant_client::QdrantClient;
use crate::grpc::qdrant::HealthCheckRequest;

const DEFAULT_GRPC_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

const SMART_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_POOL_SIZE: usize = 1;
const CHANNEL_TTL: Duration = Duration::from_secs(5);

struct ChannelPool {
    channels: Vec<Channel>,
    init_at: Instant,
}

impl ChannelPool {
    async fn init(
        uri: Uri,
        pool_size: NonZeroUsize,
        grpc_timeout: Duration,
        connection_timeout: Duration,
        tls_config: Option<ClientTlsConfig>,
    ) -> Result<Self, TonicError> {
        let mut channels = Vec::with_capacity(pool_size.into());
        for _ in 0..pool_size.into() {
            let channel = TransportChannelPool::make_channel(
                grpc_timeout,
                connection_timeout,
                uri.clone(),
                tls_config.clone(),
            )
            .await?;
            channels.push(channel);
        }

        Ok(Self {
            channels,
            init_at: Instant::now(),
        })
    }

    fn choose(&self) -> Channel {
        self.channels
            .choose(&mut rand::thread_rng())
            .expect("Pool size can not be zero")
            .clone()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RequestError<E: std::error::Error> {
    #[error("Error in closure supplied to transport channel pool: {0}")]
    FromClosure(E),
    #[error("Tonic error: {0}")]
    Tonic(#[from] TonicError),
}

/// Holds a pool of channels established for a set of URIs.
/// Channel are shared by cloning them.
/// Make the `pool_size` larger to increase throughput.
pub struct TransportChannelPool {
    uri_to_pool: tokio::sync::RwLock<HashMap<Uri, ChannelPool>>,
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

    pub async fn make_channel(
        grpc_timeout: Duration,
        connection_timeout: Duration,
        uri: Uri,
        tls_config: Option<ClientTlsConfig>,
    ) -> Result<Channel, TonicError> {
        let mut endpoint = Channel::builder(uri)
            .timeout(grpc_timeout)
            .connect_timeout(connection_timeout)
            .keep_alive_while_idle(true);
        if let Some(config) = tls_config {
            endpoint = endpoint.tls_config(config)?;
        }
        // `connect` is using the `Reconnect` network service internally to handle dropped connections
        endpoint.connect().await
    }

    /// Initialize a pool for the URI and return a clone of the first channel.
    /// Does not fail if the pool already exist.
    async fn init_pool_for_uri(&self, uri: Uri) -> Result<Channel, TonicError> {
        let mut guard = self.uri_to_pool.write().await;
        match guard.get(&uri) {
            None => {
                let channels = ChannelPool::init(
                    uri.clone(),
                    self.pool_size,
                    self.grpc_timeout,
                    self.connection_timeout,
                    self.tls_config.clone(),
                )
                .await?;
                let channel = channels.choose();
                guard.insert(uri, channels);
                Ok(channel)
            }
            Some(channels) => Ok(channels.choose()),
        }
    }

    pub async fn drop_pool(&self, uri: &Uri) {
        let mut guard = self.uri_to_pool.write().await;
        guard.remove(uri);
    }

    async fn get_pooled_channel(&self, uri: &Uri) -> Option<Channel> {
        let guard = self.uri_to_pool.read().await;
        guard.get(uri).map(|channels| channels.choose())
    }

    async fn get_or_create_pooled_channel(&self, uri: &Uri) -> Result<Channel, TonicError> {
        match self.get_pooled_channel(uri).await {
            None => self.init_pool_for_uri(uri.clone()).await,
            Some(channel) => Ok(channel),
        }
    }

    async fn get_created_at(&self, uri: &Uri) -> Option<Instant> {
        let guard = self.uri_to_pool.read().await;
        guard.get(uri).map(|channels| channels.init_at)
    }

    /// Checks if the channel is still alive.
    ///
    /// It uses duplicate "fast" chanel, equivalent ot the original, but with smaller timeout.
    /// If it can't get healthcheck response in the timeout, it assumes the channel is dead.
    /// And we need to drop the pool for the uri and try again.
    /// For performance reasons, we start the check only after `SMART_CONNECT_TIMEOUT`.
    async fn check_connectability(&self, uri: &Uri) -> Status {
        loop {
            tokio::time::sleep(SMART_CONNECT_TIMEOUT).await;
            let channel = self.get_pooled_channel(uri).await;
            match channel {
                None => return Status::unavailable("Channel dropped"),
                Some(channel) => {
                    let mut client = QdrantClient::new(channel);
                    let resp = client.health_check(HealthCheckRequest {}).await;
                    match resp {
                        Ok(_) => {
                            // continue watching
                        }
                        Err(status) => return status,
                    }
                }
            }
        }
    }

    // Allows to use channel to `uri`. If there is no channels to specified uri - they will be created.
    pub async fn with_channel_timeout<T, O: Future<Output = Result<T, Status>>>(
        &self,
        uri: &Uri,
        f: impl Fn(Channel) -> O,
        timeout: Option<Duration>,
    ) -> Result<T, RequestError<Status>> {
        let channel = self.get_or_create_pooled_channel(uri).await?;
        let max_timeout = timeout.unwrap_or_else(|| self.grpc_timeout + self.connection_timeout);

        let result: Result<T, Status> = select! {
            res = f(channel) => {
                res
            }
            res = self.check_connectability(uri) => {
               Err(res)
            }
            _res = tokio::time::sleep(max_timeout) => {
                log::debug!("Timeout reached for uri: {}", uri);
                Err(Status::deadline_exceeded("Timeout exceeded"))
            }
        };

        // Reconnect on failure to handle the case with domain name change.
        match result {
            Ok(res) => Ok(res),
            Err(err) => match err.code() {
                Code::Internal | Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded => {
                    let channel_uptime = Instant::now().duration_since(
                        self.get_created_at(uri).await.unwrap_or_else(Instant::now),
                    );
                    if channel_uptime > CHANNEL_TTL {
                        log::debug!("dropping channel pool for uri: {}", uri);
                        self.drop_pool(uri).await;
                        let channel = self.get_or_create_pooled_channel(uri).await?;
                        f(channel).await.map_err(RequestError::FromClosure)
                    } else {
                        Err(err).map_err(RequestError::FromClosure)
                    }
                }
                _ => Err(RequestError::FromClosure(err)),
            },
        }
    }

    // Allows to use channel to `uri`. If there is no channels to specified uri - they will be created.
    pub async fn with_channel<T, O: Future<Output = Result<T, Status>>>(
        &self,
        uri: &Uri,
        f: impl Fn(Channel) -> O,
    ) -> Result<T, RequestError<Status>> {
        self.with_channel_timeout(uri, f, None).await
    }
}
