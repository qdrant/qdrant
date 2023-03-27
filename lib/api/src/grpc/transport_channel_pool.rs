use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, Instant};

use tokio::sync::{self, mpsc};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Error as TonicError, Uri};
use tonic::{Code, Status};

const DEFAULT_POOL_SIZE: usize = 1;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

const CHANNEL_TTL: Duration = Duration::from_secs(5);

/// Holds a pool of channels established for a set of URIs.
/// Channel are shared by cloning them.
/// Make the `pool_size` larger to increase throughput.
pub struct TransportChannelPool {
    uri_to_pool: sync::RwLock<HashMap<Uri, ChannelPool>>,
    pool_size: usize,
    timeout: Duration,
    connect_timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
}

impl Default for TransportChannelPool {
    fn default() -> Self {
        Self {
            uri_to_pool: Default::default(),
            pool_size: DEFAULT_POOL_SIZE,
            timeout: DEFAULT_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tls_config: None,
        }
    }
}

impl TransportChannelPool {
    pub fn new(
        pool_size: usize,
        timeout: Duration,
        connect_timeout: Duration,
        tls_config: Option<ClientTlsConfig>,
    ) -> Self {
        Self {
            uri_to_pool: Default::default(),
            pool_size,
            timeout,
            connect_timeout,
            tls_config,
        }
    }

    // Allows to use channel to `uri`. If there is no channels to specified uri - they will be created.
    pub async fn with_channel<T, O: Future<Output = Result<T, Status>>>(
        &self,
        uri: &Uri,
        f: impl Fn(Channel) -> O,
    ) -> Result<T, RequestError<Status>> {
        let (channel, created_at) = self.get_or_create_pooled_channel(uri).await?;

        let result = match f(channel).await {
            Ok(resp) => Ok(resp),

            Err(err) => match err.code() {
                Code::Unavailable => {
                    let channel_uptime = created_at.elapsed();

                    if channel_uptime > CHANNEL_TTL {
                        self.drop_channel_pool(uri).await;

                        let (channel, _) = self.get_or_create_pooled_channel(uri).await?;
                        f(channel).await
                    } else {
                        Err(err)
                    }
                }

                _ => Err(err),
            },
        };

        result.map_err(RequestError::FromClosure)
    }

    pub async fn get_or_create_pooled_channel(
        &self,
        uri: &Uri,
    ) -> Result<(Channel, Instant), TonicError> {
        let mut uri_to_pool = self.uri_to_pool.write().await;

        let output = match uri_to_pool.get(uri) {
            Some(pool) => (pool.channel.clone(), pool.created_at),

            None => {
                let pool = ChannelPool::new(
                    uri.clone(),
                    self.pool_size,
                    self.timeout,
                    self.connect_timeout,
                    self.tls_config.clone(),
                )
                .await?;

                let channel = pool.channel.clone();
                let created_at = pool.created_at;

                let _ = uri_to_pool.insert(uri.clone(), pool);

                (channel, created_at)
            }
        };

        Ok(output)
    }

    pub async fn drop_channel_pool(&self, uri: &Uri) {
        self.uri_to_pool.write().await.remove(uri);
    }
}

#[derive(Clone, Debug)]
struct ChannelPool {
    channel: Channel,
    _service_discovery_sender: mpsc::Sender<tower::discover::Change<usize, Endpoint>>,
    created_at: Instant,
}

impl ChannelPool {
    pub async fn new(
        uri: Uri,
        size: usize,
        timeout: Duration,
        connect_timeout: Duration,
        tls_config: Option<ClientTlsConfig>,
    ) -> Result<Self, TonicError> {
        // TODO: Enable HTTP/2 keep-alive for *all* channels (e.g., move into `channel_builder`)?
        let endpoint = channel_builder(uri, timeout, connect_timeout, tls_config)?
            .http2_keep_alive_interval(Duration::from_millis(500))
            .keep_alive_timeout(Duration::from_millis(500))
            .keep_alive_while_idle(true);

        let (channel, service_discovery_sender) = Channel::balance_channel(16);

        for index in 0..size.saturating_sub(1) {
            service_discovery_sender
                .send(tower::discover::Change::Insert(index, endpoint.clone()))
                .await
                .unwrap(); // should never fail...
        }

        service_discovery_sender
            .send(tower::discover::Change::Insert(
                size.saturating_sub(1),
                endpoint,
            ))
            .await
            .unwrap(); // should never fail...

        let pool = Self {
            channel,
            _service_discovery_sender: service_discovery_sender,
            created_at: Instant::now(),
        };

        Ok(pool)
    }
}

pub fn channel_builder(
    uri: Uri,
    timeout: Duration,
    connect_timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
) -> Result<Endpoint, TonicError> {
    // TODO: Enable HTTP/2 keep-alive for *all* channels?
    let mut endpoint = Channel::builder(uri)
        .timeout(timeout)
        .connect_timeout(connect_timeout);

    if let Some(tls_config) = tls_config {
        endpoint = endpoint.tls_config(tls_config)?;
    }

    Ok(endpoint)
}

#[derive(Debug, thiserror::Error)]
pub enum RequestError<E> {
    #[error("Tonic error: {0}")]
    Tonic(#[from] TonicError),

    #[error("Error in closure supplied to transport channel pool: {0}")]
    FromClosure(E),
}
