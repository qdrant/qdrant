use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tonic::transport::{Channel, ClientTlsConfig, Error as TonicError, Uri};

use crate::grpc::dynamic_pool::{CountedItem, DynamicPool};

pub async fn make_grpc_channel(
    timeout: Duration,
    connection_timeout: Duration,
    uri: Uri,
    tls_config: Option<ClientTlsConfig>,
) -> Result<Channel, TonicError> {
    let mut endpoint = Channel::builder(uri)
        .timeout(timeout)
        .connect_timeout(connection_timeout)
        .keep_alive_while_idle(true);
    if let Some(config) = tls_config {
        endpoint = endpoint.tls_config(config)?;
    }
    // `connect` is using the `Reconnect` network service internally to handle dropped connections
    endpoint.connect().await
}

pub struct DynamicChannelPool {
    pool: Mutex<DynamicPool<Channel>>,
    init_at: Instant,
    uri: Uri,
    timeout: Duration,
    connection_timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
    // Timestamp of the last successful connection.
    last_success: AtomicUsize,
}

impl DynamicChannelPool {
    pub async fn new(
        uri: Uri,
        timeout: Duration,
        connection_timeout: Duration,
        tls_config: Option<ClientTlsConfig>,
        usage_per_channel: usize,
        min_channels: usize,
    ) -> Result<Self, TonicError> {
        let mut channels = Vec::with_capacity(min_channels);
        for _ in 0..min_channels {
            let channel =
                make_grpc_channel(timeout, connection_timeout, uri.clone(), tls_config.clone())
                    .await?;
            channels.push(channel);
        }

        let init_at = Instant::now();
        let last_success_since = Instant::now().duration_since(init_at).as_millis() as usize;

        let pool = DynamicPool::new(channels, usage_per_channel, min_channels);
        Ok(Self {
            pool: Mutex::new(pool),
            init_at,
            uri,
            timeout,
            connection_timeout,
            tls_config,
            last_success: AtomicUsize::new(last_success_since),
        })
    }

    pub fn init_at(&self) -> Instant {
        self.init_at
    }

    pub async fn choose(&self) -> Result<CountedItem<Channel>, TonicError> {
        let channel = self.pool.lock().choose();
        let channel = match channel {
            None => {
                let channel = make_grpc_channel(
                    self.timeout,
                    self.connection_timeout,
                    self.uri.clone(),
                    self.tls_config.clone(),
                )
                .await?;
                self.pool.lock().add(channel)
            }
            Some(channel) => channel,
        };
        Ok(channel)
    }

    pub fn set_last_success(&self) {
        let last_success_since = Instant::now().duration_since(self.init_at).as_millis() as usize;
        self.last_success
            .store(last_success_since, Ordering::Relaxed);
    }

    pub fn last_success_age(&self) -> Duration {
        let last_success_since = self.last_success.load(Ordering::Relaxed);
        let last_success = self.init_at + Duration::from_millis(last_success_since as u64);
        Instant::now().duration_since(last_success)
    }
}
