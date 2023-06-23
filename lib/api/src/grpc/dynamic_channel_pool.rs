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
        .connect_timeout(connection_timeout);
    if let Some(config) = tls_config {
        endpoint = endpoint.tls_config(config)?;
    }
    // `connect` is using the `Reconnect` network service internally to handle dropped connections
    endpoint.connect().await
}

#[derive(Debug)]
pub struct DynamicChannelPool {
    pool: Mutex<DynamicPool<Channel>>,
    init_at: Instant,
    uri: Uri,
    timeout: Duration,
    connection_timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
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

        let pool = DynamicPool::new(channels, usage_per_channel, min_channels);
        Ok(Self {
            pool: Mutex::new(pool),
            init_at,
            uri,
            timeout,
            connection_timeout,
            tls_config,
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

    pub fn drop_channel(&self, channel: CountedItem<Channel>) {
        self.pool.lock().drop_item(channel);
    }
}
